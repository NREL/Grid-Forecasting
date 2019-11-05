import argparse
from collections import defaultdict
import collections
import csv

from datetime import datetime, timezone
import json
import logging
import sys
import math
import os
import shutil
import numpy as np
import operator
from operator import itemgetter
import pandas as pd
from tabulate import tabulate
import signal
import time
from matplotlib import pyplot as plt
from itertools import islice
import zmq
import zlib

import query_model_adms as query_model
import query_historical_data as query_history
from ResultCSV import ResultCSV
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD
from gridappsd.topics import simulation_input_topic, simulation_output_topic, simulation_log_topic

from DLMP_ADMS import DLMP
from Result_ploting import Plot_helper



ghi_forecast_topic = '/topic/goss.gridappsd.forecast.ghi'
ghi_weather_topic = '/topic/goss.gridappsd.weather.ghi'

logging.basicConfig(filename='app.log',
                    filemode='w',
                    # stream=sys.stdout,
                    level=logging.INFO,
                    format="%(asctime)s - %(name)s;%(levelname)s|%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
# Only log errors to the stomp logger.
logging.getLogger('stomp.py').setLevel(logging.ERROR)

_log = logging.getLogger(__name__)


class Grid_Forecast(object):
    """ A simple class that handles publishing the solar forecast
    """

    def __init__(self, simulation_id, gridappsd_obj, model_mrid, start_time=1374498000, app_config={}, load_scale=1):
        """ Create a ``Grid_Forecast`` object


        Note
        ----
        This class does not subscribe only publishes.

        Parameters
        ----------
        simulation_id: str
            The simulation_id to use for publishing to a topic.
        gridappsd_obj: GridAPPSD
            An instatiated object that is connected to the gridappsd message bus
            usually this should be the same object which subscribes, but that
            isn't required.

        """
        _log.info("Init application")
        self._is_running = True
        self.simulation_id = simulation_id
        self._vmult = .001
        self.slack_number = 3
        self.Ysparse_file = ""
        self._gapps = gridappsd_obj
        self.fidselect = model_mrid
        self._start_time = start_time
        self._run_freq = app_config.get('run_freq', 30)
        self._historical_run = app_config.get('historical_run', 0)
        self._publish_to_topic = simulation_input_topic(simulation_id)
        start_time = start_time + (0 * 60 * 60)
        end_time = start_time + 3600 * 24
        self.present_step = 0
        self._app_config = app_config
        self._RTOPF_flag = app_config.get('OPF', 1)
        self._run_freq = app_config.get('run_freq', 30)
        self._run_realtime = app_config.get('run_realtime', True)
        self._load_scale = load_scale
        if self._historical_run != 0:
            query_history.get_meas_maps(self._historical_run, self.fidselect, start_time, end_time)
        else:
            print('No historical data!')

        self.MainDir = os.path.dirname(os.path.realpath(__file__))

        self.resFolder = ""
        self.res = {}
        self._results = {}
        self._send_simulation_status('STARTED', 'Initializing DER app for ' + str(self.simulation_id), 'INFO')
        self.MAX_LOG_LENGTH = 20
        self._generators = []
        self._the_first_violators = None


        # self.resFolder = os.path.join(self.MainDir, 'adms_result_' + circuit_name + '_' + str(self._start_time))
        # self._result_csv = ResultCSV()
        # self._result_csv.create_result_folder(self.resFolder)
        # self._result_csv.create_result_file(self.resFolder, 'result.csv',
        #                                     'second,epoch time,Load Demand,forecast time,Forecast Load Demand')

        ctx = zmq.Context()
        self._skt = ctx.socket(zmq.PUB)
        if running_on_host():
            self._skt.bind('tcp://127.0.0.1:9003')
        else:
            self._skt.bind('tcp://*:9003')

        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def running(self):
        return self._is_running

    def signal_handler(self, signal, frame):
        print('You pressed Ctrl+C! Saving output')
        self._result_csv.close()
        self.vn_file.close()
        self._DLMP_P_file.close()
        self._DLMP_P_B_file.close()
        self._DLMP_P_Vmag_file.close()
        self.save_plots()
        self._skt.close()
        self._is_running = False
        sys.exit(0)

    def _send_simulation_status(self, status, message, log_level):
        """send a status message to the GridAPPS-D log manager

        Function arguments:
            status -- Type: string. Description: The status of the simulation.
                Default: 'localhost'.
            stomp_port -- Type: string. Description: The port for Stomp
            protocol for the GOSS server. It must not be an empty string.
                Default: '61613'.
            username -- Type: string. Description: User name for GOSS connection.
            password -- Type: string. Description: Password for GOSS connection.

        Function returns:
            None.
        Function exceptions:
            RuntimeError()
        """
        simulation_status_topic = "goss.gridappsd.process.simulation.log.{}".format(self.simulation_id)

        valid_status = ['STARTING', 'STARTED', 'RUNNING', 'ERROR', 'CLOSED', 'COMPLETE']
        valid_level = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
        if status in valid_status:
            if log_level not in valid_level:
                log_level = 'INFO'
            t_now = datetime.utcnow()
            status_message = {
                "source": os.path.basename(__file__),
                "processId": str(self.simulation_id),
                "timestamp": int(time.mktime(t_now.timetuple())),
                "processStatus": status,
                "logMessage": str(message),
                "logLevel": log_level,
                "storeToDb": True
            }
            status_str = json.dumps(status_message)
            _log.info("{}\n\n".format(status_str))
            # debugFile.write("{}\n\n".format(status_str))
            self._gapps.send(simulation_status_topic, status_str)

    def _send_pause(self):
        command = {
            "command": "pause"
        }
        command = json.dumps(command)
        _log.info("{}\n\n".format(command))
        self._gapps.send(self._publish_to_topic, command)

    def _send_resume(self):
        command = {
            "command": "resume"
        }
        command = json.dumps(command)
        _log.info("{}\n\n".format(command))
        self._gapps.send(self._publish_to_topic, command)

    def setup(self):
        if not self._run_realtime:
            self._send_pause()
            _log.info("Pausing to setup")

        self.feeder_name = query_model.get_feeder_name(self.fidselect)
        _log.info("Get Y matrix")
        self._send_simulation_status('STARTED', 'Get Y matrix', 'INFO')
        ybus_config_request = {"configurationType": "YBus Export",
                               "parameters": {"simulation_id":  str(self.simulation_id)}}

        ybus = self._gapps.get_response("goss.gridappsd.process.request.config", ybus_config_request, timeout=360)

        self.AllNodeNames = ybus['data']['nodeList']
        self.node_number = len(self.AllNodeNames)
        self.AllNodeNames = [node.replace('"', '') for node in self.AllNodeNames]
        ybus_data = ybus['data']['yParse']

        self.Ysparse_file = os.path.join(self.MainDir, 'base_no_ysparse_temp.csv')
        with open(self.Ysparse_file, 'w') as f:
            for item in ybus_data:
                f.write("%s\n" % item)

        # self.AllNodeNames = ybus['data']['nodeListFilePath']
        # self.AllNodeNames = [node.replace('"','') for node in self.AllNodeNames]
        # ybus_data = ybus['data']['yParseFilePath']

        # self.ybus_setup()
        # print(self.AllNodeNames)
        _log.info("NodeNames")
        _log.info(self.AllNodeNames)
        self._send_simulation_status('STARTED', "NodeNames " + str(self.AllNodeNames)[:200], 'INFO')

        self._source_node_names = query_model.get_source_node_names(self.fidselect)
        self._source_dict = {}
        for sn in self._source_node_names:
            index = self.AllNodeNames.index(sn)
            self._source_dict[index] = {'name':sn, 'index':index}
        _log.info("Source slack")
        _log.info(self._source_dict )

        self.slack_start = sorted(self._source_dict.keys())[0]
        self.slack_end = sorted(self._source_dict.keys())[-1]

        # [self.Y00, self.Y01, self.Y10, self.Y11, Y11_sparse, self.Y11_inv, Ymatrix] = \
        #     dss_function.construct_Ymatrix_amds_slack(Ysparse_file, self.slack_number, self.slack_start, self.slack_end, self.node_number)

        self._send_simulation_status('STARTED', "Get Base Voltages", 'INFO')

        self.Vbase_allnode = query_model.get_base_voltages(self.AllNodeNames, self.fidselect)
        _log.info('Vbase_allnode')
        _log.info(self.Vbase_allnode[:500])

        self.result, self.name_map, self.node_name_map_va_power, self.node_name_map_pnv_voltage, self.gen_map, \
        self.pec_map, self.load_power_map, self.line_map, self.trans_map, self.switch_pos, self.cap_pos, self.tap_pos, \
        self.load_voltage_map, self.line_voltage_map, self.trans_voltage_map = query_model.lookup_meas(self.fidselect)

        _log.info("Get Regulators")
        self._tap_name_map = {reg['bus'].upper() + '.' + query_model.lookup[reg['phs']]: reg['rname'] for reg in query_model.get_regulator(self.fidselect)}

        _log.info("Get Switches")
        self._switches = query_model.get_switches(self.fidselect)
        self._switch_name_map = {switch['bus1'].upper() + '.' + query_model.lookup[switch['phases'][0]]: switch['name'] for switch in self._switches}

        self._generators = []
        self._generators_dict = {}
        for generator in query_model.get_generator(self.fidselect):
            for phase_index in range(generator['numPhase']):
                phase_name = generator['phases'][phase_index]
                print(generator['bus'].upper(), phase_name)
                name = generator['bus'].upper() + '.' + query_model.lookup[phase_name]
                self._generators.append(name)
                index = self.AllNodeNames.index(name)
                self._generators_dict[index] = {
                    'name': generator['name'],
                    'busname': name,
                    'meas_mrid': self.gen_map[name],
                    'busphase': generator['busphase'],
                    'numPhase': generator['numPhase'],
                    'last_time': {'p': 0.0, 'q': 0.0},
                    'current_time': {'p': 0.0, 'q': 0.0},
                    'polar': {'p': 0.0, 'q': 0.0}
                }

        self._generators_dict = collections.OrderedDict(sorted(self._generators_dict.items()))

        print(json.dumps(self._generators_dict, indent=2))
        _log.info(json.dumps(self._generators_dict, indent=2))

        _log.info("Get PVs")
        self.PVSystem = query_model.get_solar(self.fidselect)
        self.NPV = len(self.PVSystem)
        self._PV_dict = {}
        print(self.AllNodeNames)
        for pv in self.PVSystem:
            for phase_index in range(pv['numPhase']):
                phase_name = pv['phases'][phase_index]
                # print(pv['bus'].upper(), phase_name)
                name = pv['bus'].upper() + '.' + query_model.lookup[phase_name]
                index = self.AllNodeNames.index(name)
                meas_mrid = self.pec_map[name]
                self._PV_dict[index] = {'size': float(pv['kVA']),
                                        'fmrid': pv['pecid'],
                                        'id': pv['id'],
                                        'name': pv['name'],
                                        'busname': name,
                                        'meas_mrid': meas_mrid,
                                        'busphase': pv['busphase'],
                                        'numPhase': pv['numPhase'],
                                        'last_time': {'p': 0.0, 'q': 0.0},
                                        'current_time': {'p': 0.0, 'q': 0.0},
                                        'polar': {'p': 0.0, 'q': 0.0}
                                        }
                # self._PV_last_values[temp_fdrid[index]] = {'p': 0.0, 'q': 0.0}

                # self.nodeIndex_withPV.append(index)
                # self.PV_inverter_size.append(float(pv['kVA']))
        # for i in sorted(temp_index_list):
        #     index = temp_index_list.index(i)
        #     self.nodeIndex_withPV.append(temp_index_list[index])
        #     self.PV_inverter_size.append(temp_PV_inverter_size[index])
        #     self.PV_fdrid.append(temp_fdrid[index])

        self._PV_dict = collections.OrderedDict(sorted(self._PV_dict.items()))

        # print(json.dumps(self._PV_dict, indent=2))
        _log.info(json.dumps(self._PV_dict, indent=2))
        self.nodeIndex_withPV = list(self._PV_dict.keys())
        self.PV_inverter_size = [d[1]['size'] for d in self._PV_dict.items()]

        print(self.nodeIndex_withPV)
        print(self.PV_inverter_size)
        # INFO | [8, 31, 54, 79, 92, 112, 132, 145, 162, 176, 190, 208, 232, 245]
        # INFO | [120.0, 120.0, 250.0, 300.0, 400.0, 150.0, 250.0, 130.0, 260.0, 260.0, 280.0, 150.0, 300.0, 350.0]

        _log.info("PV Node indexes")
        _log.info(self.nodeIndex_withPV)
        _log.info("PV inverer sizes")
        _log.info(self.PV_inverter_size)

        self.Load, total_load = query_model.get_loads_query(self.fidselect)
        self._load_dict = {}

        for pv in self.Load:
            for phase_index in range(pv['numPhase']):
                phase_name = pv['phases'][phase_index]
                name = pv['bus'].upper() + '.' + query_model.lookup[phase_name]
                index = self.AllNodeNames.index(name)
                meas_mrid = self.load_power_map[name]
                self._load_dict[index] = {'name': pv['name'],
                                          'busname': name,
                                          'meas_mrid': meas_mrid,
                                          'busphase': pv['busphase'],
                                          'numPhase': pv['numPhase'],
                                          'phases': pv['phases'],
                                          'constant_currents': pv['constant_currents'],
                                          'phase': phase_name,
                                          'last_time': {'p': 0.0, 'q': 0.0},
                                          'current_time': {'p': 0.0, 'q': 0.0},
                                          'polar': {'p': 0.0, 'q': 0.0}}

        query_history.query_history(1466219984)

        dict_DLMP = {
            # 'Sbus': ['150.1', '150.2', '150.3'],
            'Sbus': self._source_node_names,
            'V0': np.array([1 + 0j, -0.5 - 0.866j, -0.5 + 0.866j]),
            'Iter_method': 'Linear',
            # 'DGbus': ['33.1'],
            'DGbus': ['M1209-DIES1.1', 'M1209-DIES1.2', 'M1209-DIES1.3',
                      'M2001-MT1.1', 'M2001-MT1.2', 'M2001-MT1.3'
                      'M2001-MT2.1', 'M2001-MT2.2', 'M2001-MT2.3'
                      'M2001-MT3.1', 'M2001-MT3.2', 'M2001-MT3.3'],
            'DGbus': [],
            # 'DGbus': self._generators,
            'Add e': True,
            'BaseS': 100,
            'BaseVfile': '../test/ieee123pv_V_base.csv',
            'feeder_name': self.feeder_name,
            'Weight_epsilon': 0.000001
            # 'Weight_epsilon': 10.0
        }

        # Instantiate a DLMP object for IEEE123 system
        self.IEEE123_DLMP = DLMP(dict_DLMP)

        self.IEEE123_plot = Plot_helper(self.IEEE123_DLMP.MainDir)

        # self.IEEE123_DLMP.create_model()
        self.IEEE123_DLMP.pv_indexes = self.nodeIndex_withPV # [58]

        self.IEEE123_DLMP.BaseV = np.array(query_model.get_base_voltages(self.AllNodeNames, self.fidselect))

        # self.IEEE123_DLMP.Ysparse_file = '/Users/jsimpson/git/adms/Grid-Forecasting/532108928/base_ysparse.csv'
        # path_8500 = '/Users/jsimpson/git/adms/Grid-Forecasting/532108928'
        # IEEE123_DLMP.AllNodeNames = []
        # with open(os.path.join(path_8500, 'base_nodelist.csv')) as csvfile:
        #     reader = csv.reader(csvfile, delimiter=',')
        #     for row in reader:
        #         IEEE123_DLMP.AllNodeNames.append(row[0])
        self.IEEE123_DLMP.AllNodeNames = self.AllNodeNames
        self.IEEE123_DLMP.Ysparse_file = self.Ysparse_file
        self.IEEE123_DLMP.slack_number = self.slack_number
        self.IEEE123_DLMP.slack_start = self.slack_start
        self.IEEE123_DLMP.slack_end = self.slack_end

        self._capacitors = query_model.get_capacitors(self.fidselect)
        _log.info("Get Capacitors")
        _log.info(self._capacitors)

        circuit_name = query_model.get_feeder_name(self.fidselect)
        opts = ['OPF']
        opts_str = ""
        for k in opts:
            # if k == 'OPF':
            #     opts_off_str += k + '_0_'
            # else:
            #     opts_off_str += k + '_' + str(self._app_config[k]) + '_'
            opts_str += '_' + k + '_' + str(self._app_config[k])
        opts_str = opts_str.replace('.', '_').replace('-', '_neg_')

        self.resFolder = os.path.join(self.MainDir,
                                      'adms_result_' + circuit_name + '_' + str(self._start_time)) + opts_str
        self.opf_off_folder = os.path.join(self.MainDir,
                                           'adms_result_' + circuit_name + '_' + str(self._start_time)) + "_OPF_0_"
        if self._RTOPF_flag == 0:
            self.resFolder = self.opf_off_folder
        if self._RTOPF_flag == 1:
            _log.info("self.opf_off_folder")
            _log.info(self.opf_off_folder)
            self.PVoutput_P_df = pd.read_csv(os.path.join(self.opf_off_folder, "PVoutput_P.csv"),
                                             index_col='epoch time')
            # self.PVoutput_P_df.index = pd.to_datetime(self.PVoutput_P_df.index, unit='s')
            self.results_0_df = pd.read_csv(os.path.join(self.opf_off_folder, "result.csv"), index_col='epoch time')

        if os.path.exists(self.resFolder):
            try:
                shutil.rmtree(self.resFolder)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))

        if not os.path.exists(self.resFolder):
            os.mkdir(self.resFolder)
        capNames = [cap['name'] for cap in self._capacitors]
        hCapNames = ','.join(capNames)

        headerStats = 'second,epoch time,solar_pct,GHI,Diff,Load Demand (MW),Load Demand (MVAr),' \
                      'Sub Power (MW),Sub Reactive Power (MVar),' \
                      'PVGeneration(MW),PVGeneration(MVAr),' \
                      'Vavg (p.u.),Vmax (p.u.),Vmin (p.u.),' + hCapNames + ',Active Losses(MW)'

        self._result_csv = ResultCSV()
        self._result_csv.create_result_folder(self.resFolder)
        self._result_csv.create_result_file(self.resFolder, 'result.csv', headerStats)

        volt_file = os.path.join(self.resFolder, 'voltage.csv')
        if os.path.exists(volt_file):
            os.remove(volt_file)
        self.vn_file = open(volt_file, 'a')
        self.vn = csv.writer(self.vn_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self.vn.writerow(self.AllNodeNames)

        file_name = os.path.join(self.resFolder, 'DLMP_P.csv')
        if os.path.exists(file_name):
            os.remove(file_name)
        self._DLMP_P_file = open(file_name, 'a')
        self._DLMP_P_writer = csv.writer(self._DLMP_P_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._DLMP_P_writer.writerow(self.AllNodeNames)

        file_name = os.path.join(self.resFolder, 'DLMP_P_B.csv')
        if os.path.exists(file_name):
            os.remove(file_name)
        self._DLMP_P_B_file = open(file_name, 'a')
        self._DLMP_P_B_writer = csv.writer(self._DLMP_P_B_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._DLMP_P_B_writer.writerow(self.AllNodeNames)

        file_name = os.path.join(self.resFolder, 'DLMP_P_Vmag.csv')
        if os.path.exists(file_name):
            os.remove(file_name)
        self._DLMP_P_Vmag_file = open(file_name, 'a')
        self._DLMP_P_Vmag_writer = csv.writer(self._DLMP_P_Vmag_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._DLMP_P_Vmag_writer.writerow(self.AllNodeNames)
        # Q
        file_name = os.path.join(self.resFolder, 'DLMP_Q.csv')
        if os.path.exists(file_name):
            os.remove(file_name)
        self._DLMP_Q_file = open(file_name, 'a')
        self._DLMP_Q_writer = csv.writer(self._DLMP_Q_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._DLMP_Q_writer.writerow(self.AllNodeNames)

        file_name = os.path.join(self.resFolder, 'DLMP_Q_B.csv')
        if os.path.exists(file_name):
            os.remove(file_name)
        self._DLMP_Q_B_file = open(file_name, 'a')
        self._DLMP_Q_B_writer = csv.writer(self._DLMP_Q_B_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._DLMP_Q_B_writer.writerow(self.AllNodeNames)

        file_name = os.path.join(self.resFolder, 'DLMP_Q_Vmag.csv')
        if os.path.exists(file_name):
            os.remove(file_name)
        self._DLMP_Q_Vmag_file = open(file_name, 'a')
        self._DLMP_Q_Vmag_writer = csv.writer(self._DLMP_Q_Vmag_file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        self._DLMP_Q_Vmag_writer.writerow(self.AllNodeNames)

        if not self._run_realtime:
            self._send_resume()
            _log.info("Resuming after setup")

    def on_message(self, headers, message):
        """ Handle incoming messages on the fncs_output_topic for the simulation_id
        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object
            A data structure following the protocol defined in the message structure
            of ``GridAPPSD``.  Most message payloads will be serialized dictionaries, but that is
            not a requirement.
        """
        # Check if we are getting messages
        # _log.info(message)
        # if message == '{}':
        #     return
        _log.info('msg')
        _log.info(type(message))
        _log.info(str(message)[:150])
        if type(message) == str:
            message = json.loads(message)
        if headers['destination'].startswith('/topic/goss.gridappsd.simulation.log.'+str(self.simulation_id)):
            # message = json.loads(message)
            if message['processStatus'] == "COMPLETE":
                print(message)
                self.signal_handler(None, None)
            return
        # print(message)
        # print(message[:100])

        # self._send_simulation_status('STARTED', "Rec message " + str(message)[:100], 'INFO')

        # message = json.loads(message)
        self.timestamp_ = message['message']['timestamp']
        if not message['message']['measurements']:
            print("Measurements is empty")
            return
        self.present_step = self.present_step + 1

        if (self.timestamp_ - 2) % self._run_freq != 0:
            print('Time on the time check. ' + str(self.timestamp_) + ' ' + datetime.
                  fromtimestamp(self.timestamp_, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
            return
        else:
            if not self._run_realtime:
                self._send_pause()
                _log.info("Pausing to work")

        meas_map = message['message']['measurements']
        # meas_map = query_model.get_meas_map(message)

        print("Get Tap Pos")
        # self.tap_pos = query_model.get_pos(meas_map, self.tap_pos)
        # print(self.tap_pos)

        tap_pos = query_model.get_pos(meas_map, self.tap_pos)
        for tn in self._tap_name_map.keys():
            print(tn, self._tap_name_map[tn], tap_pos[tn])

        print("Get Switches Pos")
        switch_pos = query_model.get_pos(meas_map, self.switch_pos)
        for name, value in self._switch_name_map.items():
            print(name, value, switch_pos[name])

        # print(switch_pos)

        print("Get PQ Generator")
        PQ_gen = query_model.get_PQNode(self.AllNodeNames, meas_map, self.gen_map, convert_to_radian=True)
        query_model.get_pv_PQ(self._generators_dict, self.AllNodeNames, meas_map, self.gen_map, convert_to_radian=True)
        PQ_gen = np.array(PQ_gen) / 1000.
        _log.info("PQ_gen")
        _log.info(PQ_gen)
        print(PQ_gen)
        # exit(0)

        print("Get PQ Load")
        PQ_load = query_model.get_PQNode(self.AllNodeNames, meas_map, self.load_power_map, convert_to_radian=True)
        query_model.get_pv_PQ(self._load_dict, self.AllNodeNames, meas_map, self.load_power_map, convert_to_radian=True)
        PQ_load = np.array(PQ_load) / 1000.
        # PV CIM id, Meas ID, TermID,  Name,
        _log.info("PQ_load")
        _log.info(repr(PQ_load))

        total_pq,PQ_load_calc = self.get_demand(self.AllNodeNames, self._load_dict, self.load_power_map, self.load_voltage_map,
                                   meas_map)
        print(total_pq)

        pv_df = pd.DataFrame(self._load_dict)
        pv_df.loc['index'] = pv_df.columns
        pv_df.columns = [d[1]['name'] for d in self._load_dict.items()]
        # print(tabulate(pv_df, headers='keys', tablefmt='psql'))
        _log.info("Load data \n" + tabulate(pv_df, headers='keys', tablefmt='psql'))

        V_node = query_model.get_PQNode(self.AllNodeNames, meas_map, self.node_name_map_pnv_voltage, convert_to_radian=True)
        _log.info(V_node)
        # temp_voltage = self.Vbase_allnode[0]
        # print('Base voltage ')
        # print(temp_voltage,complex(temp_voltage, 0.))
        # exit(0)
        for index, v1 in enumerate(V_node):
            if v1.real == 0:
                temp_voltage = self.Vbase_allnode[index]
                V_node[index] = complex(temp_voltage, 0.)
                _log.info("Zero value for " + self.AllNodeNames[index])
                self._send_simulation_status('RUNNING', "Zero value for node " + self.AllNodeNames[index], 'WARN')

        Ppv, Qpv, pv_mrids = query_model.get_pv_PQ(self._PV_dict, self.AllNodeNames, meas_map, self.pec_map, convert_to_radian=True)
        # print('PQ_load')
        # print(PQ_load.tolist())
        # PQ_load = np.array(Pload) + 1j * np.array(Qload)

        pv_df = pd.DataFrame(self._PV_dict)
        pv_df.loc['index'] = pv_df.columns
        pv_df.columns = [d[1]['name'] for d in self._PV_dict.items()]
        # print(tabulate(pv_df, headers='keys', tablefmt='psql'))
        _log.info("PV data \n" + tabulate(pv_df, headers='keys', tablefmt='psql'))
        PVmax = [v['size'] for k, v in self._PV_dict.items()]
        ## Check for zero pv p
        current_pv = [v['current_time']['p'] for k, v in self._PV_dict.items()]
        PVname = np.array([v['name'] for k, v in self._PV_dict.items()])
        pv_zero_mask = np.array(current_pv) == 0
        if pv_zero_mask.sum() > 0:
            print('Check pv zero')
            print(repr(PVmax))
            print(repr(pv_zero_mask))
            PVmax = np.array(PVmax)
            PVmax[pv_zero_mask] = 0
            print(repr(PVmax))
            PVmax = PVmax.tolist()
            pct_pv_off = pv_zero_mask.sum() / len(PVmax)
            if pv_zero_mask.sum() > 0:
                _log.info('These PV are unavailable ' + repr(PVname[pv_zero_mask]))
            if pct_pv_off < .20:
                # _log.info('The percentage of pv that are unavailable ' + str(pv_zero_mask.sum()) + '/' + len(PVmax) + '=' + pct_pv_off)
                _log.info('The percentage of pv that are unavailable {}/{}={}'.format(pv_zero_mask.sum(),len(PVmax), pct_pv_off))

        source_total = complex(0, 0)
        # for node_name in self._source_node_names:
        #     if node_name in self.line_map:
        #         term = self.line_map[node_name][0]
        #         source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        #     elif node_name in self.trans_map:
        #         term = self.trans_map[node_name]
        #         temp_source_meas = complex(*query_model.pol2cart(meas_map[term]['magnitude'], meas_map[term]['angle']))
        #         source_total += temp_source_meas
        #         # source_total += complex(meas_map[term][u'magnitude'], meas_map[term][u'angle'])
        #         print(node_name, temp_source_meas)
        #     else:
        #         print("Figure out how to do this with transformer ends.")
        # source_total /= 1000000.

        _log.info("pv index")
        _log.info(self._PV_dict.keys())
        Vmeas = np.concatenate((V_node[:self.slack_start], V_node[self.slack_end + 1:]))
        Pmeas = np.concatenate((PQ_load[:self.slack_start], PQ_load[self.slack_end + 1:]))
        Pmeas = np.concatenate((PQ_load[:self.slack_start], PQ_load[self.slack_end + 1:]))
        _log.info("Vmeas")
        _log.info(repr(Vmeas))
        _log.info("V_node")
        _log.info(repr(V_node))
        _log.info("Pmeas")
        _log.info(repr(Pmeas))

        V1_withoutOPF_pu = abs(np.array(V_node)) / np.array(self.Vbase_allnode)
        self.vn.writerow(V1_withoutOPF_pu.tolist())

        # v_violation_mask = list(vv < 0.95 or vv > 1.05 for vv in V1_withoutOPF_pu)
        v_violation_mask = (V1_withoutOPF_pu < 0.95) | (V1_withoutOPF_pu > 1.05)
        # np.array(self.AllNodeNames)[v_violation_mask]
        # np.array(V_node)[v_violation_mask]
        # self.Vbase_allnode[v_violation_mask]
        # V1_withoutOPF_pu[v_violation_mask]

        violators = list(zip(np.array(self.AllNodeNames)[v_violation_mask], np.array(V_node)[v_violation_mask],
                 self.Vbase_allnode[v_violation_mask],V1_withoutOPF_pu[v_violation_mask]))
        violators.sort(key=itemgetter(3))
        if self._the_first_violators == None:
            self._the_first_violators = violators

        print('Violators')
        print(violators[0])
        print(violators[-1])
        _log.info('violators ')
        _log.info(repr(violators))
        self._results['second'] = self.present_step
        self._results['epoch time'] = self.timestamp_
        # self._results['solar_pct'] = solar_pct
        # self._results['GHI'] = ghi
        # self._results['Diff'] = solar_diff


        ### Save data for offline test
        #
        # Vmag = np.absolute(self.V0).tolist()
        # for i in self.model.nodes_nonslack:
        #     Vmag.append(value(self.model.Vmag[i]))
        pd.DataFrame(V1_withoutOPF_pu, index=self.AllNodeNames, columns=['Vmag']).to_csv('Vmag_platform.csv')

        np.savez_compressed('mes_'+self.feeder_name+'night', Vmeas=Vmeas, V_node=V_node, Pmeas=Pmeas, PQ_load=PQ_load)
        Pmeas_calc = np.concatenate((PQ_load_calc[:self.slack_start], PQ_load_calc[self.slack_end + 1:]))
        np.savez_compressed('mes_calc_load_pq' + self.feeder_name + 'night', V_node=V_node, Pmeas=Pmeas_calc, PQ_load=PQ_load_calc)
        ### Done


        DLMP_P_pro =None
        Forecast = True
        if Forecast == True:
            num_iterations = 5
            Sload = Pmeas_calc / -1000.

            Sload = Sload / self.IEEE123_DLMP.BaseS
            self.IEEE123_DLMP.successive_linear_programming(num_iterations, V_node, Sload)

            self.IEEE123_DLMP.record_results()

            # Record the Voltage magnitude data for ploting purpose
            # self.IEEE123_DLMP.record_Vmag()

            # Plot the integrated and decomposed DLMPs
            DLMP_P_pro, DLMP_P_B_pro, DLMP_P_Vmag_pro, DLMP_Q_pro, DLMP_Q_B_pro, DLMP_Q_Vmag_pro = self.IEEE123_plot.plot_DLMPs(None)

            # Plot the voltage magnitude before and after adding DGs to the network
            # IEEE123_plot.plot_Vmag()

            # Plot the vhat convergance
            self.IEEE123_plot.plot_Vhat_convergence(num_iterations, None)

        self._results['Load Demand (MW)'] = total_pq.real * self._vmult * self._vmult
        self._results['Load Demand (MVAr)'] = total_pq.imag * self._vmult * self._vmult
        self._results['Sub Power (MW)'] = source_total.real * self._vmult * self._vmult
        self._results['Sub Reactive Power (MVar)'] = source_total.imag * self._vmult * self._vmult
        self._results['PVGeneration(MW)'] = sum(Ppv) * self._vmult
        self._results['PVGeneration(MVAr)'] = sum(Qpv) * self._vmult
        self._results['Vavg (p.u.)'] = sum(V1_withoutOPF_pu) / self.node_number
        self._results['Vmax (p.u.)'] = max(V1_withoutOPF_pu)
        self._results['Vmin (p.u.)'] = min(V1_withoutOPF_pu)
        # self._results_writer.writerow(self._results)
        # self._res_csvfile.flush()

        self._result_csv.write(self._results)

        self._DLMP_P_writer.writerow(np.insert(pd.read_csv(os.path.join(self.MainDir, 'Result/DLMP_P.csv'))['DLMP_P'].tolist(), 0, self.timestamp_))
        self._DLMP_P_B_writer.writerow(np.insert(pd.read_csv(os.path.join(self.MainDir, 'Result/DLMP_P_B.csv'))['DLMP_P_B'].tolist(), 0, self.timestamp_))
        self._DLMP_P_Vmag_writer.writerow(np.insert(pd.read_csv(os.path.join(self.MainDir, 'Result/DLMP_P_Vmag.csv'))['DLMP_P_Vmag'].tolist(), 0, self.timestamp_))
        # Q
        self._DLMP_Q_writer.writerow(np.insert(pd.read_csv(os.path.join(self.MainDir, 'Result/DLMP_Q.csv'))['DLMP_Q'].tolist(), 0, self.timestamp_))
        self._DLMP_Q_B_writer.writerow(np.insert(pd.read_csv(os.path.join(self.MainDir, 'Result/DLMP_Q_B.csv'))['DLMP_Q_B'].tolist(), 0, self.timestamp_))
        self._DLMP_Q_Vmag_writer.writerow(np.insert(pd.read_csv(os.path.join(self.MainDir, 'Result/DLMP_Q_Vmag.csv'))['DLMP_Q_Vmag'].tolist(), 0, self.timestamp_))

        # self._PV_P_writer.writerow(np.insert(self.res['PV_Poutput'], 0, self.timestamp_))
        # self._PV_Q_writer.writerow(np.insert(self.res['PV_Qoutput'], 0, self.timestamp_))

        result0_df = defaultdict(float)
        if self._RTOPF_flag == 1:
            result0_df = self.results_0_df .iloc[self.results_0_df .index.get_loc(self.timestamp_, method='nearest')]

        plot_time = self.timestamp_
        self.send_plots(None, plot_time, result0_df, None, source_total, V_node, Sload, DLMP_P_pro, DLMP_P_B_pro, DLMP_P_Vmag_pro, meas_map, violators)

        if not self._run_realtime:
            self._send_resume()
            _log.info("Resuming to work")

    def send_plots(self, ghi, plot_time, result0_df, solar_diff, source_total, V_node, Sload, DLMP_P_pro, DLMP_P_B_pro, DLMP_P_Vmag_pro, meas_map, violators):
        obj = {}

        temp_dict = {}
        temp_dict[u'Load Demand (MW)'] = {'data': self._results['Load Demand (MW)'], 'time': plot_time}
        temp_dict[u'Sub Station (MW)'] = {'data': source_total.real, 'time': plot_time}
        obj[u'Load Demand (MW)'] = temp_dict
        temp_dict = {}
        temp_dict[u'V_node 0'] = {'data': V_node[0].real, 'time': plot_time}
        temp_dict[u'V_node 50'] = {'data': V_node[50].real, 'time': plot_time}
        # temp_dict[u'V_node 100'] = {'data': V_node[100].real, 'time': plot_time}
        temp_dict[u'V_node 150'] = {'data': V_node[150].real, 'time': plot_time}
        # temp_dict[u'V_node 384'] = {'data': V_node[384].real, 'time': plot_time}
        # temp_dict[u'V_node 385'] = {'data': V_node[385].real, 'time': plot_time}
        # 384, 385
        # obj[u'V_node'] = temp_dict
        # temp_dict = {}
        # temp_dict[u'Sload 0'] = {'data': Sload[0].real, 'time': plot_time}
        # temp_dict[u'Sload 50'] = {'data': Sload[50].real, 'time': plot_time}
        # # temp_dict[u'Sload 100'] = {'data': Sload[100].real, 'time': plot_time}
        # temp_dict[u'Sload 150'] = {'data': Sload[150].real, 'time': plot_time}
        obj[u'Sload'] = temp_dict

        if len(self.AllNodeNames) > 1000:
            temp_dict_ = {}
            # temp_dict = {}
            # # "78.1"
            # # "78.2"
            # # "78.3"
            # name1 = 'M1108404'
            # name2 = 'L2729414'

            # print (DLMP_P_pro['DLMP_P_1'][50])
            # print(DLMP_P_pro['DLMP_P_1'].keys())
            name1 = self.AllNodeNames[50].split('.')[0]
            name2 = self.AllNodeNames[250].split('.')[0]

            # N1140519.1,0.1982390730877176
            # N1140519.2,0.19707890013380805
            # N1140519.3,0.19758271113940057
            name1 = 'N1140519'
            # new Transformer.hvmv69_11sub1 phases=3 windings=2 xhl=7.940000 %imag=0.000 %noloadloss=0.000
            # ~ normamps=184.08 emergamps=251.02
            # ~ wdg=1 bus=hvmv69sub1_hsb conn=w kv=69.000 kva=20000.0 %r=0.670000
            # ~ wdg=2 bus=regxfmr_hvmv11sub1_lsb conn=w kv=12.470 kva=20000.0 %r=0.670000
            # HVMV69SUB1_HSB.1,0.2000179568015425
            # HVMV69SUB1_HSB.2,0.1998915324737931
            # HVMV69SUB1_HSB.3,0.20008820467098298
            name2 = 'HVMV69SUB1_HSB'


            # name3 = self.AllNodeNames[5000].split('.')[0]


            temp_dict_[name1+'.1'] = {'data': DLMP_P_pro['DLMP_P_1'][name1], 'time': plot_time}
            temp_dict_[name1+'.2'] = {'data': DLMP_P_pro['DLMP_P_2'][name1], 'time': plot_time}
            temp_dict_[name1+'.3'] = {'data': DLMP_P_pro['DLMP_P_3'][name1], 'time': plot_time}
            temp_dict_[name2 + '.1'] = {'data': DLMP_P_pro['DLMP_P_1'][name2], 'time': plot_time}
            temp_dict_[name2 + '.2'] = {'data': DLMP_P_pro['DLMP_P_2'][name2], 'time': plot_time}
            temp_dict_[name2 + '.3'] = {'data': DLMP_P_pro['DLMP_P_3'][name2], 'time': plot_time}
            obj[u'Active Power DLMP $/kW'] = temp_dict_

        elif DLMP_P_pro is not None:
            temp_dict_ = {}
            # if len(violators) > 0:
            #     m = max(violators)[0]
            #     node, phase = m.split('.')
            #     print(max(violators, key=operator.itemgetter(3)))
            #     temp_dict_[m] = {'data': DLMP_P_pro['DLMP_P_'+phase][node], 'time': plot_time}
            temp_dict_v_node = {}

            if len(self._the_first_violators) > 0:
                # self._the_first_violators[-10:] + self._the_first_violators[:10]
                for vio in self._the_first_violators[-10:] + self._the_first_violators[:10]:
                    m = str(vio[0])
                    # print(type(m))
                    node, phase = m.split('.')
                    node_index = self.AllNodeNames.index(m)
                    # print(m , DLMP_P_pro['DLMP_P_'+phase][node] , type(DLMP_P_pro['DLMP_P_'+phase][node]), )
                    temp_dict_[m] = {'data': DLMP_P_pro['DLMP_P_'+phase][node], 'time': plot_time}
                    temp_dict_v_node[u'V_node_'+m] = {'data': vio[3], 'time': plot_time}
            else:
                temp_dict_[u'76.1'] = {'data': DLMP_P_pro['DLMP_P_1']['76'], 'time': plot_time}
                temp_dict_[u'76.2'] = {'data': DLMP_P_pro['DLMP_P_2']['76'], 'time': plot_time}
                temp_dict_[u'75.3'] = {'data': DLMP_P_pro['DLMP_P_3']['76'], 'time': plot_time}
                temp_dict_[u'42.1'] = {'data': DLMP_P_pro['DLMP_P_1']['42'], 'time': plot_time}
                temp_dict_[u'42.2'] = {'data': DLMP_P_pro['DLMP_P_2']['42'], 'time': plot_time}
                temp_dict_[u'42.3'] = {'data': DLMP_P_pro['DLMP_P_3']['42'], 'time': plot_time}
            obj['Violator Voltages'] = temp_dict_v_node
            obj[u'Active Power DLMP $/kW'] = temp_dict_


            # temp_dict[u'113.1'] = {'data': DLMP_P_pro['DLMP_P_1']['113'], 'time': plot_time}
            # temp_dict[u'113.2'] = {'data': DLMP_P_pro['DLMP_P_2']['113'], 'time': plot_time}
            # temp_dict[u'113.3'] = {'data': DLMP_P_pro['DLMP_P_3']['113'], 'time': plot_time}
            # temp_dict[u'114.1'] = {'data': DLMP_P_pro['DLMP_P_1']['114'], 'time': plot_time}
            # temp_dict[u'114.2'] = {'data': DLMP_P_pro['DLMP_P_2']['114'], 'time': plot_time}
            # temp_dict[u'114.3'] = {'data': DLMP_P_pro['DLMP_P_3']['114'], 'time': plot_time}
            # temp_dict[u'S50C.1'] = {'data': DLMP_P_pro['DLMP_P_1']['S50C'], 'time': plot_time}
            # temp_dict[u'S50C.2'] = {'data': DLMP_P_pro['DLMP_P_2']['S50C'], 'time': plot_time}
            # temp_dict[u'S50C.3'] = {'data': DLMP_P_pro['DLMP_P_3']['S50C'], 'time': plot_time}
            # temp_dict[u'S24C.1'] = {'data': DLMP_P_pro['DLMP_P_1']['S24C'], 'time': plot_time}
            # temp_dict[u'S24C.2'] = {'data': DLMP_P_pro['DLMP_P_2']['S24C'], 'time': plot_time}
            # temp_dict[u'S24C.3'] = {'data': DLMP_P_pro['DLMP_P_3']['S24C'], 'time': plot_time}
            # temp_dict[u'150.1'] = {'data': DLMP_P_pro['DLMP_P_1']['150'], 'time': plot_time}
            # temp_dict[u'150.2'] = {'data': DLMP_P_pro['DLMP_P_2']['150'], 'time': plot_time}
            # temp_dict[u'150.3'] = {'data': DLMP_P_pro['DLMP_P_3']['150'], 'time': plot_time}
            # temp_dict[u'250.1'] = {'data': DLMP_P_pro['DLMP_P_1']['250'], 'time': plot_time}
            # temp_dict[u'250.2'] = {'data': DLMP_P_pro['DLMP_P_2']['250'], 'time': plot_time}
            # temp_dict[u'250.3'] = {'data': DLMP_P_pro['DLMP_P_3']['250'], 'time': plot_time}
            # obj[u'Active Power DLMP $/kW'] = temp_dict_

            # temp_dict = {}
            # temp_dict[u'76.1'] = {'data': DLMP_P_B_pro['DLMP_P_B_1']['76'], 'time': plot_time}
            # temp_dict[u'76.2'] = {'data': DLMP_P_B_pro['DLMP_P_B_2']['76'], 'time': plot_time}
            # temp_dict[u'75.3'] = {'data': DLMP_P_B_pro['DLMP_P_B_3']['76'], 'time': plot_time}
            # temp_dict[u'42.1'] = {'data': DLMP_P_B_pro['DLMP_P_B_1']['42'], 'time': plot_time}
            # temp_dict[u'42.2'] = {'data': DLMP_P_B_pro['DLMP_P_B_2']['42'], 'time': plot_time}
            # temp_dict[u'42.3'] = {'data': DLMP_P_B_pro['DLMP_P_B_3']['42'], 'time': plot_time}
            # temp_dict[u'S50C.1'] = {'data': DLMP_P_B_pro['DLMP_P_B_1']['S50C'], 'time': plot_time}
            # temp_dict[u'S50C.2'] = {'data': DLMP_P_B_pro['DLMP_P_B_2']['S50C'], 'time': plot_time}
            # temp_dict[u'S50C.3'] = {'data': DLMP_P_B_pro['DLMP_P_B_3']['S50C'], 'time': plot_time}
            # temp_dict[u'S24C.1'] = {'data': DLMP_P_B_pro['DLMP_P_B_1']['S24C'], 'time': plot_time}
            # temp_dict[u'S24C.2'] = {'data': DLMP_P_B_pro['DLMP_P_B_2']['S24C'], 'time': plot_time}
            # temp_dict[u'S24C.3'] = {'data': DLMP_P_B_pro['DLMP_P_B_3']['S24C'], 'time': plot_time}
            # temp_dict[u'150.1'] = {'data': DLMP_P_B_pro['DLMP_P_B_1']['150'], 'time': plot_time}
            # temp_dict[u'150.2'] = {'data': DLMP_P_B_pro['DLMP_P_B_2']['150'], 'time': plot_time}
            # temp_dict[u'150.3'] = {'data': DLMP_P_B_pro['DLMP_P_B_3']['150'], 'time': plot_time}
            # temp_dict[u'250.1'] = {'data': DLMP_P_B_pro['DLMP_P_B_1']['250'], 'time': plot_time}
            # temp_dict[u'250.2'] = {'data': DLMP_P_B_pro['DLMP_P_B_2']['250'], 'time': plot_time}
            # temp_dict[u'250.3'] = {'data': DLMP_P_B_pro['DLMP_P_B_3']['250'], 'time': plot_time}
            # obj[u'Active Power DLMP Load Balance $/kW'] = temp_dict

            # temp_dict = {}
            # temp_dict[u'76.1'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_1']['76'], 'time': plot_time}
            # temp_dict[u'76.2'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_2']['76'], 'time': plot_time}
            # temp_dict[u'75.3'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_3']['76'], 'time': plot_time}
            # temp_dict[u'42.1'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_1']['42'], 'time': plot_time}
            # temp_dict[u'42.2'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_2']['42'], 'time': plot_time}
            # temp_dict[u'42.3'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_3']['42'], 'time': plot_time}
            # S49a
            # temp_dict[u'S49A.1'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_1']['S49A'], 'time': plot_time}
            # temp_dict[u'S49A.2'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_2']['S49A'], 'time': plot_time}
            # temp_dict[u'S49A.3'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_3']['S49A'], 'time': plot_time}
            # temp_dict[u'S50C.1'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_1']['S50C'], 'time': plot_time}
            # temp_dict[u'S50C.2'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_2']['S50C'], 'time': plot_time}
            # temp_dict[u'S50C.3'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_3']['S50C'], 'time': plot_time}

            # temp_dict[u'S24C.1'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_1']['S24C'], 'time': plot_time}
            # temp_dict[u'S24C.2'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_2']['S24C'], 'time': plot_time}
            # temp_dict[u'S24C.3'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_3']['S24C'], 'time': plot_time}
            # temp_dict[u'150.1'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_1']['150'], 'time': plot_time}
            # temp_dict[u'150.2'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_2']['150'], 'time': plot_time}
            # temp_dict[u'150.3'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_3']['150'], 'time': plot_time}
            # temp_dict[u'250.1'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_1']['250'], 'time': plot_time}
            # temp_dict[u'250.2'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_2']['250'], 'time': plot_time}
            # temp_dict[u'250.3'] = {'data': DLMP_P_Vmag_pro['DLMP_P_Vmag_3']['250'], 'time': plot_time}
            # obj[u'Active Power DLMP Vmag Inequality $/kW'] = temp_dict

        temp_dict = {}
        temp_dict[u'Vmax'] = {'data': self._results['Vmax (p.u.)'], 'time': plot_time}
        # temp_dict[u'Vmax OPF=0'] = {'data': result0_df['Vmax (p.u.)'], 'time': plot_time}
        temp_dict[u'Vavg'] = {'data': self._results['Vavg (p.u.)'], 'time': plot_time}
        # temp_dict[u'Vavg OPF=0'] = {'data': result0_df['Vavg (p.u.)'], 'time': plot_time}
        temp_dict[u'Vmin'] = {'data': self._results['Vmin (p.u.)'], 'time': plot_time}
        # temp_dict[u'Vmin OPF=0'] = {'data': result0_df['Vmin (p.u.)'], 'time': plot_time}
        obj[u'Voltages P.U.'] = temp_dict
        temp_dict = {}
        temp_dict[u'PVGeneration(MW)'] = {'data': self._results['PVGeneration(MW)'], 'time': plot_time}
        # temp_dict[u'PVGeneration(MW) OPF=0'] = {'data': result0_df['PVGeneration(MW)'], 'time': plot_time}
        obj[u'PVGeneration(MW)'] = temp_dict
        temp_dict = {}
        temp_dict[u'PVGeneration(MVAr)'] = {'data': self._results['PVGeneration(MVAr)'], 'time': plot_time}
        # temp_dict[u'PVGeneration(MVAr) OPF=0'] = {'data': result0_df['PVGeneration(MVAr)'], 'time': plot_time}
        obj[u'PVGeneration(MVar)'] = temp_dict

        plot_range = len(self._PV_dict.items())
        if plot_range > 120:
            plot_range = 120
        for count, i in enumerate(range(0, plot_range, self.MAX_LOG_LENGTH)):
            last = i
            temp_pv_dict = dict(islice(self._PV_dict.items(), last, i+self.MAX_LOG_LENGTH))
            temp_dict_pv_p = {pv[1]['name']: {'data': pv[1]['current_time']['p'] * self._vmult, 'time': plot_time} for pv in
                         temp_pv_dict.items()}
            obj[u'Power P (KW) ' + str(count)] = temp_dict_pv_p
            temp_dict_pv_q = {pv[1]['name']: {'data': pv[1]['current_time']['q'] * self._vmult, 'time': plot_time} for pv in
                         temp_pv_dict.items()}
            obj[u'Power P (KVar) ' + str(count)] = temp_dict_pv_q

        # temp_dict = {}
        # temp_dict[u'DirectCH1'] = {'data': ghi, 'time': plot_time}
        # temp_dict[u'Diffuse'] = {'data': solar_diff, 'time': plot_time}
        # obj[u'Weather'] = temp_dict
        # print(repr(temp_dict))
        # temp_pv_dict = dict(islice(self._PV_dict.items(), self.MAX_LOG_LENGTH))
        # temp_dict = {pv[1]['name']: {'data': pv[1]['current_time']['p'] * self._vmult, 'time': plot_time} for pv in
        #              temp_pv_dict.items()}
        # obj[u'Power P (KW)'] = temp_dict

        tap_pos = query_model.get_pos(meas_map, self.tap_pos)
        temp_tap_dict = dict(islice(self._tap_name_map.items(), self.MAX_LOG_LENGTH))
        temp_dict = {pv[1]: {'data': tap_pos[pv[0]], 'time': plot_time} for pv in temp_tap_dict.items()}
        obj[u'Regulator Pos'] = temp_dict

        switch_pos = query_model.get_pos(meas_map, self.switch_pos)
        temp_switch_dict = dict(islice(self._switch_name_map.items(), self.MAX_LOG_LENGTH))
        temp_dict = {pv[1]: {'data': switch_pos[pv[0]], 'time': plot_time} for pv in temp_switch_dict.items()}
        obj[u'Switch Pos'] = temp_dict

        print(repr(obj))
        jobj = json.dumps(obj).encode('utf8')
        zobj = zlib.compress(jobj)
        print('zipped pickle is %i bytes' % len(zobj))
        self._skt.send(zobj)


    def get_demand(self, nodenames, load_name_dict, load_powers, load_voltage_map, meas_map):
        load_results = {}
        total_pq = 0
        PQ_load = [complex(0,0)] * len(nodenames)
        for mrid in load_powers.items():
            if mrid[1] in meas_map:
                # print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]))
                # print(load_name_dict[mrid[1]])
                name = mrid[0]
                index = nodenames.index(name)
                phase_name = load_name_dict[index]['phase']
                # meas_mrid = load_name_dict[index]
                # print(phase_name)
                if 's' in phase_name:
                    temp_voltage = meas_map[mrid[1]]
                    temp_voltage = complex(*query_model.pol2cart(temp_voltage['magnitude'], temp_voltage['angle']))
                    # print(mrid[1], temp_voltage)
                    p = temp_voltage
                    # p = meas_map[mrid[1]]
                    total_pq += p
                    # print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]))
                    load_results[mrid[0]] = {'name': mrid[0], 'mrid': mrid[1], 'power': p, 'index':index, 'name': name}
                    PQ_load[index] = p
                    # print(load_name_dict[mrid[1]])
            else:
                print("No MRID for " + str(mrid))
        for mrid in load_voltage_map.items():
            if mrid[1] in meas_map:
                # load = load_name_volt_dict[mrid[1]]
                name = mrid[0]
                index = nodenames.index(name)
                load = load_name_dict[index]
                phase_name = load['phase']
                if 's' not in phase_name:
                    # print(load['constant_currents'])
                    constant_current = complex(load['constant_currents'][phase_name])
                    temp_voltage = meas_map[mrid[1]]
                    # print(mrid[0], temp_voltage)
                    temp_voltage_cart = complex(*query_model.pol2cart(temp_voltage['magnitude'], temp_voltage['angle']))
                    # print(mrid[1], temp_voltage_cart, phase_name)
                    p = temp_voltage['magnitude'] * constant_current.conjugate()
                    p = p * query_model.phase_shift[phase_name]
                    load_results[mrid[0]] = {'name': mrid[0], 'mrid': mrid[1], 'power': p, 'index':index, 'name': nodenames[index], 'voltage':(temp_voltage['magnitude'], temp_voltage['angle']), 'phase':phase_name}
                    PQ_load[index] = p
                    # print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]) + " " + str(constant_current) + " " + str(p))
                    total_pq += p

        pv_df = pd.DataFrame(load_results)
        _log.info(tabulate(pv_df, headers='keys', tablefmt='psql'))
        _log.info(total_pq)
        print("Total demand " + str(total_pq) + " * scale " +  str(self._load_scale) + " " + str(total_pq*self._load_scale))
        return total_pq, PQ_load

    def save_plots(self):
        """
        Save plots for comparison. Need to have OPF off run first and the OPF on for comparison.
        :return:
        """

        results0 = pd.read_csv(os.path.join(self.resFolder, "result.csv"), index_col='epoch time')
        results0.index = pd.to_datetime(results0.index, unit='s')
        results1 = pd.read_csv(os.path.join(self.resFolder, "result.csv"), index_col='epoch time')
        results1.index = pd.to_datetime(results1.index, unit='s')
        size = (10, 10)
        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        ax.plot(results1[[u'Vmax (p.u.)']])
        ax.plot(results1[[u'Vavg (p.u.)']])
        ax.plot(results1[[u'Vmin (p.u.)']])
        ax.legend(['Vmax (p.u.)', 'Vavg (p.u.)', 'Vmin (p.u.)'])
        fig.savefig(os.path.join(self.resFolder, "Vmax"), bbox_inches='tight')

        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        ax.plot(results1[[u'PVGeneration(MW)']])
        ax.legend(['PV MW'])

        # plt.show()
        fig.savefig(os.path.join(self.resFolder, "PV_Generation_MW"), bbox_inches='tight')
        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        ax.plot(results1[[u'PVGeneration(MVAr)']])
        ax.legend(['PV MVar'])

        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        # Load Demand,forecast time,Forecast Load Demand
        # ax.plot(results0[['Load Demand']].applymap(complex).applymap(np.real))
        ax.plot(results1[['Load Demand (MW)']].applymap(complex).applymap(np.real))
        ax.legend(['Load Demand (MW)'])
        fig.savefig(os.path.join(self.resFolder, "Load Demand (MW)"), bbox_inches='tight')

def _main_local():
    simulation_id = 1466219984
    listening_to_topic = simulation_output_topic(simulation_id)
    sim_request = None
    model_mrid = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D'
    gapps = GridAPPSD(simulation_id)
    der_0 = Grid_Forecast(simulation_id, gapps, model_mrid)
    der_0.setup()


    while der_0.running():
        time.sleep(0.1)


def _main():
    from gridappsd import utils
    _log.info("Starting application")
    _log.info("Run local only -JEFF")
    _log.info("Args ")
    for arg in sys.argv[1:]:
        _log.info(type(arg))
        _log.info(arg)
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="Simulation Request")
    parser.add_argument("opt",
                        help="opt")

    opts = parser.parse_args()
    _log.info(opts)
    listening_to_topic = simulation_output_topic(opts.simulation_id)
    log_topic = simulation_log_topic(opts.simulation_id)
    print(listening_to_topic)
    print(log_topic)

    sim_request = json.loads(opts.request.replace("\'",""))
    model_mrid = sim_request["power_system_config"]["Line_name"]
    start_time = sim_request['simulation_config']['start_time']
    app_config = sim_request["application_config"]["applications"]
    # app_config = [json.loads(app['config_string']) for app in app_configs if app['name'] == 'grid_forecasting_app'][0]
    app = [app for app in app_config if app['name'] == 'grid_forecasting_app'][0]
    if app['config_string']:
        app_config = json.loads(app['config_string'].replace(u'\u0027', '"'))
    else:
        app_config = {'run_freq': 60, 'run_on_host': False}
    _log.info(app_config['run_on_host'])
    ## Run the docker container. WOOT!
    if 'run_on_host' in app_config and app_config['run_on_host']:
        exit(0)

    _log.info("Model mrid is: {}".format(model_mrid))
    gapps = GridAPPSD(opts.simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())

    der_0 = Grid_Forecast(opts.simulation_id, gapps, model_mrid, start_time, app_config)
    der_0.setup()
    gapps.subscribe(listening_to_topic, der_0)
    gapps.subscribe(log_topic, der_0)

    while der_0.running():
        time.sleep(0.1)

def running_on_host():
    __GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
    if __GRIDAPPSD_URI__ == 'localhost:61613':
        return True
    return False


if __name__ == '__main__':
    if running_on_host():
        _main_local()
    else:
        _main()
