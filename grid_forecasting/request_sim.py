import json
import time
from time import strptime, strftime, mktime, gmtime
from calendar import timegm
import argparse
from gridappsd import GOSS
from main_app import Grid_Forecast

from gridappsd import GridAPPSD
from gridappsd.topics import simulation_output_topic, simulation_log_topic

goss_sim = "goss.gridappsd.process.request.simulation"
test_topic = 'goss.gridappsd.test'
responseQueueTopic = '/temp-queue/response-queue'
goss_simulation_status_topic = '/topic/goss.gridappsd/simulation/status/'


def _startTest(username,password,gossServer='localhost',stompPort='61613', simulationID=1234, rulePort=5000, topic="input"):
    req_template = {"power_system_config": {"SubGeographicalRegion_name": "_1CD7D2EE-3C91-3248-5662-A43EFEFAC224",
                                            "GeographicalRegion_name": "_24809814-4EC6-29D2-B509-7F8BFB646437",
                                            "Line_name": "_C1C3E687-6FFD-C753-582B-632A27E28507"},
                    "simulation_config": {"power_flow_solver_method": "NR",
                                          "duration": 120,
                                          "simulation_name": "ieee123",
                                          "simulator": "GridLAB-D",
                                          "start_time": 1248156000,
                                          "run_realtime": True,
                                          "timestep_frequency": "1000",
                                          "timestep_increment": "1000",
                                          "model_creation_config": {"load_scaling_factor": 1.0, "triplex": "y",
                                                                    "encoding": "u", "system_frequency": 60,
                                                                    "voltage_multiplier": 1.0,
                                                                    "power_unit_conversion": 1.0, "unique_names": "y",
                                                                    "schedule_name": "ieeezipload", "z_fraction": "0",
                                                                    "i_fraction": "0", "p_fraction": "1",
                                                                    "randomize_zipload_fractions": False,
                                                                    "use_houses": False},
                                          "simulation_broker_port": 52798, "simulation_broker_location": "127.0.0.1"},
                    "application_config": {"applications": [{"name": "der_dispatch_app", "config_string": "{}"}]},
                    "simulation_request_type": "NEW","test_config": {"events": []}}

    # _B221C5F6-E08A-BC62-91DE-ADC1E2F8FF96
    sw3_event = {
            "message": {
                "forward_differences": [
                    {
                        "object": "_B221C5F6-E08A-BC62-91DE-ADC1E2F8FF96",
                        "attribute": "Switch.open",
                        "value": 1
                    }
                ],
                "reverse_differences": [
                    {
                        "object": "_B221C5F6-E08A-BC62-91DE-ADC1E2F8FF96",
                        "attribute": "Switch.open",
                        "value": 0
                    }
                ]
            },
            "event_type": "ScheduledCommandEvent",
            "occuredDateTime": 1374258660 + (4*60),
            "stopDateTime": 1374258660 + (8*60)
        }
    pv_84_90_event = {
        "allOutputOutage": False,
        "allInputOutage": False,
        "inputOutageList": [{"objectMRID": "_233D4DC1-66EA-DF3C-D859-D10438ECCBDF", "attribute": "PowerElectronicsConnection.p"},
                            {"objectMRID": "_233D4DC1-66EA-DF3C-D859-D10438ECCBDF", "attribute": "PowerElectronicsConnection.q"},
                            {"objectMRID": "_60E702BC-A8E7-6AB8-F5EB-D038283E4D3E", "attribute": "PowerElectronicsConnection.p"},
                            {"objectMRID": "_60E702BC-A8E7-6AB8-F5EB-D038283E4D3E", "attribute": "PowerElectronicsConnection.q"},
                            ],
        "outputOutageList": ['_a5107987-1609-47b2-8f5b-f91f99658390', '_2c4e0cb2-4bf0-4a2f-be94-83ee9b87d1e5'],
        "event_type": "CommOutage",
        "occuredDateTime": 1374510600 + (5*60),
        "stopDateTime": 1374510600 + (10*60)
    }

    pv_84_90_event = {
        "allOutputOutage": False,
        "allInputOutage": False,
        "inputOutageList": [{"objectMRID": "_EAE0584D-6B67-2F23-FC02-E3F2C8C6A48D", "attribute": "PowerElectronicsConnection.p"},
                            {"objectMRID": "_EAE0584D-6B67-2F23-FC02-E3F2C8C6A48D", "attribute": "PowerElectronicsConnection.q"},
                            {"objectMRID": "_73E7B579-37DB-B7F2-EBC6-D083E8BBA1F3", "attribute": "PowerElectronicsConnection.p"},
                            {"objectMRID": "_73E7B579-37DB-B7F2-EBC6-D083E8BBA1F3", "attribute": "PowerElectronicsConnection.q"},
                            ],
        "outputOutageList": ['_b8442bbd-4d3e-4b2e-884e-96639bb207bc', '_b005322e-7dba-48d3-b6ce-f6fe57c4dd61'],
        "event_type": "CommOutage",
        "occuredDateTime": 1374510600 + (5*60),
        "stopDateTime": 1374510600 + (10*60)
    }



    # {"applications": [{"name": "der_dispatch_app", "config_string": ""}]}
    req_template['simulation_config']['model_creation_config']['load_scaling_factor'] = 1
    req_template['simulation_config']['run_realtime'] = False
    req_template['simulation_config']['duration'] = 60 * 15

    req_template['simulation_config']['start_time'] = 1538510000
    req_template['simulation_config']['start_time'] = 1374498000  # 7/22/2013 What I was doing
    req_template['simulation_config']['start_time'] = 1374510600  # GMT: Monday, July 22, 2013 4:30:00 PM
    req_template['simulation_config']['start_time'] = 1374483600 # 3:00 am
    # req_template['simulation_config']['start_time'] = 1374519600  # (GMT): Monday, July 22, 2013 7:00:00 PM
    req_template['simulation_config']['start_time'] = 1374530400  # (GMT): Monday, July 22, 2013 10:00:00 PM Cause
    req_template['simulation_config']['start_time'] = 1374240600 # July 19 13:30 AM GMT / 7:30 AM MST
    # req_template['simulation_config']['start_time'] = 1374249600  #  July 19, 2013 4:00:00 PM
    req_template['simulation_config']['start_time'] = 1374258600  # July 19, 2013 6:30:00 PM  - 2013-07-19 18:30:00 / 12:30 PM MST  # MAX PV!!!
    req_template['simulation_config']['start_time'] = 1248156000  # Tuesday, July 21, 2009 6:00:00 AM
    req_template['simulation_config']['start_time'] = 1248192000  # Tuesday, July 21, 2009 4:00:00 PM / 10:00:00 AM
    req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 15:03:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))

    req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 00:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 08:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    # req_template['simulation_config']['start_time'] = timegm(strptime('2009-07-21 12:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    req_template['simulation_config']['start_time'] = timegm(strptime('2019-07-22 12:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))
    req_template['simulation_config']['start_time'] = timegm(strptime('2019-07-23 14:50:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))

    pv_84_90_event["occuredDateTime"] = req_template['simulation_config']['start_time'] + (2*60)
    pv_84_90_event["stopDateTime"]    = req_template['simulation_config']['start_time'] + (4*60)
    req_template["test_config"]["events"].append(pv_84_90_event)

    # sw3_event
    sw3_event["occuredDateTime"] = req_template['simulation_config']['start_time'] + (7*60)
    sw3_event["stopDateTime"]    = req_template['simulation_config']['start_time'] + (9*60)
    # req_template["test_config"]["events"].append(sw3_event)

    app_config = {'OPF': 0,
                  'run_freq': 60,
                  'run_on_host': True,
                  'run_realtime':req_template['simulation_config']['run_realtime'],
                  'historical_run':875592221}  #30
    app_config['historical_run'] = 0  # Turn Off
    req_template["application_config"]["applications"] = [{"name": "grid_forecasting_app", "config_string": json.dumps(app_config)}]

    # GMT: Tuesday, October 2, 2018 4:50:00 PM
    # Your time zone: Tuesday, October 2, 2018 10:50:00 AM GMT-06:00 DST
    req_template['power_system_config']['Line_name'] = '_C1C3E687-6FFD-C753-582B-632A27E28507'
    req_template['power_system_config']['Line_name'] = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D'  # Mine 123pv
    # req_template['power_system_config']['Line_name'] = '_EBDB5A4A-543C-9025-243E-8CAD24307380'  # 123 with reg

    req_template['power_system_config']['Line_name'] = '_DA00D94F-4683-FD19-15D9-8FF002220115'  # mine with house
    # req_template['power_system_config']['Line_name'] = '_88B3A3D8-51CD-7466-5E29-B692F21723CB' # Mine with feet conv
    # req_template['power_system_config']['Line_name'] = '_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62'  # 13
    # req_template['power_system_config']['Line_name'] = '_C1C3E687-6FFD-C753-582B-632A27E28507'  # 123
    # req_template['power_system_config']['Line_name'] = '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44'  # New 8500
    # req_template['power_system_config']['Line_name'] = '_AAE94E4A-Jeff'

    # req_template['power_system_config']['Line_name'] = '_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3'
    req_template["application_config"]["applications"][0]['name'] = 'grid_forecasting_app'

    simCfg13pv = json.dumps(req_template)
    print(simCfg13pv)
    # exit(0)
    goss = GOSS()
    goss.connect()

    simulation_id = goss.get_response(goss_sim, simCfg13pv, timeout=200)
    simulation_id = int(simulation_id['simulationId'])
    print(simulation_id)
    print('sent simulation request')
    time.sleep(1)

    if app_config['run_on_host']:
        listening_to_topic = simulation_output_topic(simulation_id)
        log_topic = simulation_log_topic(simulation_id)
        model_mrid = req_template['power_system_config']['Line_name']
        start_time = req_template['simulation_config']['start_time']
        app_configs = req_template["application_config"]["applications"]
        app_config = [json.loads(app['config_string']) for app in app_configs if app['name'] == 'grid_forecasting_app'][0]

        gapps = GridAPPSD(simulation_id)
        load_scale = req_template['simulation_config']['model_creation_config']['load_scaling_factor']
        app = Grid_Forecast(simulation_id, gapps, model_mrid, start_time, app_config, load_scale)
        app.setup()
        gapps.subscribe(listening_to_topic, app)
        # gapps.subscribe(log_topic, app)

        while True:
            time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--topic", type=str, help="topic, the default is input", default="input", required=False)
    parser.add_argument("-p","--port", type=int, help="port number, the default is 5000", default=5000, required=False)
    parser.add_argument("-i", "--id", type=int, help="simulation id", required=False)
    # parser.add_argument("--start_date", type=str, help="Simulation start date", default="2017-07-21 12:00:00", required=False)
    # parser.add_argument("--end_date", type=str, help="Simulation end date" , default="2017-07-22 12:00:00", required=False)
    # parser.add_argument('-o', '--options', type=str, default='{}')
    args = parser.parse_args()

    _startTest('system','manager',gossServer='127.0.0.1',stompPort='61613', simulationID=args.id, rulePort=args.port, topic=args.topic)
