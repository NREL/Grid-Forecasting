
import os
import sys
import json
import pandas as pd
from collections import defaultdict
from tabulate import tabulate
import query_model_adms as query_model
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD


__GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
print (__GRIDAPPSD_URI__)
if __GRIDAPPSD_URI__ == 'localhost:61613':
    gridappsd_obj = GridAPPSD(1234)
else:
    from gridappsd import utils
    # gridappsd_obj = GridAPPSD(simulation_id=1234, address=__GRIDAPPSD_URI__)
    print (utils.get_gridappsd_address())
    gridappsd_obj = GridAPPSD(1069573052, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())
goss_sim = "goss.gridappsd.process.request.simulation"
timeseries = 'goss.gridappsd.process.request.data.timeseries'

def get_meas_dict(historical_results,convert2rect=True):
    meas_dict = defaultdict(dict)
    pos_dict = defaultdict(dict)
    diff_dict = defaultdict(dict)
    if 'data' in historical_results:
        for row in historical_results['data']['measurements'][0]['points']:
            temp = {entry['key']: entry['value'] for entry in row['row']['entry']}
            time_of_meas = int(temp['time'])
            if 'measurement_mrid' in temp and 'angle' in temp:
                measurement_mrid = temp['measurement_mrid']
                if convert2rect:
                    meas_dict[time_of_meas][measurement_mrid] = complex(*query_model.pol2cart(float(temp['magnitude']), float(temp['angle'])))
                else:
                    meas_dict[time_of_meas][measurement_mrid] = complex(float(temp['magnitude']), float(temp['angle']))
            elif 'measurement_mrid' in temp and 'value' in temp:
                measurement_mrid = temp['measurement_mrid']
                pos_dict[time_of_meas][measurement_mrid] = temp['value']
            elif 'difference_mrid' in temp:
                difference_mrid = temp['difference_mrid']
                diff_dict[time_of_meas][difference_mrid] = temp
            else:
                print(temp)
    return meas_dict, pos_dict, diff_dict

def query_history(simulation_id=182942650, start_time=0, end_time=0):
    start_time = start_time + (0 * 60 * 60)
    start_time=start_time*1000000
    end_time=end_time*1000000
    query = {"queryMeasurement": "PROVEN_MEASUREMENT",
     "queryFilter": {"simulation_id": "751004304"
         # ,
         #             "startTime": "1357048800000000",
         #             "endTime": "1357048860000000"
                     },
     "responseFormat": "JSON"}
    query['queryFilter']['simulation_id'] = simulation_id
    # query['queryFilter']['startTime'] = start_time
    # query['queryFilter']['endTime'] = end_time
    print(query)

    historical_results = gridappsd_obj.get_response(timeseries, query, timeout=360)
    # print(historical_results)
    with open('data.json', 'w') as fp:
        json.dump(historical_results, fp)

    return historical_results

def get_meas_maps(sim_id = 1370702498, fidselect=u'_C1C3E687-6FFD-C753-582B-632A27E28507', start_time=0, end_time=0):
    # sim_id = 1483458967
      # A 13 node run
    # fidselect = u'_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62' # 13

    result, name_map, node_name_map_va_power, node_name_map_pnv_voltage, pec_map, load_power_map, line_map, trans_map,\
    cap_pos, tap_pos, load_voltage_map, line_voltage_map, trans_voltage_map = query_model.lookup_meas(fidselect)
    historical_results = query_history(sim_id, start_time, end_time)
    meas_dict, pos_dict, diff_dict = get_meas_dict(historical_results, convert2rect=True)
    loads, total_load = query_model.get_loads_query(fidselect)
    # print(json.dumps(loads,indent=2))

    load_name_dict={}
    load_name_volt_dict = {}
    for load in loads:
        for phase_name in load['phases']:
            # print(load['bus'].upper(), phase_name)
            name = load['bus'].upper() + '.' + query_model.lookup[phase_name]
            datum = {
                'name': load['name'],
                'busname': name,
                # 'meas_mrid': meas_mrid,
                'busphase': load['busphase'],
                'numPhase': load['numPhase'],
                'phases': load['phases'],
                'constant_currents': load['constant_currents'],
                'phase':phase_name
            }
            load_name_dict[load_power_map[name]] = datum
            load_name_volt_dict[load_voltage_map[name]] = datum

    print(tabulate_string(load_name_dict))
    source_node_names = query_model.get_source_node_names(fidselect)
    loads_dict = {}
    for time in sorted(meas_dict.keys()):

        meas_map = meas_dict[time]
        total_pq = 0
        # Names are needed for Y matrix
        load_powers = load_power_map.items()

        total_pq = getDemand(load_name_dict, load_name_volt_dict, load_powers, load_voltage_map, meas_map)

        source_total = complex(0, 0)
        for node_name in source_node_names:
            if node_name in line_map:
                term = line_map[node_name][0]
                source_total += meas_map[term]
            elif node_name in trans_map:
                term = trans_map[node_name]
                # print("Voltage for " + node_name, trans_voltage_map[node_name], meas_map[trans_voltage_map[node_name]])
                # print("Power for " + node_name, meas_map[term])
                source_total += meas_map[term]
            else:
                print("Figure out how to do this with transformer ends.")

            # PQ_load = query_model.get_PQNode(self.AllNodeNames, meas_map, self.load_power_map, convert_to_radian=False)
            # query_model.get_pv_PQ(self._load_dict, self.AllNodeNames, meas_map, self.load_power_map, convert_to_radian=False)

        # print(total_pq, source_total)
        # print(tabulate_string(load_results))
        loads_dict[time] = total_pq
        # break

    # Process data to be one minute data with x number minute 1 dim arrays.
    hist_load_df = pd.DataFrame.from_dict(loads_dict, orient='index')
    hist_load_df.index = pd.to_datetime(hist_load_df.index, unit='s')
    hist_load_df_1min = hist_load_df[0].resample('min').mean()
    print(hist_load_df.shape)
    print(hist_load_df_1min.real.reshape((-1, 1)))
    return hist_load_df_1min.real.reshape((-1, 1))


def getDemand(load_name_dict, load_name_volt_dict, load_powers, load_voltage_map, meas_map):
    '''
    Caculate the demand from the loads and secondary loads, the ones with 's' in the phase.
    Use the abs value or magnitude of the voltage times the constant current.
    :param load_name_dict:
    :param load_name_volt_dict:
    :param load_powers:
    :param load_voltage_map:
    :param meas_map:
    :return total_pq:
    '''
    load_results = {}
    total_pq = 0
    for name, mrid in load_powers:
        if mrid in meas_map:
            phase_name = load_name_dict[mrid]['phase']
            if 's' in phase_name:
                p = meas_map[mrid]
                total_pq += p
                print(name + " " + mrid + " " + str(meas_map[mrid]))
                load_results[name] = {'name': name, 'mrid': mrid, 'power': p}
        else:
            print("No MRID for " + str(mrid))
    for name, mrid in load_voltage_map.items():
        if mrid in meas_map:
            load = load_name_volt_dict[mrid]
            phase_name = load['phase']
            if 's' not in phase_name:
                constant_current = complex(load['constant_currents'][phase_name])
                print(mrid, meas_map[mrid])
                voltage = abs(meas_map[mrid])
                p = voltage * constant_current.conjugate()
                p = p * query_model.phase_shift[phase_name]
                load_results[name] = {'name': name, 'mrid': mrid, 'power': p}
                # print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]) + " " + str(constant_current) + " " + str(p))
                total_pq += p

        # for load in loads[0]:
        #     for phase_name in load['phases']:
        #         name = load['bus'].upper() + '.' + query_model.lookup[phase_name]
        #         if name == mrid[0]:
        #             constant_current = complex(load['constant_currents'][phase_name])
        #             p = meas_map[mrid[1]] * constant_current
        #             p = p.conjugate() * query_model.phase_shift[phase_name]
        #             load_results[mrid[0]] = {'mrid': mrid[1], 'power': p}
        #             print(mrid[0] + " " + mrid[1] + " " + str(meas_map[mrid[1]]) + " " + str(constant_current) + " " + str(p))
        # total_pq += p
    print(total_pq)
    return total_pq


def tabulate_string(_dict):
    pv_df = pd.DataFrame(_dict)
    # pv_df.loc['index'] = pv_df.columns
    #     # pv_df.columns = [d[1]['name'] for d in _dict.items()]
    return tabulate(pv_df, headers='keys', tablefmt='psql')


if __name__ == '__main__':
    sim_id = 1483458967
    sim_id = 1343477253
    sim_id = 875592221
    fidselect = u'_4F76A5F9-271D-9EB8-5E31-AA362D86F2C3'
    fidselect = '_EBDB5A4A-543C-9025-243E-8CAD24307380'
    historical_results = query_history(sim_id, 1248156000, 1248242400)
    meas_dict, pos_dict, diff_dict = get_meas_dict(historical_results, convert2rect=True)
    # print(meas_dict)
    get_meas_maps(sim_id=sim_id, fidselect=fidselect)

    # sim_id=936498831
    # historical_results = query_history(sim_id, 1248156000, 1248242400)
    # meas_dict, pos_dict, diff_dict = get_meas_dict(historical_results, convert2rect=True)
    # get_meas_maps(sim_id,'_EBDB5A4A-543C-9025-243E-8CAD24307380')


    # df.to_csv("meas_"+str(sim_id)+".csv")
    # query_weather(1357048800000000, 1357058860000000)