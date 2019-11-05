

## NREL Feeder additions

```bash
python ListFeeders.py
python ListMeasureables.py ieee123_pv_reg _EBDB5A4A-543C-9025-243E-8CAD24307380    
python InsertMeasurements.py ieee123_pv_reg_loads.txt
python InsertMeasurements.py ieee123_pv_reg_node_v.txt
python InsertMeasurements.py ieee123_pv_reg_special.txt
python InsertMeasurements.py ieee123_pv_reg_switch_i.txt
python InsertMeasurements.py ieee123_pv_reg_xfmr_pq.txt
python InsertMeasurements.py ieee123_pv_reg_lines_pq.txt
```

```bash
python ListFeeders.py
python ListMeasureables.py ieee123_pv_reg_house _DA00D94F-4683-FD19-15D9-8FF002220115    
python InsertMeasurements.py ieee123_pv_reg_house_loads.txt
python InsertMeasurements.py ieee123_pv_reg_house_node_v.txt
python InsertMeasurements.py ieee123_pv_reg_house_special.txt
python InsertMeasurements.py ieee123_pv_reg_house_switch_i.txt
python InsertMeasurements.py ieee123_pv_reg_house_xfmr_pq.txt
python InsertMeasurements.py ieee123_pv_reg_house_lines_pq.txt
```

```bash
python ListMeasureables.py test9500new_jeff _AAE94E4A-Jeff
python InsertMeasurements.py test9500new_jeff_loads.txt
python InsertMeasurements.py test9500new_jeff_node_v.txt
python InsertMeasurements.py test9500new_jeff_special.txt
python InsertMeasurements.py test9500new_jeff_switch_i.txt
python InsertMeasurements.py test9500new_jeff_xfmr_pq.txt
python InsertMeasurements.py test9500new_jeff_lines_pq.txt
python InsertMeasurements.py test9500new_jeff_machines.txt

```

```bash
python ListMeasureables.py test9500new_jeff_off_gen _AAE94E4A-Jeff-off-gen
python InsertMeasurements.py test9500new_jeff_off_gen_loads.txt
python InsertMeasurements.py test9500new_jeff_off_gen_node_v.txt
python InsertMeasurements.py test9500new_jeff_off_gen_special.txt
python InsertMeasurements.py test9500new_jeff_off_gen_switch_i.txt
python InsertMeasurements.py test9500new_jeff_off_gen_xfmr_pq.txt
python InsertMeasurements.py test9500new_jeff_off_gen_lines_pq.txt
python InsertMeasurements.py test9500new_jeff_off_gen_machines.txt

```




Docker cp commands
```bash
#docker cp
cp /Users/jsimpson/git/adms/gridappsd-docker-upstream/DER-Dispatch-app/updated_glm2/531088114 gridappsd-docker_gridappsd_1:/tmp/gridappsd_tmp
docker exec -u root  -it gridappsd-docker_gridappsd_1 chown -R gridappsd:gridappsd /tmp/gridappsd_tmp/531088114
docker exec -it -u root df3a37a776e2 chown -R gridappsd:gridappsd /tmp/gridappsd_tmp/1721908100/
 
```

```bash
# fncs
docker cp services/fncsgossbridge/service/fncs_goss_bridge.py  gridappsd-docker_gridappsd_1:/gridappsd/services/fncsgossbridge/service/

```
### Expected Results
 pip install git+git://github.com/GRIDAPPSD/gridappsd-python.git --upgrade
```


```
mysql -u gridappsd -pgridappsd1234
use gridappsd;

select * from log where source = 'fncs_goss_bridge.py' and log_message LIKE '%phase_%';
select * from log where source = 'fncs_goss_bridge.py' and log_message LIKE '%commun%';
select * from log where timestamp >= now() - INTERVAL 1 HOUR and source = 'ProcessEvents' or source = 'TestManagerImpl' ;

select * from log where timestamp >= now() - INTERVAL 1 HOUR and source = 'ProcessEvent' ;

```

```bash
python -m py_compile DLMP.py 
cp __pycache__/DLMP.cpython-37.pyc /Users/jsimpson/git/adms/DER-Dispatch-app-Public/der_dispatch_app/__pycache__/DLMP.pyc 
cp __pycache__/DLMP.cpython-37.pyc /Users/jsimpson/git/adms/DER-Dispatch-app-Public/der_dispatch_app/DLMP.pyc 
cp __pycache__/DLMP.cpython-37.pyc /Users/jsimpson/git/adms/DER-Dispatch-app-Public/der_dispatch_app/__pycache__/DLMP.cpython-37.pyc 


```