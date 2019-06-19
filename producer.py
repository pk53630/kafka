#!/usr/local/bin/python3

import sys
import os
import cx_Oracle
import mysql.connector
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import arrow
import re
import json
from kafka import KafkaProducer
from bson import json_util
from kafka import KafkaConsumer


div_id = sys.argv[1]
#pwd = sys.argv[1]
#sid = sys.argv[1]

dt=arrow.now().format('YYYY-MM-DD')
filename="""/home/odmbatch/odm_kafka/Logs/oracle_to_memSQL_producer_Div10_"""+dt+""".log"""

my_file = Path(filename)
if (not my_file.is_file()):
    # file not exists
    #print("file is not present")
    Path(filename).touch()


log_format = "%(asctime)s - %(levelname)s - %(message)s"
log_level = 10
handler = TimedRotatingFileHandler(filename, when="midnight", interval=1)
handler.setLevel(log_level)
formatter = logging.Formatter(log_format)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# add a suffix which you want
handler.suffix = "%Y-%m-%d %HH:%M:%S"

#need to change the extMatch variable to match the suffix for it
handler.extMatch = re.compile(r"^\d{8}$")

# finally add handler to logger
logger.addHandler(handler)

logger.info("                                                                                                                ")
logger.info("################################################################################################################")
logger.info("##################################### ODM WELL DELTA EXTACTION STARTED #########################################")
logger.info("################################################################################################################")
logger.info("                                                                                                                ")


producer = KafkaProducer(bootstrap_servers=["ktyprdkafka01.abc.com:9092","ktyprdkafka02.abc.com:9092","ktyprdkafka03.abc.com:9092"])

connection = cx_Oracle.connect('odm_dba/xxxxx@r1date.abc.com')
cursor = cx_Oracle.Cursor(connection)

qry="""select
DIVISION_ID as DIVISION_ID,
COMPLETION_ID as COMPLETION_ID,
DATE_ID	as DATE_ID,
to_char(DATE_VALUE,'YYYY-MM-DD HH24:MI:SS') as DATE_VALUE,
nvl(to_char(GROSS_GAS_PROD),'NULL') as GROSS_GAS_PROD,
nvl(to_char(GROSS_OIL_PROD),'NULL') as GROSS_OIL_PROD,
nvl(to_char(WATER_PROD),'NULL') as WATER_PROD,
nvl(to_char(GROSS_GAS_SALES),'NULL') as GROSS_GAS_SALES,
nvl(to_char(GROSS_OIL_SALES),'NULL') as GROSS_OIL_SALES,
nvl(to_char(POTENTIAL_GAS_PROD),'NULL') as POTENTIAL_GAS_PROD,
nvl(to_char(POTENTIAL_OIL_PROD),'NULL') as POTENTIAL_OIL_PROD,
nvl(to_char(POTENTIAL_WATER_PROD),'NULL') as POTENTIAL_WATER_PROD,
nvl(to_char(FORECAST_GAS_PROD),'NULL') as FORECAST_GAS_PROD,
nvl(to_char(FORECAST_OIL_PROD),'NULL') as FORECAST_OIL_PROD,
nvl(to_char(FORECAST_WATER_PROD),'NULL') as FORECAST_WATER_PROD,
nvl(to_char(FORECAST_NGL_PROD),'NULL') as FORECAST_NGL_PROD,
nvl(to_char(FORECAST_COND_PROD),'NULL') as FORECAST_COND_PROD,
nvl(to_char(FORECAST_GAS_SALES),'NULL') as FORECAST_GAS_SALES,
nvl(to_char(FORECAST_OIL_SALES),'NULL') as FORECAST_OIL_SALES,
nvl(to_char(FORECAST_WATER_SALES),'NULL') as FORECAST_WATER_SALES,
nvl(to_char(FORECAST_NGL_SALES),'NULL') as FORECAST_NGL_SALES,
nvl(to_char(FORECAST_COND_SALES),'NULL') as FORECAST_COND_SALES,
nvl(to_char(FORECAST_NET_GAS_SALES),'NULL') as FORECAST_NET_GAS_SALES,
nvl(to_char(FORECAST_NET_OIL_SALES),'NULL') as FORECAST_NET_OIL_SALES,
nvl(to_char(FORECAST_NET_WATER_SALES),'NULL') as FORECAST_NET_WATER_SALES,
nvl(to_char(FORECAST_NET_NGL_SALES),'NULL') as FORECAST_NET_NGL_SALES,
nvl(to_char(FORECAST_NET_COND_SALES),'NULL') as FORECAST_NET_COND_SALES,
nvl(to_char(GAS_INJ),'NULL') as GAS_INJ,
nvl(to_char(OIL_INJ),'NULL') as OIL_INJ,
nvl(to_char(WATER_INJ),'NULL') as WATER_INJ,
nvl(to_char(CHOKE),'NULL') as CHOKE,
nvl(to_char(CASING_PRESSURE1),'NULL') as CASING_PRESSURE1,
nvl(to_char(CASING_PRESSURE2),'NULL') as CASING_PRESSURE2,
nvl(to_char(TUBING_PRESSURE),'NULL') as TUBING_PRESSURE,
nvl(to_char(TEMPERATURE),'NULL') as TEMPERATURE,
nvl(to_char(DOWNTIME_HRS),'NULL') as DOWNTIME_HRS,
nvl(to_char(DOWNTIME_REASON_REF_ID),'NULL') as DOWNTIME_REASON_REF_ID,
nvl(to_char(COMMENTS),'NULL') as COMMENTS,
nvl(to_char(POTENTIAL_UPDATE_FL),'NULL') as POTENTIAL_UPDATE_FL,
nvl(to_char(POTENTIAL_PROCESS_ID),'NULL') as POTENTIAL_PROCESS_ID,
nvl(to_char(FORECAST_PROCESS_ID),'NULL') as FORECAST_PROCESS_ID,
nvl(to_char(UPDATE_PROCESS_ID),'NULL') as UPDATE_PROCESS_ID,
nvl(to_char(NRI_OIL),'NULL') as NRI_OIL,
nvl(to_char(NRI_GAS),'NULL') as NRI_GAS,
nvl(to_char(NRI_NGL),'NULL') as NRI_NGL,
nvl(to_char(CUM_GAS_PROD),'NULL') as CUM_GAS_PROD,
nvl(to_char(CUM_OIL_PROD),'NULL') as CUM_OIL_PROD,
nvl(to_char(CUM_WATER_PROD),'NULL') as CUM_WATER_PROD,
nvl(to_char(CUM_POTENTIAL_GAS_PROD),'NULL') as CUM_POTENTIAL_GAS_PROD,
nvl(to_char(CUM_POTENTIAL_OIL_PROD),'NULL') as CUM_POTENTIAL_OIL_PROD,
nvl(to_char(CUM_POTENTIAL_WATER_PROD),'NULL') as CUM_POTENTIAL_WATER_PROD,
nvl(to_char(CUM_FORECAST_GAS_PROD),'NULL') as CUM_FORECAST_GAS_PROD,
nvl(to_char(CUM_FORECAST_OIL_PROD),'NULL') as CUM_FORECAST_OIL_PROD,
nvl(to_char(CUM_FORECAST_WATER_PROD),'NULL') as CUM_FORECAST_WATER_PROD,
nvl(to_char(LINE_PRESSURE),'NULL') as LINE_PRESSURE,
nvl(to_char(QBYTE_CC_NUM),'NULL') as QBYTE_CC_NUM,
nvl(to_char(QBYTE_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS'),'NULL') as QBYTE_UPDATE_TS,
nvl(to_char(TOW_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS'),'NULL') as TOW_UPDATE_TS,
nvl(to_char(PROCOUNT_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS'),'NULL') as PROCOUNT_UPDATE_TS,
nvl(to_char(CREATE_USER_ID),'NULL') as CREATE_USER_ID,
nvl(to_char(CREATE_TS,'YYYY-MM-DD HH24:MI:SS'),'NULL') as CREATE_TS,
nvl(to_char(UPDATE_USER_ID),'NULL') as UPDATE_USER_ID,
nvl(to_char(UPDATE_TS,'YYYY-MM-DD HH24:MI:SS'),'NULL') as UPDATE_TS,
nvl(to_char(SRC_SYS_CD),'NULL') as SRC_SYS_CD,
nvl(to_char(WATER_SALES),'NULL') as WATER_SALES,
nvl(to_char(GROSS_OIL_BEG_INV),'NULL') as GROSS_OIL_BEG_INV,
nvl(to_char(GROSS_OIL_END_INV),'NULL') as GROSS_OIL_END_INV,
nvl(to_char(SURF_CASING_PRESS),'NULL') as SURF_CASING_PRESS,
nvl(to_char(CHLORIDES),'NULL') as CHLORIDES,
nvl(to_char(GOR_PROD),'NULL') as GOR_PROD,
nvl(to_char(WGR_PROD),'NULL') as WGR_PROD,
nvl(to_char(VENT_VOL),'NULL') as VENT_VOL,
nvl(to_char(OIL_DENSITY),'NULL') as OIL_DENSITY,
nvl(to_char(WATER_DENSITY),'NULL') as WATER_DENSITY,
nvl(to_char(WOR_PROD),'NULL') as WOR_PROD,
nvl(to_char(WLR_PROD),'NULL') as WLR_PROD,
nvl(to_char(OIL_CUT_PCT),'NULL') as OIL_CUT_PCT,
nvl(to_char(MEASURED_FLUID_LEVEL),'NULL') as MEASURED_FLUID_LEVEL,
nvl(to_char(MEASURED_BHP),'NULL') as MEASURED_BHP,
nvl(to_char(GAS_LIFT_VOL),'NULL') as GAS_LIFT_VOL,
nvl(to_char(H2S),'NULL') as H2S,
nvl(to_char(STROKES),'NULL') as STROKES,
nvl(to_char(LOAD_WATER_REM),'NULL') as LOAD_WATER_REM,
nvl(to_char(GAS_FLARE),'NULL') as GAS_FLARE,
nvl(to_char(WATER_HAULED),'NULL') as WATER_HAULED,
nvl(to_char(WATER_TRANSFER),'NULL') as WATER_TRANSFER,
nvl(to_char(OIL_GRAVITY),'NULL') as OIL_GRAVITY,
nvl(to_char(C5),'NULL') as C5,
nvl(to_char(GROSS_FCST_OIL_PROD),'NULL') as GROSS_FCST_OIL_PROD,
nvl(to_char(GROSS_FCST_GAS_PROD),'NULL') as GROSS_FCST_GAS_PROD,
nvl(to_char(GROSS_FCST_WATER_PROD),'NULL') as GROSS_FCST_WATER_PROD,
nvl(to_char(FULL_CHOKE_SIZE),'NULL') as FULL_CHOKE_SIZE,
nvl(to_char(WATER_TRNSFR_VOL),'NULL') as WATER_TRNSFR_VOL,
nvl(to_char(FC_H2S),'NULL') as FC_H2S,
nvl(to_char(PRED_FCST_OIL_PROD),'NULL') as PRED_FCST_OIL_PROD,
nvl(to_char(PRED_FCST_GAS_PROD),'NULL') as PRED_FCST_GAS_PROD,
nvl(to_char(PRED_FCST_WATER_PROD),'NULL') as PRED_FCST_WATER_PROD,
nvl(to_char(GROSS_NGL_SALES),'NULL') as GROSS_NGL_SALES,
nvl(to_char(NET_NGL_SALES),'NULL') as NET_NGL_SALES,
nvl(to_char(GROSS_DRY_GAS_PROD),'NULL') as GROSS_DRY_GAS_PROD,
nvl(to_char(NET_DRY_GAS_PROD),'NULL') as NET_DRY_GAS_PROD,
nvl(to_char(NET_OIL_PROD),'NULL') as NET_OIL_PROD,
nvl(to_char(NET_GAS_PROD),'NULL') as NET_GAS_PROD,
nvl(to_char(NET_OIL_SALES),'NULL') as NET_OIL_SALES,
nvl(to_char(NET_GAS_SALES),'NULL') as NET_GAS_SALES,
nvl(to_char(GROSS_DRY_GAS_SALES),'NULL') as GROSS_DRY_GAS_SALES,
nvl(to_char(NET_DRY_GAS_SALES),'NULL') as NET_DRY_GAS_SALES,
nvl(to_char(INTERMEDIATE_CASING),'NULL') as INTERMEDIATE_CASING,
nvl(to_char(CO2),'NULL') as CO2,
nvl(to_char(EOR_GAS_TOTAL_RETURN),'NULL') as EOR_GAS_TOTAL_RETURN,
nvl(to_char(UNDERPERFORM_REASON_REF_ID),'NULL') as UNDERPERFORM_REASON_REF_ID,
nvl(to_char(ALERT_POTN_OIL_PROD),'NULL') as ALERT_POTN_OIL_PROD,
nvl(to_char(ALERT_GROSS_OIL_PROD),'NULL') as ALERT_GROSS_OIL_PROD,
nvl(to_char(ALERT_POTN_GAS_PROD),'NULL') as ALERT_POTN_GAS_PROD,
nvl(to_char(ALERT_GROSS_GAS_PROD),'NULL') as ALERT_GROSS_GAS_PROD,
nvl(to_char(AIR_INJECTION),'NULL') as AIR_INJECTION,
nvl(to_char(FRESH_WATER_INJ),'NULL') as FRESH_WATER_INJ,
nvl(to_char(SALT_WATER_INJ),'NULL') as SALT_WATER_INJ,
nvl(to_char(UNDERPERFORM_REASON_COMMENT),'NULL') as UNDERPERFORM_REASON_COMMENT,
nvl(to_char(FLOWBACK_WATER_VOL),'NULL') as FLOWBACK_WATER_VOL,
nvl(to_char(FUEL_GAS),'NULL') as FUEL_GAS,
nvl(to_char(OIL_GRAVITY_WELLHEAD),'NULL') as OIL_GRAVITY_WELLHEAD
from odm_dba.odm_comp_production partition (comp_prod_DIV"""+div_id+""")"""


qry_com="""select distinct completion_id as cnt_comp_id from odm_dba.odm_comp_production partition (comp_prod_DIV"""+div_id+""")"""

cursor.execute(qry_com)
rows = cursor.fetchall()

for message in rows:
    print(message[0])
    #print(qry + " where completion_id="+str(message[0]))    
    cursor.execute(qry + " where completion_id="+str(message[0]))
    rows = [x for x in cursor]
    cols = [x[0] for x in cursor.description]
    jds = []
    for row in rows:
        jd = {}
        for prop, val in zip(cols, row):
            jd[prop] = val
        producer.send('Div_10', json.dumps(jd, indent=4, default=json_util.default).encode('utf-8'))
  #jds.append(jd)
  #producer.send('test', json.dumps(jds, indent=4, default=json_util.default).encode('utf-8'))


cursor.close()
connection.close()

