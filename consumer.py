#!/usr/bin/python3

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
from bson import json_util
from kafka import KafkaConsumer
from kafka import TopicPartition
import threading
import time
from ast import literal_eval
import threading
from multiprocessing import Process

dt=arrow.now().format('YYYY-MM-DD')
filename="""/home/odmbatch/odm_kafka/Logs/oracle_to_memSQL_cosumer_Div63_"""+dt+""".log"""

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



cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
cur = cnx.cursor()

#transaction_sql="""insert into MRTE_DBA.odm_well_delta (WELL_ID, DIVISION_ID, PRIMO_ID, PRIMO_PRPRTY, RIG_TYPE) values (%(WELL_ID)s, %(DIVISION_ID)s, %(PRIMO_ID)s, %(PRIMO_PRPRTY)s, %(RIG_TYPE)s)"""


transaction_sql="""insert into MRTE_DBA.ODM_COMP_PRODUCTION 
(DIVISION_ID,COMPLETION_ID,DATE_ID,DATE_VALUE,GROSS_GAS_PROD,GROSS_OIL_PROD,WATER_PROD,GROSS_GAS_SALES,GROSS_OIL_SALES,POTENTIAL_GAS_PROD,POTENTIAL_OIL_PROD,POTENTIAL_WATER_PROD,FORECAST_GAS_PROD,FORECAST_OIL_PROD,FORECAST_WATER_PROD,FORECAST_NGL_PROD,FORECAST_COND_PROD,FORECAST_GAS_SALES,FORECAST_OIL_SALES,FORECAST_WATER_SALES,FORECAST_NGL_SALES,FORECAST_COND_SALES,FORECAST_NET_GAS_SALES,FORECAST_NET_OIL_SALES,FORECAST_NET_WATER_SALES,FORECAST_NET_NGL_SALES,FORECAST_NET_COND_SALES,GAS_INJ,OIL_INJ,WATER_INJ,CHOKE,CASING_PRESSURE1,CASING_PRESSURE2,TUBING_PRESSURE,TEMPERATURE,DOWNTIME_HRS,DOWNTIME_REASON_REF_ID,COMMENTS,POTENTIAL_UPDATE_FL,POTENTIAL_PROCESS_ID,FORECAST_PROCESS_ID,
UPDATE_PROCESS_ID,NRI_OIL,NRI_GAS,NRI_NGL,CUM_GAS_PROD,CUM_OIL_PROD,CUM_WATER_PROD,CUM_POTENTIAL_GAS_PROD,CUM_POTENTIAL_OIL_PROD,CUM_POTENTIAL_WATER_PROD,CUM_FORECAST_GAS_PROD,CUM_FORECAST_OIL_PROD,CUM_FORECAST_WATER_PROD,LINE_PRESSURE,QBYTE_CC_NUM,QBYTE_UPDATE_TS,TOW_UPDATE_TS,PROCOUNT_UPDATE_TS,CREATE_USER_ID,CREATE_TS,UPDATE_USER_ID,UPDATE_TS,SRC_SYS_CD,WATER_SALES,GROSS_OIL_BEG_INV,GROSS_OIL_END_INV,SURF_CASING_PRESS,CHLORIDES,GOR_PROD,WGR_PROD,VENT_VOL,OIL_DENSITY,WATER_DENSITY,WOR_PROD,WLR_PROD,OIL_CUT_PCT,MEASURED_FLUID_LEVEL,MEASURED_BHP,GAS_LIFT_VOL,H2S,STROKES,LOAD_WATER_REM,GAS_FLARE,WATER_HAULED,WATER_TRANSFER,OIL_GRAVITY,C5,GROSS_FCST_OIL_PROD,GROSS_FCST_GAS_PROD,GROSS_FCST_WATER_PROD,
FULL_CHOKE_SIZE,WATER_TRNSFR_VOL,FC_H2S,PRED_FCST_OIL_PROD,PRED_FCST_GAS_PROD,PRED_FCST_WATER_PROD,GROSS_NGL_SALES,NET_NGL_SALES,GROSS_DRY_GAS_PROD,NET_DRY_GAS_PROD,NET_OIL_PROD,NET_GAS_PROD,NET_OIL_SALES,NET_GAS_SALES,GROSS_DRY_GAS_SALES,NET_DRY_GAS_SALES,INTERMEDIATE_CASING,CO2,EOR_GAS_TOTAL_RETURN,UNDERPERFORM_REASON_REF_ID,ALERT_POTN_OIL_PROD,ALERT_GROSS_OIL_PROD,ALERT_POTN_GAS_PROD,ALERT_GROSS_GAS_PROD,AIR_INJECTION,FRESH_WATER_INJ,SALT_WATER_INJ,UNDERPERFORM_REASON_COMMENT,FLOWBACK_WATER_VOL,FUEL_GAS,OIL_GRAVITY_WELLHEAD)
values
(%(DIVISION_ID)s,%(COMPLETION_ID)s,%(DATE_ID)s,%(DATE_VALUE)s,%(GROSS_GAS_PROD)s,%(GROSS_OIL_PROD)s,%(WATER_PROD)s,%(GROSS_GAS_SALES)s,%(GROSS_OIL_SALES)s,%(POTENTIAL_GAS_PROD)s,%(POTENTIAL_OIL_PROD)s,%(POTENTIAL_WATER_PROD)s,%(FORECAST_GAS_PROD)s,%(FORECAST_OIL_PROD)s,%(FORECAST_WATER_PROD)s,%(FORECAST_NGL_PROD)s,%(FORECAST_COND_PROD)s,%(FORECAST_GAS_SALES)s,%(FORECAST_OIL_SALES)s,%(FORECAST_WATER_SALES)s,%(FORECAST_NGL_SALES)s,%(FORECAST_COND_SALES)s,%(FORECAST_NET_GAS_SALES)s,%(FORECAST_NET_OIL_SALES)s,%(FORECAST_NET_WATER_SALES)s,%(FORECAST_NET_NGL_SALES)s,%(FORECAST_NET_COND_SALES)s,%(GAS_INJ)s,%(OIL_INJ)s,%(WATER_INJ)s,%(CHOKE)s,%(CASING_PRESSURE1)s,%(CASING_PRESSURE2)s,%(TUBING_PRESSURE)s,%(TEMPERATURE)s,%(DOWNTIME_HRS)s,%(DOWNTIME_REASON_REF_ID)s,
%(COMMENTS)s,%(POTENTIAL_UPDATE_FL)s,%(POTENTIAL_PROCESS_ID)s,%(FORECAST_PROCESS_ID)s,%(UPDATE_PROCESS_ID)s,%(NRI_OIL)s,%(NRI_GAS)s,%(NRI_NGL)s,%(CUM_GAS_PROD)s,%(CUM_OIL_PROD)s,%(CUM_WATER_PROD)s,%(CUM_POTENTIAL_GAS_PROD)s,%(CUM_POTENTIAL_OIL_PROD)s,%(CUM_POTENTIAL_WATER_PROD)s,%(CUM_FORECAST_GAS_PROD)s,%(CUM_FORECAST_OIL_PROD)s,%(CUM_FORECAST_WATER_PROD)s,%(LINE_PRESSURE)s,%(QBYTE_CC_NUM)s,%(QBYTE_UPDATE_TS)s,%(TOW_UPDATE_TS)s,%(PROCOUNT_UPDATE_TS)s,%(CREATE_USER_ID)s,%(CREATE_TS)s,%(UPDATE_USER_ID)s,%(UPDATE_TS)s,%(SRC_SYS_CD)s,%(WATER_SALES)s,%(GROSS_OIL_BEG_INV)s,%(GROSS_OIL_END_INV)s,%(SURF_CASING_PRESS)s,%(CHLORIDES)s,%(GOR_PROD)s,%(WGR_PROD)s,%(VENT_VOL)s,%(OIL_DENSITY)s,%(WATER_DENSITY)s,%(WOR_PROD)s,%(WLR_PROD)s,%(OIL_CUT_PCT)s,%(MEASURED_FLUID_LEVEL)s,
%(MEASURED_BHP)s,%(GAS_LIFT_VOL)s,%(H2S)s,%(STROKES)s,%(LOAD_WATER_REM)s,%(GAS_FLARE)s,%(WATER_HAULED)s,%(WATER_TRANSFER)s,%(OIL_GRAVITY)s,%(C5)s,%(GROSS_FCST_OIL_PROD)s,%(GROSS_FCST_GAS_PROD)s,%(GROSS_FCST_WATER_PROD)s,%(FULL_CHOKE_SIZE)s,%(WATER_TRNSFR_VOL)s,%(FC_H2S)s,%(PRED_FCST_OIL_PROD)s,%(PRED_FCST_GAS_PROD)s,%(PRED_FCST_WATER_PROD)s,%(GROSS_NGL_SALES)s,%(NET_NGL_SALES)s,%(GROSS_DRY_GAS_PROD)s,%(NET_DRY_GAS_PROD)s,%(NET_OIL_PROD)s,%(NET_GAS_PROD)s,%(NET_OIL_SALES)s,%(NET_GAS_SALES)s,%(GROSS_DRY_GAS_SALES)s,%(NET_DRY_GAS_SALES)s,%(INTERMEDIATE_CASING)s,%(CO2)s,%(EOR_GAS_TOTAL_RETURN)s,%(UNDERPERFORM_REASON_REF_ID)s,%(ALERT_POTN_OIL_PROD)s,%(ALERT_GROSS_OIL_PROD)s,%(ALERT_POTN_GAS_PROD)s,%(ALERT_GROSS_GAS_PROD)s,%(AIR_INJECTION)s,%(FRESH_WATER_INJ)s,%(SALT_WATER_INJ)s,%(UNDERPERFORM_REASON_COMMENT)s,%(FLOWBACK_WATER_VOL)s,%(FUEL_GAS)s,%(OIL_GRAVITY_WELLHEAD)s)"""


# To consume latest messages and auto-commit offsets 
#consumer = KafkaConsumer('Div_63', bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
 
#for message in consumer: 
# message value and key are raw bytes -- decode if necessary! 
# e.g., for unicode: `message.value.decode('utf-8')` 
    #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    #value_to_process = message.value
    #print(value_to_process)
    #print(type(value_to_process))
    #data = literal_eval(value_to_process.decode('utf8'))
    #print(type(data[0]))
    #print(data[0])
    #print(data)
    #cur.execute(transaction_sql, data)
    #cnx.commit()



def consumer_0():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 0)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()

def consumer_1():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 1)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()

def consumer_2():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 2)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()
		
def consumer_3():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 3)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()

def consumer_4():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 4)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()

def consumer_5():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 5)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()		
    cur.close()
    cnx.close()

def consumer_6():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 6)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()

def consumer_7():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 7)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()
		
def consumer_8():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 8)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()

def consumer_9():
    cnx = mysql.connector.connect(user='MRTE_DBA', password='testpass123', host='OPSMSQLDEV01')
    cur = cnx.cursor()
    consumer = KafkaConsumer(bootstrap_servers=["ktyprdkafka01.eogresources.com:9092","ktyprdkafka02.eogresources.com:9092","ktyprdkafka03.eogresources.com:9092"])
    consumer.assign([TopicPartition('Div_10', 9)])
    for message in consumer: 
        # message value and key are raw bytes -- decode if necessary! 
        # e.g., for unicode: `message.value.decode('utf-8')` 
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
        value_to_process = message.value
        data = literal_eval(value_to_process.decode('utf8'))
        cur.execute(transaction_sql, data)
        cnx.commit()
    cur.close()
    cnx.close()

if __name__=='__main__':
    p1 = Process(target = consumer_0)
    p1.start()
    p2 = Process(target = consumer_1)
    p2.start()
    p3 = Process(target = consumer_2)
    p3.start()
    p4 = Process(target = consumer_3)
    p4.start()
    p5 = Process(target = consumer_4)
    p5.start()
    p6 = Process(target = consumer_5)
    p6.start()
    p7 = Process(target = consumer_6)
    p7.start()
    p8 = Process(target = consumer_7)
    p8.start()
    p9 = Process(target = consumer_8)
    p9.start()
    p10 = Process(target = consumer_9)
    p10.start()
    # This is where I had to add the join() function.
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()
		



# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)

# Subscribe to a regex topic pattern
consumer = KafkaConsumer()
consumer.subscribe(pattern='^awesome.*')




cur.close()
cnx.close()
