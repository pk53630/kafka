#!/usr/bin/python3
import sys
import os
import cx_Oracle
import mysql.connector
import datetime
import pandas as pd
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import arrow
import re
import smtplib

src_owner_nm = sys.argv[1]
src_table_nm = sys.argv[2]
tgt_owner_nm = sys.argv[3]
tgt_table_nm = sys.argv[4]
#param_nm = sys.argv[3]
div_id = sys.argv[5]

dt=arrow.now().format('YYYYMMDD')
dtMS=arrow.now().format('HHMS')

filename="""/home/odmbatch/odm/logs/ODM_VALIDATION_DELTA_"""+dt+""".log"""
archive_file= """/home/odmbatch/odm/archive/"""+dtMS+"""_ODM_VALIDATION_"""+src_table_nm+"""_"""+dt+""".csv"""

validation_file="""/home/odmbatch/odm/data/"""+dtMS+"""_ODM_VALIDATION_"""+src_table_nm+"""_"""+dt+""".csv"""
Path(validation_file).touch()


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
logger.info("##################################### ODM VALIDATION EXTACTION STARTED #########################################")
logger.info("################################################################################################################")
logger.info("                                                                                                                ")


query1="""select abs(a.ifac_sche - b.ea_assoc) as v_cnt_assoc from (select count(*) as ifac_sche from ifac_dba.ifac_schematic where update_ts>sysdate-1) a, (select count(*) as ea_assoc from im_dba.ea_assoc where update_ts>sysdate-1) b"""
query2="""select count(*) as v_cnt_asset from im_dba.im_asset a, im_dba.im_property_asset pa where a.asset_id=pa.asset_id and a.update_ts>sysdate-1 order by a.update_ts desc"""

#print(src_query)
#print(tgt_query)

connection = cx_Oracle.connect('odm_dba/R1dba_101@r1date.eogresources.com')
cursor = cx_Oracle.Cursor(connection)

cursor.execute(src_query)

src_column_names =[]

for row in cursor.fetchall():
    src_column_names.append(list(row))

x=""
for i in range(0,len(src_column_names)):
    x=x+","+(''.join(src_column_names[i]))

b=len(x)
c=x[1:b]
#print(x[1:b])


cursor.execute(src_order_query)

src_order_column=[]

for row1 in cursor.fetchall():
    src_order_column.append(list(row1))

x1=""
for i1 in range(0,len(src_order_column)):
    x1=x1+","+(''.join(src_order_column[i1]))

b1=len(x1)
c1=x1[1:b1]
#print(x1[1:b1])


src_query_in="""select """+c+""" from """+src_owner_nm+"""."""+src_table_nm+""" where division_id="""+div_id+""" order by """+c1+""" asc"""
#print(src_query_in)
logger.info("                                                                                                                ")
logger.info("############################# Source Query wilth Dynamic SELECT Statement Execution ############################")

src_data = pd.read_sql(src_query_in,connection)
src_data.to_csv(src_filename)

#print("------------------------------------------------------------------------------------------------------")
logger.info("                                                                                                                ")
logger.info("                                                                                                                ")

cursor.execute(tgt_query)
tgt_column_names =[]

for row2 in cursor.fetchall():
    tgt_column_names.append(list(row2))

y=""
for j in range(0,len(tgt_column_names)):
    y=y+","+(''.join(tgt_column_names[j]))

d=len(y)
e=y[1:d].upper()
#print(y[1:d])


cursor.execute(tgt_order_query)

tgt_order_column=[]

for row2 in cursor.fetchall():
    tgt_order_column.append(list(row2))

x2=""
for i2 in range(0,len(tgt_order_column)):
    x2=x2+","+(''.join(tgt_order_column[i2]))

b2=len(x2)
c2=x1[1:b2]
#print(x2[1:b2])


tgt_query_in="""select """+e+""" from """+tgt_owner_nm+"""."""+tgt_tbl+""" where division_id="""+div_id+""" order by """+c2+""" asc"""
#print(tgt_query_in)
logger.info("############################# Target Query wilth Dynamic SELECT Statement Execution ############################")
logger.info("                                                                                                                ")

cnx = mysql.connector.connect(user='ODM_DBA', password='Test_123', host='OPSMSQLDEV01')
cur = cnx.cursor()

tgt_data = pd.read_sql(tgt_query_in,cnx)
tgt_data.to_csv(tgt_filename)

cur.close()
cnx.close()

cursor.close()
connection.close()


a1=src_data.shape[0]
b1=tgt_data.shape[0]

if a1==b1:
    logger.info("                                                                                                                ")
    logger.info("################################ Source Query and Target Query Count MATCHED ###################################")
    log1="Source Query and Target Query Count MATCHED"
    #print("count match")
else:
    logger.info("                                                                                                                ")
    logger.info("############################## Source Query and Target Query Count NOT MATCHED #################################")
    log1="Source Query and Target Query Count NOT MATCHED"
    #print("count miss match")

def report_diff(x):
    return x[0] if x[0] == x[1] else '{} | {}'.format(*x)

df5=src_data[0:5]
df6=tgt_data[0:5]
#my_panel = pd.Panel(dict(src_data=src_data,tgt_data=tgt_data))
my_panel = pd.Panel(dict(df5=df5,df6=df6))
df=my_panel.apply(report_diff, axis=0)

df.to_csv(validation_file)

os.rename(validation_file,archive_file)

test=src_data.equals(tgt_data)
#print(test)
logger.info("                                                                                                                ")
logger.info("########################### Source Query and Target Query COLUMN Status: %s ####################################",test)
logger.info("                                                                                                                ")
logger.info("                                                                                                                ")
logger.info("################################################################################################################")
logger.info("################################## ODM VALIDATION EXTACTION COMPLETED ##########################################")
logger.info("################################################################################################################")
logger.info("                                                                                                                ")
logger.info("                                                                                                                ")
