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

div_id = sys.argv[1]
#pwd = sys.argv[1]
#sid = sys.argv[1]

dt=arrow.now().format('YYYY-MM-DD')
filename="""/home/odmbatch/odm/logs/ODM_WELL_DELTA_"""+dt+""".log"""

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


query="""select
WELL_ID,
DIVISION_ID,
PRIMO_ID,
PRIMO_PRPRTY,
PRIMO_PRPSUB,
PERC_WELL_ID_NBR,
TOW_SCHEMA,
TOW_WELL_BORE_SK,
ARIES_SCHEMA,
ARIES_DBSKEY,
ARIES_PROPNUM,
NAV1_WELL_ID,
WELL_NAME,
to_char(PRIMO_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as primo_update_ts,
to_char(PERC_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as perc_update_ts,
to_char(TOW_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as tow_update_ts,
to_char(PROCOUNT_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as procount_update_ts,
to_char(ARIES_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as aries_update_ts,
to_char(NAV1_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as nav1_update_ts,
to_char(CREATE_TS,'YYYY-MM-DD HH24:MI:SS') as create_ts,
CREATE_USER_ID,
to_char(UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as update_ts,
UPDATE_USER_ID,
API_NB,
STATE_REF_ID,
COUNTY_REF_ID,
ACCT_STATUS_REF_ID,
WELL_TYPE_REF_ID,
PRODUCING_STATUS_REF_ID,
WELL_DIRECTION_REF_ID,
WELL_OPERATOR,
DDA_GROUP_CODE,
DDA_GROUP_NAME,
FIELD_CODE,
FIELD_NAME,
TEAM,
TREND,
PLAY,
to_char(QBYTE_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as qbyte_update_ts,
QBYTE_CC_NUM,
FV_SITE_ID,
AFE_NUMBER,
PLAT_DESC,
SURVEY_NAME,
MARKER_INT_1,
MARKER_INT_2,
MARKER_INT_1_END,
MARKER_INT_2_END,
DRILLING_ENGINEER,
PHONE_WORK,
PHONE_MOBILE,
NEAR_TOWN_DISTANCE,
KELLEY_BUSHING,
round(LATITUDE,6) as LATITUDE,
round(LONGITUDE,6) as LONGITUDE,
JOINT_LENGTH_1,
JOINT_LENGTH_2,
PROCESS_HISTORY_ID,
SURF_X_COORD,
SURF_Y_COORD,
MAX_TVD,
TARGET,
SPUD_DATE_ID,
SPUD_TIME_ID,
FORMER_DIVISION,
WELL_SPACE_EAST,
WELL_SPACE_WEST,
DDA_GROUP_REF_ID,
to_char(RIG_RELEASE_DATE,'YYYY-MM-DD HH24:MI:SS') as rig_release_date,
to_char(DRILLING_COMP_DATE,'YYYY-MM-DD HH24:MI:SS') as drilling_comp_date,
TIGHTHOLE_FL,
MAX_MD,
FORMATION_NM,
CLASS_CD,
SUBTREND2,
to_char(TD_DATE,'YYYY-MM-DD HH24:MI:SS') as td_date,
SUBTREND2_REF_ID,
WELL_CLASS_ID,
TIGHTHOLE_ID,
OPERATED_ID,
OPERATOR_ID,
TARGET_ID,
TEAM_ID,
REGION_REF_ID,
TREND_ID,
SUBTREND_ID,
TARGET_FORMATION_ID,
BEGIN_BUILD_DPTH,
END_BUILD_DPTH,
MUD_TYPE_AT_TD_ID,
RIG_ID,
MUD_WT,
BIG_RIG_RELEASE_DATE_ID,
CURRENT_DPTH,
PLAY_ID,
EXECUTIVE_PLAY_ID,
DDA_POOL_ID,
BIG_RIG_RELEASE_TIME,
BEGIN_BUILD_DATE_ID,
BEGIN_BUILD_TIME,
END_BUILD_DATE_ID,
END_BUILD_TIME,
TD_TIME,
GWI,
TOT_DRILL_COST,
TOT_COMP_COST,
PLAN_MD_DPTH,
PLAN_TVD_DPTH,
FIRST_SALES_DT_ID,
TAX_CREDIT,
KB_ELEVATION,
GL_ELEVATION,
MUD_TYPE_ID,
PHASE_ID,
SUBPHASE_ID,
AFE_TYPE_ID,
GNG_PROSPECT_ID,
NET_PAY_FOOTAGE,
OLD_UWI,
to_char(CSE_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as cse_update_ts,
to_char(WLV_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as wlv_update_ts,
SECTION,
TOWNSHIP,
RANGE as well_range,
LOCATION_COMMENT,
to_char(SCHED_COMP_START_DT,'YYYY-MM-DD HH24:MI:SS') as sched_comp_start_dt,
ASSOC_FAC,
UWI,
to_char(WELLVIEW_UPDATE_TS,'YYYY-MM-DD HH24:MI:SS') as wellview_update_ts,
WLV_IDWELL,
CURRENT_WI,
CURRENT_NRI_OIL,
CURRENT_NRI_NGL,
CURRENT_NRI_GAS,
MAJOR_ID,
PROCOUNT_SCHEMA,
PROCOUNT_MERRICK_ID,
GIS_FL,
PROGRAM_YEAR_ID,
CURRENT_TVD,
ORIGINATION_TYPE_ID,
WELL_PAD_ID,
to_char(RELEASE_TO_DRILL_DATE,'YYYY-MM-DD HH24:MI:SS') as release_to_drill_date,
SURFACE_STATUS,
to_char(RELEASE_TO_BUILD_DATE,'YYYY-MM-DD HH24:MI:SS') as release_to_build_date,
WELL_STATUS,
to_char(PERMIT_EXPIRES_DATE,'YYYY-MM-DD HH24:MI:SS') as permit_expires_date,
to_char(PERMIT_RECEIVED_DATE,'YYYY-MM-DD HH24:MI:SS') as permit_received_date,
to_char(AFE_APPROVED_DATE,'YYYY-MM-DD HH24:MI:SS') as afe_approved_date,
SUB_DIVISION_ID,
LOCATION_1_ID,
LOCATION_2_ID,
LOCATION_3_ID,
OPS_DIV_ID,
EC_ENTITY_ID,
EC_ENTITY_COLOR,
BLUE_WELL_FL,
DRLG_ENGR_ID,
BHL_LATITUDE,
BHL_LONGITUDE,
PREMIUM_WELL_FL,
TREATED_AZIMUTH,
YPC_EOG_FL,
ENERTIA_KEY,
ENERTIA_WELL_HID,
ENERTIA_WELL_CODE,
AW_ROR,
CDM_WELL_ID,
CDM_DIVISION_ID,
CDM_FL,
PRPRTY_TYPE,
SIDETRACK_FL,
PROJECTION,
COMPRSR_STATION_AREA_ID,
to_char(BIG_RIG_SPUD_DATE,'YYYY-MM-DD HH24:MI:SS') as big_rig_spud_date,
BIG_RIG_SPUD_TIME,
RIG_TYPE
from odm_dba.odm_well_delta where division_id="""+div_id+""" and delta_ts >= (select nvl(
(select end_ts from (select end_ts, rank() over (order by end_ts desc) rn from odm_dba.utl_process_history where process_id=(select process_id from odm_dba.utl_process where process_name = 'LOAD_ODM_WELL_DELTA') and division_id ="""+div_id+""") where rn = 2),(select max(start_ts) from odm_dba.utl_process_history where process_id=(select process_id from odm_dba.utl_process where process_name = 'LOAD_ODM_WELL_DELTA') and division_id = """+div_id+""")) as ts from dual)  and division_id="""+div_id

well_id="""select well_id from odm_dba.odm_well_delta where division_id ="""+div_id+""" and delta_ts >= (select nvl(
(select end_ts from (select end_ts, rank() over (order by end_ts desc) rn from odm_dba.utl_process_history where process_id=(select process_id from odm_dba.utl_process where process_name = 'LOAD_ODM_WELL_DELTA') and division_id ="""+div_id+""") where rn = 2),(select max(start_ts) from odm_dba.utl_process_history where process_id=(select process_id from odm_dba.utl_process where process_name = 'LOAD_ODM_WELL_DELTA') and division_id = """+div_id+""")) as ts from dual)  and division_id="""+div_id

logger.info("                                                                                                                ")
#logger.debug("Selected well id are %s",well_id)
logger.info("                                                                                                                ")

connection = cx_Oracle.connect('odm_dba/R1dba_101@r1date.eogresources.com')
cursor = cx_Oracle.Cursor(connection)
cursor.execute(query)

data_list=[]
column_names =[]

for i in cursor.description:
    column_names.append(i[0])
for row in cursor.fetchall():
    #data_dict={}
    #data_dict=dict(zip(column_names,row))
    #print(tuple(row))
    data_list.append(list(row))

#print(data_list)
logger.debug("The Delete and Insert list are %s :",data_list)
cursor.execute(well_id)

well_id_list=[]
well_id_col_nm =[]

for j in cursor.description:
    well_id_col_nm.append(j[0])
for row_well_id in cursor.fetchall():
    #print(tuple(row_well_id))
    well_id_list.append(list(row_well_id))


#print(well_id_list)

cnx = mysql.connector.connect(user='ODM_DBA', password='Test_123', host='OPSMSQLDEV01')
cur = cnx.cursor()

logger.info("###################################### Delete Statement STARTED Execution ######################################")
stmt = "DELETE FROM ODM_DBA.odm_well_delta WHERE well_id = (%s) and division_id = "+div_id
cur.executemany(stmt,well_id_list)

#logger.debug("Deleted Well ID are %s",well_id_list)
logger.info("###################################### Delete Statement COMPLETED Execution ####################################")

logger.info("                                                                                                                ")
logger.info("                                                                                                                ")
logger.info("###################################### Insert Statement STARTED Execution ######################################")
stmt1 = """INSERT INTO ODM_DBA.odm_well_delta
(well_id,division_id,primo_id,primo_prprty,primo_prpsub,perc_well_id_nbr,tow_schema,tow_well_bore_sk,aries_schema,aries_dbskey,aries_propnum,nav1_well_id,well_name,primo_update_ts,perc_update_ts,tow_update_ts,procount_update_ts,aries_update_ts,nav1_update_ts,create_ts,create_user_id,update_ts,update_user_id,api_nb,state_ref_id,county_ref_id,acct_status_ref_id,well_type_ref_id,producing_status_ref_id,well_direction_ref_id,well_operator,dda_group_code,dda_group_name,field_code,field_name,team,trend,play,qbyte_update_ts,qbyte_cc_num,fv_site_id,afe_number,plat_desc,survey_name,marker_int_1,marker_int_2,marker_int_1_end,marker_int_2_end,drilling_engineer,phone_work,phone_mobile,near_town_distance,kelley_bushing,latitude,longitude,joint_length_1,joint_length_2,process_history_id,surf_x_coord,surf_y_coord,max_tvd,target,spud_date_id,spud_time_id,former_division,well_space_east,well_space_west,dda_group_ref_id,rig_release_date,drilling_comp_date,tighthole_fl,max_md,formation_nm,class_cd,subtrend2,td_date,subtrend2_ref_id,well_class_id,tighthole_id,operated_id,operator_id,target_id,team_id,region_ref_id,trend_id,subtrend_id,target_formation_id,begin_build_dpth,end_build_dpth,mud_type_at_td_id,rig_id,mud_wt,big_rig_release_date_id,current_dpth,play_id,executive_play_id,dda_pool_id,big_rig_release_time,begin_build_date_id,begin_build_time,end_build_date_id,end_build_time,td_time,gwi,tot_drill_cost,tot_comp_cost,plan_md_dpth,plan_tvd_dpth,first_sales_dt_id,tax_credit,kb_elevation,gl_elevation,mud_type_id,phase_id,subphase_id,afe_type_id,gng_prospect_id,net_pay_footage,old_uwi,cse_update_ts,wlv_update_ts,section,township,well_range,location_comment,sched_comp_start_dt,assoc_fac,uwi,wellview_update_ts,wlv_idwell,current_wi,current_nri_oil,current_nri_ngl,current_nri_gas,major_id,procount_schema,procount_merrick_id,gis_fl,program_year_id,current_tvd,origination_type_id,well_pad_id,release_to_drill_date,surface_status,release_to_build_date,well_status,permit_expires_date,permit_received_date,afe_approved_date,sub_division_id,location_1_id,location_2_id,location_3_id,ops_div_id,ec_entity_id,ec_entity_color,blue_well_fl,drlg_engr_id,bhl_latitude,bhl_longitude,premium_well_fl,treated_azimuth,ypc_eog_fl,enertia_key,enertia_well_hid,enertia_well_code,aw_ror,cdm_well_id,cdm_division_id,cdm_fl,prprty_type,sidetrack_fl,projection,comprsr_station_area_id,big_rig_spud_date,big_rig_spud_time,rig_type)
 VALUES
(%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

#cur.executemany(stmt1, data_list)
for k in range(0,len(data_list)):
    #print(len(data_list))
    cur.execute(stmt1,data_list[k])
    #cur.execute(stmt1,data_list[1])



cnx.commit()
#logger.debug("Inserted Well ID are %s",data_list)
logger.info("###################################### Insert Statement COMPLETED Execution ####################################")
logger.info("                                                                                                                ")
cur.close()
cnx.close()

cursor.close()
connection.close()

logger.info("################################################################################################################")
logger.info("##################################### ODM WELL DELTA EXTACTION COMPLETED #######################################")
logger.info("################################################################################################################")
logger.info("                                                                                                                ")
logger.info("                                                                                                                ")
