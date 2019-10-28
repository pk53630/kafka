#!/usr/bin/python3
import csv
import sys
import shutil
import glob
import os
import arrow
import gzip
import cx_Oracle
import datetime
import pandas as pd

path = '/home/odmbatch/iFluid/data/*.csv'
archived_path='/home/odmbatch/iFluid/archive/' 
files=glob.glob(path)

qry_dt=""" SELECT sysdate from dual"""
qry_cnt="""SELECT count(*) FROM ODM_INFO.ODM_COMPLETION WHERE PRIMO_PRPRTY ="""
query=""" SELECT distinct Division_ID FROM ODM_INFO.ODM_COMPLETION WHERE PRIMO_PRPRTY ="""
#connection = cx_Oracle.connect('DATAMART_READ_ONLY/Welcome_1@P1DATE """
connection = cx_Oracle.connect('iprod_dba/Test_123@r1date.eogresources.com')
cursor = cx_Oracle.Cursor(connection)


for file in files: 
    #print(file)
    
    df1 = pd.read_csv(file,sep=',',encoding="ISO-8859-1")
    df2 = df1.dropna(subset=['Well ID'])
    df3 = df2.iloc[:,[0,1,2,3,4,8,24,25,26,27,31,32,33,35,36,39,41,44,50,51,60,61,62,63,126,128,166,220,224,225,227,240,241,245,252,253,257,260,269,270,271,272,278,279,283,293,298,300,306,307,310,318,330,333,336,338,341,343,346]]
    df3=df3.fillna('XXXXXXXXXX')
    #print(df3.shape)
    rows = [list(x) for x in df3.values]
    print(len(rows))


    val_lst=[]
    	
    for j in range(0,len(rows)):
        if ((rows[j][51]) != ""):
            #print(rows[j][51])
            cursor.execute(qry_cnt+str(rows[j][51]))
            row = cursor.fetchone()    
            #print(row[0])
            if(row[0] != 0):
                val_lst.append(rows[j])
                #print(val_lst)
    
    print(len(val_lst)) 
    
    #for k in range(0,len(val_lst)):
    #    print(val_lst[k][51]) 

    final_lst=[]
    
    for i in range(0,len(val_lst)):
        cursor.execute(query+str(val_lst[i][51]))
        rowi = cursor.fetchone()
        DIVISION_ID = rowi[0]
        cursor.execute(qry_dt)
        row_dt = cursor.fetchone()
        CREATE_DATETIME=(row_dt[0])
        CREATE_USER='iFLUID'
        DATE_VALUE = val_lst[i][0]
        TIME_VALUE = val_lst[i][1]
        #print(DIVISION_ID)
        DATE_TIME = (str(DATE_VALUE)+" "+str(TIME_VALUE))
        #print(DATE_TIME)
        val_lst[i].insert(59,DIVISION_ID)
        val_lst[i].insert(60,DATE_TIME)
        val_lst[i].insert(61,DATE_TIME)
        val_lst[i].insert(62,val_lst[i][14])
        val_lst[i].insert(63,CREATE_USER)
        val_lst[i].insert(64,CREATE_DATETIME)
        val_lst[i].insert(65,CREATE_USER)
        val_lst[i].insert(66,CREATE_DATETIME)

        final_lst.append(val_lst[i][2:])
        #print(final_lst[i])

        if(final_lst[i][0] == 'XXXXXXXXXX'):
            final_lst[i][0]=0
        if(final_lst[i][1] == 'XXXXXXXXXX'):
            final_lst[i][1]=''
        if(final_lst[i][2] == 'XXXXXXXXXX'):
            final_lst[i][2]=0
        if(final_lst[i][3] == 'XXXXXXXXXX'):
            final_lst[i][3]=0
        if(final_lst[i][4] == 'XXXXXXXXXX'):
            final_lst[i][4]=0
        if(final_lst[i][5] == 'XXXXXXXXXX'):
            final_lst[i][5]=0
        if(final_lst[i][6] == 'XXXXXXXXXX'):
            final_lst[i][6]=0
        if(final_lst[i][7] == 'XXXXXXXXXX'):
            final_lst[i][7]=0
        if(final_lst[i][8] == 'XXXXXXXXXX'):
            final_lst[i][8]=''
        if(final_lst[i][9] == 'XXXXXXXXXX'):
            final_lst[i][9]=0
        if(final_lst[i][10] == 'XXXXXXXXXX'):
            final_lst[i][10]=0
        if(final_lst[i][11] == 'XXXXXXXXXX'):
            final_lst[i][11]=0
        if(final_lst[i][12] == 'XXXXXXXXXX'):
            final_lst[i][12]=0
        if(final_lst[i][13] == 'XXXXXXXXXX'):
            final_lst[i][13]=0
        if(final_lst[i][14] == 'XXXXXXXXXX'):
            final_lst[i][14]=0
        if(final_lst[i][15] == 'XXXXXXXXXX'):
            final_lst[i][15]=0
        if(final_lst[i][16] == 'XXXXXXXXXX'):
            final_lst[i][16]=0
        if(final_lst[i][17] == 'XXXXXXXXXX'):
            final_lst[i][17]=0
        if(final_lst[i][18] == 'XXXXXXXXXX'):
            final_lst[i][18]=0
        if(final_lst[i][19] == 'XXXXXXXXXX'):
            final_lst[i][19]=0
        if(final_lst[i][20] == 'XXXXXXXXXX'):
            final_lst[i][20]=0
        if(final_lst[i][21] == 'XXXXXXXXXX'):
            final_lst[i][21]=''
        if(final_lst[i][22] == 'XXXXXXXXXX'):
            final_lst[i][22]=0
        if(final_lst[i][23] == 'XXXXXXXXXX'):
            final_lst[i][23]=0
        if(final_lst[i][24] == 'XXXXXXXXXX'):
            final_lst[i][24]=0
        if(final_lst[i][25] == 'XXXXXXXXXX'):
            final_lst[i][25]=''
        if(final_lst[i][26] == 'XXXXXXXXXX'):
            final_lst[i][26]=0
        if(final_lst[i][27] == 'XXXXXXXXXX'):
            final_lst[i][27]=0
        if(final_lst[i][28] == 'XXXXXXXXXX'):
            final_lst[i][28]=0
        if(final_lst[i][29] == 'XXXXXXXXXX'):
            final_lst[i][29]=0
        if(final_lst[i][30] == 'XXXXXXXXXX'):
            final_lst[i][30]=0
        if(final_lst[i][31] == 'XXXXXXXXXX'):
            final_lst[i][31]=0
        if(final_lst[i][32] == 'XXXXXXXXXX'):
            final_lst[i][32]=''
        if(final_lst[i][33] == 'XXXXXXXXXX'):
            final_lst[i][33]=0
        if(final_lst[i][34] == 'XXXXXXXXXX'):
            final_lst[i][34]=0
        if(final_lst[i][35] == 'XXXXXXXXXX'):
            final_lst[i][35]=0
        if(final_lst[i][36] == 'XXXXXXXXXX'):
            final_lst[i][36]=0
        if(final_lst[i][37] == 'XXXXXXXXXX'):
            final_lst[i][37]=0
        if(final_lst[i][38] == 'XXXXXXXXXX'):
            final_lst[i][38]=0
        if(final_lst[i][39] == 'XXXXXXXXXX'):
            final_lst[i][39]=0
        if(final_lst[i][40] == 'XXXXXXXXXX'):
            final_lst[i][40]=0
        if(final_lst[i][41] == 'XXXXXXXXXX'):
            final_lst[i][41]=0
        if(final_lst[i][42] == 'XXXXXXXXXX'):
            final_lst[i][42]=0

        if(final_lst[i][43] == 'XXXXXXXXXX'):
            final_lst[i][43]=0
        elif(final_lst[i][43] != 'XXXXXXXXXX'):
            DEPTH_MAKER=str(final_lst[i][43]).split()
            final_lst[i][43]=DEPTH_MAKER[0]

        if(final_lst[i][44] == 'XXXXXXXXXX'):
            final_lst[i][44]=0
        if(final_lst[i][45] == 'XXXXXXXXXX'):
            final_lst[i][45]=0
        if(final_lst[i][46] == 'XXXXXXXXXX'):
            final_lst[i][46]=0
        if(final_lst[i][47] == 'XXXXXXXXXX'):
            final_lst[i][47]=0
        if(final_lst[i][48] == 'XXXXXXXXXX'):
            final_lst[i][48]=0
        if(final_lst[i][49] == 'XXXXXXXXXX'):
            final_lst[i][49]=0
        if(final_lst[i][50] == 'XXXXXXXXXX'):
            final_lst[i][50]=0
        if(final_lst[i][51] == 'XXXXXXXXXX'):
            final_lst[i][51]=0
        if(final_lst[i][52] == 'XXXXXXXXXX'):
            final_lst[i][52]=0
        if(final_lst[i][53] == 'XXXXXXXXXX'):
            final_lst[i][53]=0
        if(final_lst[i][54] == 'XXXXXXXXXX'):
            final_lst[i][54]=0
        if(final_lst[i][55] == 'XXXXXXXXXX'):
            final_lst[i][55]=0
        if(final_lst[i][56] == 'XXXXXXXXXX'):
            final_lst[i][56]=''
        if(final_lst[i][60] == 'XXXXXXXXXX'):
            final_lst[i][60]=0

        #print(final_lst[i])

        cursor.execute("""
        insert into iprod_dba.echometer_fluid_survey_test (ACOUSTIC_VEL,ACOUSTIC_VEL_CALC_METHOD,CASING_PRESSURE,JOINTS_PER_SECOND,
        TOTAL_GASEOUS_LIQ_COLUMN_HT,EQUIVALENT_GAS_FREE_LIQ_HT,GAS_PRODUCTION,GAS_LIQ_INTERFACE_PRESSURE,IPR_METHOD,
        JOINTES_TO_LIQ_LEVEL,JOINTS_COUNT,OIL_PRODUCTION,PBHP,PUMP_INTAKE_PRESSURE,PRODUCTION_EFFICIENCY,
        STATIC_BHP,MANUAL_ACOUSTIC_VEL,WATER_PRODUCTION,BALANCED_DOWNSTROKE_PEAK,BALANCED_UPSTROKE_PEACK,MEASURED_DOWNSTROKE_PEAK,
        MEASURED_UPSTROKE_PEAK,RATED_HP,COUNTER_BALANCE_CHANGE,TUBING_PRESSURE_PSI,CBE_METHOD,HERTZ,RATED_FULL_LOAD_AMP,
        GEARBOX_RATING,POWER_CONSUMPTION,POWER_DEMAND,PUMP_INTAKE_DEPTH,UNIT_API_NUMBER,RATED_FULL_LOAD_RPM,RUN_TIME,
        MEASURED_STOKE_LENGTH,SYNCHRONOUS_RPM,TOP_TAPER_ROD_DIAMETER,TOP_TAPER_ROD_LENGTH,TOP_TAPER_ROD_TYPE,VOLTAGE,
        WEIGHT_OF_COUNTER_WEIGHTS,KELLEY_BUSHING,DEPTH_MAKER,BOTTOMHOLE_TEMPERATURE,GAS_GRAVITY,OIL_API,SURFACE_TEMPERATURE,
        WATER_SPECIFIC_GRAVITY,WELL_ID,AVERAGE_JOINT_LENGTH,CASING_OD,FORMATION_DEPTH,PLUNGER_DIAMETER,ANCHOR_DEPTH,
        TUBING_OD,COMMENT_TXT, DIVISION_ID, DATE_VALUE,FLUID_LEVEL_SURVEY_DATE,PRODUCING_BHP, 
        CREATE_USERID,CREATE_TS,UPDATE_USERID,UPDATE_TS )
        values (:1, :2 ,:3 ,:4 ,:5 ,:6 ,:7 ,:8 ,:9 ,:10 ,:11 ,:12 ,:13 ,:14 ,:15 ,:16 ,:17 ,:18 ,:19 ,:20 ,:21 ,:22 ,:23 ,:24 ,:25 ,:26 ,:27 ,:28 ,:29 ,:30 ,
        :31 ,:32 ,:33 ,:34 ,:35 ,:36 ,:37 ,:38 ,:39 ,:40 ,:41 ,:42 ,:43 ,:44 ,:45 ,:46 ,:47 ,:48 ,:49 ,:50 ,:51 ,:52 ,:53 ,:54 ,:55 ,:56 ,:57 ,:58 ,:59 ,
        :60,:61,:62,:63,:64,:65)""", final_lst[i])
		
        cursor.execute("""commit""")

    dt=arrow.now().format('YYYYMMDD_HHMS')
    file_rename=file+"_"+dt+"_Processed"
    print(file_rename)
    os.rename(file, file_rename)
     
    in_data = open(file_rename, "rb").read()
    out_gz = file_rename+".gz"
    gzf = gzip.open(out_gz, "wb")
    gzf.write(in_data)
    gzf.close()
    print(out_gz)
    shutil.move(out_gz, archived_path)
    os.remove(file_rename)
    
 
cursor.close()
connection.close()
