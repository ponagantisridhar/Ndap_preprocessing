import psycopg2
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from datetime import datetime
import json
import traceback
import logging
import calendar


def preprocessing(mapidsList,tableName,input):
    try:
        conn = psycopg2.connect("host='ndap-stgdb-dev-qa.cluster-cuovo5wifiaq.ap-south-1.rds.amazonaws.com' port=5432 dbname=ndap_sandbox user=Sridhar password=kEdTGUpAHB")
        cur = conn.cursor()
        engine = create_engine('postgresql://Sridhar:kEdTGUpAHB@ndap-stgdb-dev-qa.cluster-cuovo5wifiaq.ap-south-1.rds.amazonaws.com:5432/ndap_sandbox')
        print("connected sucessfully")
        # logging.info("connected succesfully database")
    except:
        print('error occured while connecting to Database')
        print('please check the credentials once....')
    try:
        conn1 = psycopg2.connect("host='ndap-stgdb-dev-qa.cluster-cuovo5wifiaq.ap-south-1.rds.amazonaws.com' port=5432 dbname=postgres user=Sridhar password=kEdTGUpAHB")
        cur1 = conn1.cursor()
        engine1 = create_engine('postgresql://Sridhar:kEdTGUpAHB@ndap-stgdb-dev-qa.cluster-cuovo5wifiaq.ap-south-1.rds.amazonaws.com:5432/postgres')
        print("connected sucessfully")
        # logging.info("connected succesfully database")
    except:
        print('error occured while connecting to Database')
        print('please check the credentials once....')
    try:
        for mapid in mapidsList:
            dataInsertionStartedTime = datetime.now()
            df = pd.read_sql_query("""select * from staging.s_preprocessing where "Update Status" = 'Success'   and mapid = {} """.format(mapid),con=engine)
            table=df["table_name"].iloc[0]
            print(table)
            dfTotal=pd.read_sql_query('select * from public."{}"'.format(table),con=engine1)
            deleteList=input["deleteColumnsList"]
            for delcol in deleteList:
                if delcol in list(dfTotal.columns):
                    del dfTotal[delcol]
            originalList=list(dfTotal.columns)
            print(originalList)
            numeric_data = list(dfTotal.select_dtypes(include=[np.number]).columns)
            categorical_data = list(dfTotal.select_dtypes(exclude=[np.number]).columns)
            if len(categorical_data) > 1:
                objectFlag = True
            else:
                objectFlag = False
            changedcolumnslist=[]
            countcat=1
            countnum=1
            geographicalColumnsList=input["geographicalColumnsList"]
            geographicalColumnschangedList=input["geographicalColumnschangedList"]
            for column in originalList:
            #     print(columns)
                if column in numeric_data:
                #         print("num")
                    changedcolumnslist.append("INDT_"+str(mapid)+"_"+str(countnum))
                    countnum=countnum+1
                elif column == 'timestamp' or column == 'Timestamp':
                    changedcolumnslist.append(column)
                elif column in geographicalColumnsList:
                    for par in range(0,len(geographicalColumnsList)):
                        if column == geographicalColumnsList[par]:
                            changedcolumnslist.append(geographicalColumnschangedList[par])
                elif column not in geographicalColumnsList:
                    changedcolumnslist.append("DIMX_"+str(mapid)+"_"+str(countcat))
                    countcat=countcat+1
            dfTotal.columns=changedcolumnslist
            print(changedcolumnslist)

            if "INDT_FT" in changedcolumnslist:
                dfTotal['INDT_TT']=dfTotal['INDT_TT'].astype('datetime64[ns]').dt.date
                dfTotal['INDT_FT']=dfTotal['INDT_FT'].astype('datetime64[ns]').dt.date
            sourcecolumn= pd.DataFrame({"source_code":[mapid]*len(changedcolumnslist),"source_column_name":originalList,"dim_ind_id":changedcolumnslist})
            # sourcecolumn.to_sql('s_column_names',engine,schema="staging", if_exists="append",index=False)
            linksList=df['source_link'].tolist()
            sourceurldf=pd.DataFrame({"source_code":[mapid]*len(linksList),"source_link":linksList})
            # sourceurldf.to_sql("s_Urls",engine,schema="staging", if_exists="append",index=False)
            if typeLoad=='A':
                endtime=dfTotal['DIMX_TT'].tolist()[-1]
                cur.execute("""UPDATE reporting.source_master SET to_time = '{}' WHERE  source_code = {} """.format(endtime,mapid))
                cur.execute("""UPDATE reporting.source_master SET lastupdateddate = '{}' WHERE  source_code = {} """.format(datetime.now(),mapid))
                cur.execute("""select stage_table_name from reporting.source_master where source_code = {} """.format(mapid))
                existingtablename=cur.fetchall()[0][0]
                cur.execute("""INSERT INTO staging.s_jobruns(table_name, source_code, starttime, endtime, record_count, upload_type, job_status, reporting_layer_status, text) VALUES('{}', {}, '{}', '{}', {}, 0, '', '', '')""".format(existingtablename,mapid,datetime.now(),datetime.now(),len(dfTotal)))
                conn.commit()
                cur.execute("""SELECT jobrun_id from staging.s_jobruns  ORDER BY endtime DESC LIMIT 1""")
                jobrunid=cur.fetchall()[0][0]
                dfTotal["source_code"]=mapid
                dfTotal["jobrun_id"]=jobrunid
                dfTotal.to_sql(existingtablename,engine,schema="staging", if_exists="append",index=False)
            elif typeLoad == 'I':
                print("entered initial")
                if "INDT_FT" in changedcolumnslist:
                    startTime=min(pd.to_datetime(dfTotal['INDT_FT']).dt.date)
                    endtime=max(pd.to_datetime(dfTotal['INDT_TT']).dt.date)
                    print(startTime,endtime)
                    cur.execute("""INSERT INTO reporting.source_master
                    (ministry_code, department_code, source_code, source_name, datasource_link, yojana_scheme_id, yojana_scheme_name, location_level, from_time, to_time, dataset_information, lastupdateddate, stage_table_name, reporting_table_name, sociometric_indicator_status, source_domain_name, update_frequency, sociometric_mapping_status, reporting_status, time_granularity, other_dimension_flag)
                    VALUES({}, {}, {}, '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}); """
                    .format(df["ministry_code"].iloc[0],df["department_code"].iloc[0],mapid,df["source_name"].iloc[0],'',
                    0,"",df["location_level"].iloc[0],startTime,endtime,"",datetime.now(),table,
                    "NDAP_REPORT_"+str(mapid),"","",df["frequency"].iloc[0],"","",df["frequency"].iloc[0],objectFlag))
                else:
                    cur.execute("""INSERT INTO reporting.source_master
                    (ministry_code, department_code, source_code, source_name, datasource_link, yojana_scheme_id, yojana_scheme_name, location_level, dataset_information, lastupdateddate, stage_table_name, reporting_table_name, sociometric_indicator_status, source_domain_name, update_frequency, sociometric_mapping_status, reporting_status, time_granularity, other_dimension_flag)
                    VALUES({}, {}, {}, '{}', '{}', {}, '{}', '{}','{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}); """
                    .format(df["ministry_code"].iloc[0],df["department_code"].iloc[0],mapid,df["source_name"].iloc[0],'',
                    0,"",df["location_level"].iloc[0],"",datetime.now(),table,
                    "NDAP_REPORT_"+str(mapid),"","",df["frequency"].iloc[0],"","",df["frequency"].iloc[0],objectFlag))

                print("datainserting started")
                # cur.execute("select max(jobrun_id) from staging.s_jobruns")
                # jobrunid=cur.fetchall()[0][0]+1
                # dfTotal["source_code"]=parameters["sourcecode"]
                # dfTotal["jobrun_id"]=jobrunid
                dfTotal.to_sql(table,engine,schema="staging", if_exists="replace",chunksize=10000,index=False)
                cur.execute("""INSERT INTO staging.s_jobruns(table_name, source_code, starttime, endtime, record_count, upload_type, job_status, reporting_layer_status, text) VALUES('{}', {}, '{}', '{}', {}, 0, '', '', '')  returning jobrun_id""".format(table,mapid,dataInsertionStartedTime,datetime.now(),len(dfTotal)))
                jobrunid=cur.fetchall()[0][0]
                cur.execute("""ALTER TABLE staging."{}" ADD COLUMN source_code int;""".format(table))
                cur.execute("""ALTER TABLE staging."{}" ADD COLUMN jobrun_id int;""".format(table))
                cur.execute("""update staging."{}"  set source_code = {};""".format(table,mapid))
                cur.execute("""update staging."{}"  set jobrun_id = {};""".format(table,jobrunid))
                cur.execute("""UPDATE staging.s_preprocessing SET "Update Status" = 'Success' WHERE  mapid = {} """.format(mapid))
                # cur.execute("""UPDATE staging.s_preprocessing SET source_code = {} WHERE  table_name = {} """.format(parameters['sourcecode'],parameters['table_name']))
                cur.execute("""UPDATE staging.s_preprocessing SET source_code = {} WHERE  mapid = {} ;""".format(mapid,mapid))
                cur.execute("""GRANT SELECT, INSERT, UPDATE, DELETE ON  TABLE staging."{}" TO "Sociometrik","Satish Chappa","Vighnesh","Nikhil"; """.format(table))
                cur.execute("""UPDATE staging.s_jobruns SET endtime = '{}' WHERE  jobrun_id = {} """.format(datetime.now(),jobrunid))
                conn.commit()
                sourceurldf.to_sql("s_Urls",engine,schema="staging", if_exists="append",index=False)
                sourcecolumn.to_sql('s_column_names',engine,schema="staging", if_exists="append",index=False)
                log_df=pd.DataFrame({'SourceId':mapid,'Tablename':table,
                             'Status':'Success','logtime':datetime.now()},index=[0])
                log_df.to_sql('preprocessinglog', engine1, if_exists="append",index=False)
                print("datainserting ended")
    except Exception as e:
        s=traceback.format_exc()
        print(s)
        log_df=pd.DataFrame({'SourceId':mapid,'Tablename':table,
                     'Status':str(s),'logtime':datetime.now()},index=[0])
        log_df.to_sql('preprocessinglog', engine1, if_exists="append",index=False)
        cur.execute("""UPDATE staging.s_preprocessing SET "Update Status" = 'NA' WHERE  mapid = {} """.format(parameters['mapid']))
        conn.commit()
    finally:
        conn.close()
        conn1.close()
if __name__ == "__main__":
    processingidstable=pd.read_csv("")#present csv location with mapid and table name
    mapidsList=processingidstable['Mapid'].tolist()
    TableNameList=processingidstable['Tablename'].tolist()
    typeLoad='I'
    with open(r"input.json") as f:
        input = json.load(f)
    preprocessing(mapidsList,TableNameList,input)
