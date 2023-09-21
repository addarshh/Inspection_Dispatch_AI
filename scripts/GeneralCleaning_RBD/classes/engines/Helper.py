
import os
import datetime
import sqlalchemy
import pandas as pd
import cx_Oracle
import config
from abc import ABC 
from sqlalchemy import create_engine

import os
import sys
import config
import geopandas
import cx_Oracle
import pandas as pd
import os.path as path
from shapely import wkt
import sqlalchemy as sql
import datetime
from abc import ABC


class Helper(ABC):

    engine = None
    conn_targetDB = None

    def __init__(self, amanaCode = None):
        if not 'C:\\instantclient_21_6' in os.environ['PATH']:
            os.environ['PATH'] = os.environ['PATH'] + ';C:\\instantclient_21_6'

        try:
            connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
                + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])

            self.engine = sql.create_engine(connectionString)
            #self.connection = engine.connect()

            self.conn_targetDB = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))

    
        except Exception as error:
            print("Error with creating connection")
            print(error)
            sys.exit(1)


    def AddMonths(self, d,x):
        newmonth =  (((d.month-1) + x)%12)+1
        newyear = int(d.year+((( d.month-1) + x) /12))
        return datetime.date(newyear, newmonth, d.day)

    

  
    ## NEW CODE BLOCK HERE
    def Engine_Start_Metadata_Update(self):
        #######SET MODEL NAME##############
        #MODEL_NAME = 'HEALTH RBD ENGINE'
        cursor_targetDB = self.conn_targetDB.cursor()

        #######INSERT QUERY################
        parameters= dict( name = config.MODEL_NAME, status = 'IN PROGRESS', time = datetime.datetime.now())
        sql_query_time = """Insert into %s.MLMODEL_DETAILS \
                    (MODEL_NAME, MODEL_STATUS, MODEL_START_TIME) \
                        VALUES (:name,:status,:time)""" % config.DB['input_schema']
        result = cursor_targetDB.execute(sql_query_time,parameters)
        #return (result, time)
        self.conn_targetDB.commit()
        cursor_targetDB.close()

    def Engine_End_Metadata_Update(self):
        
        #######GET START_TIME#################

        cursor_targetDB = self.conn_targetDB.cursor() 
        MODEL_NAME = config.MODEL_NAME
        
        cursor_targetDB.execute('''SELECT * FROM %s.MLMODEL_DETAILS where MODEL_NAME = :name \
        AND MODEL_STATUS =  'IN PROGRESS' ORDER BY MODEL_START_TIME DESC''' % config.DB['input_schema'], name=config.MODEL_NAME)
        model_details = cursor_targetDB.fetchall()

        index, model_name, model_status, start_time, end_time = model_details[0]
        # for rows in model_details:
        #     if config.MODEL_NAME == rows[1]:# and time_start == rows[3]:
        #         index, model_name, model_status, start_time, end_time = rows
                #print('found')
        #print(rows)

        #######UPDATE##################
        parameters= dict(status1 = 'COMPLETED',end = datetime.datetime.now(), name =config.MODEL_NAME, start1 = start_time )
        sql_query_time = "Update %s.MLMODEL_DETAILS \
                        set MODEL_STATUS = :status1 ,MODEL_END_TIME = :end \
                        where MODEL_NAME = :name and MODEL_START_TIME = :start1" % config.DB['input_schema']
        result = cursor_targetDB.execute(sql_query_time,parameters)
        self.conn_targetDB.commit()
        cursor_targetDB.close()

    def Engine_End_Metadata_Update_Failed(self):
        
        #######GET START_TIME#################

        cursor_targetDB = self.conn_targetDB.cursor() 
        MODEL_NAME = config.MODEL_NAME
        
        cursor_targetDB.execute('''Select * FROM %s.MLMODEL_DETAILS where MODEL_NAME = :name \
        AND MODEL_STATUS =  'IN PROGRESS' ORDER BY MODEL_START_TIME DESC''' % config.DB['input_schema'], name=config.MODEL_NAME)
        model_details = cursor_targetDB.fetchall()

        index, model_name, model_status, start_time, end_time = model_details[0]
        # for rows in model_details:
        #     if config.MODEL_NAME == rows[1]:# and time_start == rows[3]:
        #         index, model_name, model_status, start_time, end_time = rows
                #print('found')
        #print(rows)

        #######UPDATE##################
        parameters= dict(status1 = 'FAILED',end = datetime.datetime.now(), name =config.MODEL_NAME, start1 = start_time )
        sql_query_time = "Update %s.MLMODEL_DETAILS set MODEL_STATUS = :status1 ,MODEL_END_TIME = :end \
                        where MODEL_NAME = :name and MODEL_START_TIME = :start1" % config.DB['input_schema']
        result = cursor_targetDB.execute(sql_query_time,parameters)
        self.conn_targetDB.commit()
        cursor_targetDB.close()

    def backup (self,tablename):
        import os

        cursor_targetDB = self.conn_targetDB.cursor() 
        MODEL_NAME = config.MODEL_NAME
        cursor_targetDB.execute('''Select * FROM %s.MLMODEL_DETAILS where MODEL_NAME = :name \
        AND MODEL_STATUS =  'COMPLETED' ORDER BY MODEL_START_TIME DESC \
        ''' % config.DB['input_schema'], name=config.MODEL_NAME)
        model_details = cursor_targetDB.fetchall()

        if len(model_details) > 0:
            index, model_name, model_status, start_time, end_time = model_details[0]
            #backup_tablename = tablename +'_'+str(pd.to_datetime(start_time).strftime("%d/%m/%Y"))
            backup_tablename = tablename +'_'+str(start_time)
            
            query = '''CREATE TABLE "{}"."{}" AS (SELECT * FROM "{}"."{}")'''.format(config.DB['outputschema'],backup_tablename,config.DB['outputschema'],tablename)
            try:
                cursor_targetDB.execute(query)
            except cx_Oracle.DatabaseError as e:
                pass
            self.conn_targetDB.commit()
        cursor_targetDB.close()

    def insert_df_Batchwise(self, df, tablename, batch_size):

        conn = self.conn_targetDB
        cursor = conn.cursor()
        
        # for col_name in df.columns():
        #     if 'date' in col_name
        df['RUN_TIME'] = datetime.datetime.now()
        df.columns = df.columns.str.upper()
        df = df.astype(str)
        dtypedict = {}

        #print("1")

        #print(df.columns)
        for item in df.columns:
            if 'date' in item.lower():
                dtypedict[item] = sqlalchemy.types.DateTime()
                df[item] = df[item].replace('NaT', '')
                df[item] = pd.to_datetime(df[item], errors='coerce')
            elif 'geometry' not in item.lower():
                dtypedict[item] = sqlalchemy.types.VARCHAR(length = 255)
                #df[item] = df[item].replace('NaT', '')
                #df[item] = pd.to_datetime(df[item], errors='coerce')
        #df = df.where(pd.notnull(df), None)
        #print(dtypedict)
        df_temp = df.head(1)
        #print("2")


        df_temp.head(1).to_sql(name='{}'.format(tablename.lower()), con=self.engine, if_exists='replace', schema=config.DB['outputschema'], 
        dtype = dtypedict)
        
        # DELETE 1 ROW.. DONT DROP
        
        query = '''
                SELECT * FROM "{}"."{}"
                '''.format(config.DB['outputschema'],tablename)
        #print(query)
        cursor.execute(query)

        rows = cursor.fetchall()
        #print("rows:" + str(rows))
        col_names = [row[0] for row in cursor.description]

        query1 = '''
                TRUNCATE TABLE "{}"."{}"
                '''.format(config.DB['outputschema'],tablename)
        cursor.execute(query1)
        
        conn.commit()
        #print("3")
        
        # FETCH TABLE SCHEMA
        query = '''SELECT * FROM "{}"."{}"'''.format(config.DB['outputschema'], tablename)
        #print("4")
        cursor.execute(query)
        #print("5")

        rows = cursor.fetchall()
        col_names = [row[0] for row in cursor.description]
        #print(col_names)
        schema_str = ''
        num_str = ''
        schema_str = ''
        i1 = 0

        #print("4")
        
        for item in col_names:
            if i1 == 0:
                schema_str = '"' + item
                num_str ="''"
                
            elif i1 == len(col_names)-1:
                schema_str = schema_str + '", "' + item + '"'
                if 'date' in item.lower():
                    num_str = num_str + ', TO_TIMESTAMP (:' + str(i1) +', '+ "'" +'YYYY-MM-DD HH24:MI:SS' + "'" +')'
                # elif 'geometry' not in item.lower():
                #     num_str = num_str + ', TO_CHAR (:' + str(i1) +', '+ "'" +'YYYY-MM-DD HH24:MI:SS' + "'" +')'
                else:
                    num_str = num_str + ', :' + str(i1)
            else:
                schema_str = schema_str + '", "' + item
                
                if 'date' in item.lower():
                    num_str = num_str + ', TO_TIMESTAMP (:' + str(i1) +', '+ "'" +'YYYY-MM-DD HH24:MI:SS' + "'" +')'
                # elif 'geometry' not in item.lower():
                #     num_str = num_str + ', TO_CHAR (:' + str(i1) +', '+ "'" +'YYYY-MM-DD HH24:MI:SS' + "'" +')'
                else:
                    num_str = num_str + ', :' + str(i1)
            i1 = i1 + 1
        #print(len(col_names))
        #print(schema_str)
        #print(num_str)

        #print("5")

        # UPDATE ALL ROWS
        splits = []
        for i in range(0,df.shape[0], batch_size):
            splits.append(df[i:i+batch_size])

        #print(splits)
        
        for i in range(len(splits)):
            df_new = splits[i]#.where((pd.notnull(df)), None)
            df_new = df_new.where(pd.notnull(df_new), None)
            #df_new = df_new.astype(str)
            #df_new['Last License renewal date'] = df_new['Last License renewal date'].replace('NaT', '')
            df_row_list = [tuple(x) for x in df_new.values.tolist()]
            insert_sql = '''
                INSERT INTO "{}"."{}" ({}) VALUES ({})'''.format(config.DB['outputschema'],tablename,schema_str,num_str )
            
            #print(insert_sql)
            cursor.executemany(insert_sql,df_row_list, batcherrors=True)
            #print(batcherrors)
        conn.commit()
        #print("6")
        #print(batcherrors)
        cursor.close()







        


