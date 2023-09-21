import datetime
import os
import sys
import config
import cx_Oracle
import os.path as path
import sqlalchemy as sql
from datetime import datetime
from abc import ABC

class Helper(ABC):

    engine = None
    conn_targetDB = None

    def __init__(self, amanaCode = None):
        if not 'C:\\instantclient_21_6' in os.environ['PATH']:
            os.environ['PATH'] = os.path.join(os.environ['PATH'],';C:\\instantclient_21_6')

        try:
            if config.DB['connectiontype'] == 'service_name': #STAGE
                connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
                    + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
                dsn_str = cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
                self.engine = sql.create_engine('oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@'+dsn_str, \
                execution_options ={'isolation_level': 'AUTOCOMMIT'})
                #self.connection = engine.connect()
                print("STAGE HERE")

                self.conn_targetDB = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
            if config.DB['connectiontype'] == 'SID': #DEV
                connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
                    + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])

                self.engine = sql.create_engine(connectionString)
                #self.connection = engine.connect()
                print("DEV HERE")
                self.conn_targetDB = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))

        except Exception as error:
            print("Error with creating connection")
            print(error)
            sys.exit(1)

    def _connect(self):
        lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])
        print("before try")
        # try:
        #     cx_Oracle.init_oracle_client(lib_dir = lib_dir)
        # except Exception as error:
        #     pass
        #     print("Error handling cx_Oracle.init_oracle_client")
        #     print(error)
        #     # sys.exit(1)

        try:
            self.conn_targetDB = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
        except Exception as error:
            pass
            print("Error with creating connection")
            print(error)
    ## NEW CODE BLOCK HERE
    def Engine_Start_Metadata_Update(self):
        #######SET MODEL NAME##############
        #MODEL_NAME = 'HEALTH RBD ENGINE'
        cursor_targetDB = self.conn_targetDB.cursor()

        #######INSERT QUERY################
        parameters= dict( name = config.MODEL_NAME, status = 'IN PROGRESS', time = datetime.now())
        sql_query_time = 'INSERT INTO ' + config.DB['input_schema'] + '.MLMODEL_DETAILS (MODEL_NAME, MODEL_STATUS, MODEL_START_TIME) VALUES (:name,:status,:time)'
        result = cursor_targetDB.execute(sql_query_time,parameters)
        #return (result, time)
        self.conn_targetDB.commit()
        cursor_targetDB.close()
        #self.conn_targetDB.close()

    def Engine_End_Metadata_Update(self):
        
        #######GET START_TIME#################

        #self._connect()
        cursor_targetDB = self.conn_targetDB.cursor() 
        
        
        MODEL_NAME = config.MODEL_NAME
        
        cursor_targetDB.execute('SELECT * FROM ' + config.DB['input_schema'] + '.MLMODEL_DETAILS where MODEL_NAME = :name AND MODEL_STATUS = \'IN PROGRESS\' ORDER BY MODEL_START_TIME DESC ', name = config.MODEL_NAME)
        model_details = cursor_targetDB.fetchall()

        index, model_name, model_status, start_time, end_time = model_details[0]
        # for rows in model_details:
        #     if config.MODEL_NAME == rows[1]:# and time_start == rows[3]:
        #         index, model_name, model_status, start_time, end_time = rows
                #print('found')
        #print(rows)

        #######UPDATE##################
        parameters= dict(status1 = 'COMPLETED',end = datetime.now(), name =config.MODEL_NAME, start1 = start_time )
        sql_query_time = 'UPDATE ' + config.DB['input_schema'] + '.MLMODEL_DETAILS SET MODEL_STATUS = :status1 ,MODEL_END_TIME = :end WHERE MODEL_NAME = :name and MODEL_START_TIME = :start1'
        result = cursor_targetDB.execute(sql_query_time,parameters)
        self.conn_targetDB.commit()
        cursor_targetDB.close()

    def Engine_Fail_Metadata_Update(self):

        #######GET START_TIME#################

        # self._connect()
        cursor_targetDB = self.conn_targetDB.cursor()
        cursor_targetDB.execute('SELECT * FROM ' + config.DB['input_schema'] + '.MLMODEL_DETAILS where MODEL_NAME = :name AND MODEL_STATUS = \'IN PROGRESS\' ORDER BY MODEL_START_TIME DESC', name=config.MODEL_NAME)
        model_details = cursor_targetDB.fetchall()

        index, model_name, model_status, start_time, end_time = model_details[0]
        #######UPDATE##################
        parameters = dict(status1='FAILED', end=datetime.now(), name=config.MODEL_NAME, start1=start_time)
        sql_query_time = 'UPDATE ' + config.DB['input_schema'] + '.MLMODEL_DETAILS SET MODEL_STATUS = :status1 ,MODEL_END_TIME = :end WHERE MODEL_NAME = :name and MODEL_START_TIME = :start1'
        result = cursor_targetDB.execute(sql_query_time, parameters)
        self.conn_targetDB.commit()
        cursor_targetDB.close()

    def backup (self,tablename):
        import os

        cursor_targetDB = self.conn_targetDB.cursor() 
        MODEL_NAME = config.MODEL_NAME
        cursor_targetDB.execute('SELECT * FROM ' + config.DB['input_schema'] + '.MLMODEL_DETAILS where MODEL_NAME = :name AND MODEL_STATUS = \'COMPLETED\' ORDER BY MODEL_START_TIME DESC', name = config.MODEL_NAME)
        model_details = cursor_targetDB.fetchall()

        if len(model_details) > 0:
            index, model_name, model_status, start_time, end_time = model_details[0]
            #backup_tablename = tablename +'_'+str(pd.to_datetime(start_time).strftime("%d/%m/%Y"))
            backup_tablename = tablename +'_'+str(start_time)
            
            query = 'CREATE TABLE "{}"."{}" AS (SELECT * FROM "{}"."{}")'.format(config.DB['outputschema'],backup_tablename,config.DB['outputschema'],tablename)
            try:
                cursor_targetDB.execute(query)
            except cx_Oracle.DatabaseError as e:
                pass
            self.conn_targetDB.commit()
        cursor_targetDB.close()

    def insert_df_Batchwise(self, df, tablename, batch_size):
        import sqlalchemy
        import pandas as pd
        import cx_Oracle
        import config
        from sqlalchemy import create_engine
        
        conn = self.conn_targetDB
        cursor = conn.cursor()
        df=df.astype(str)
        # for col_name in df.columns():
        #     if 'date' in col_name
        df['RUN_TIME'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['RUN_TIME'] = df['RUN_TIME'].astype(str)
        df.columns = df.columns.str.upper()
        # if "GEOMETRY" in df.columns:
        #     df.drop(['GEOMETRY'], axis=1, inplace=True)
        #df['GEOMETRY']=df['GEOMETRY'].astype(str)
        dtypedict = {}
        print(df.columns)
        for item in df.columns:
            if 'date' in item.lower():
                print(item)
                dtypedict[item] = sqlalchemy.types.DateTime()
                df[item] = df[item].replace('NaT', '')
                df[item] = pd.to_datetime(df[item], errors='coerce')
            elif 'geometry' not in item.lower():
                dtypedict[item] = sqlalchemy.types.VARCHAR(length = 255)
                #df[item] = df[item].replace('NaT', '')
                #df[item] = pd.to_datetime(df[item], errors='coerce')
        #df = df.where(pd.notnull(df), None)
        print(dtypedict)
        df_temp = df.head(1)


        df_temp.head(1).to_sql(name='{}'.format(tablename.lower()), con=self.engine, if_exists='replace', schema=config.DB['outputschema'], 
        dtype = dtypedict)
        
        # DELETE 1 ROW.. DONT DROP
        
        query = '''
                SELECT * FROM "{}"."{}"
                '''.format(config.DB['outputschema'],tablename)
        print(query)
        cursor.execute(query)

        rows = cursor.fetchall()
        #print(rows)
        col_names = [row[0] for row in cursor.description]

        query1 = '''
                TRUNCATE TABLE "{}"."{}"
                '''.format(config.DB['outputschema'],tablename)
        cursor.execute(query1)
        
        conn.commit()
        
        # FETCH TABLE SCHEMA
        query = 'SELECT * FROM "' + config.DB['outputschema'] + '"."{}"'.format(tablename)
        cursor.execute(query)

        rows = cursor.fetchall()
        col_names = [row[0] for row in cursor.description]
        #print(col_names)
        schema_str = ''
        num_str = ''
        schema_str = ''
        i1 = 0
        
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
                # print("This is schema string\n")
                # print(schema_str)
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

        # UPDATE ALL ROWS
        splits = []
        for i in range(0,df.shape[0], batch_size):
            splits.append(df[i:i+batch_size])
        
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
        cursor.close()