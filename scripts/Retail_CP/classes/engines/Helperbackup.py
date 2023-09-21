import sqlalchemy
import pandas as pd
import cx_Oracle
import config
from sqlalchemy import create_engine
import os   
from abc import ABC 
from datetime import datetime

## NEW CODE BLOCK HERE
def Engine_Start_Metadata_Update():
    #######SET MODEL NAME##############
    #MODEL_NAME = 'HEALTH RBD ENGINE'
    if not config.DB['instaclinetpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclinetpath']
    conn_targetDB = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service'] ))
    cursor_targetDB = conn_targetDB.cursor()

    #######INSERT QUERY################
    parameters= dict( name = config.MODEL_NAME, status = 'IN PROGRESS', time = datetime.now())
    sql_query_time = """Insert into %s.MLMODEL_DETAILS \
                (MODEL_NAME, MODEL_STATUS, MODEL_START_TIME) \
                    VALUES (:name,:status,:time)""" % config.DB['input_schema']
    result = cursor_targetDB.execute(sql_query_time,parameters)
    #return (result, time)
    conn_targetDB.commit()
    cursor_targetDB.close()

def Engine_End_Metadata_Update():
    
    #######GET START_TIME#################
    if not config.DB['instaclinetpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclinetpath']
    conn_targetDB = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service'] ))
    cursor_targetDB = conn_targetDB.cursor() 
    MODEL_NAME = config.MODEL_NAME
    
    cursor_targetDB.execute(
    '''
    Select 
        *
    FROM %s.MLMODEL_DETAILS where MODEL_NAME = :name \
    AND MODEL_STATUS =  'IN PROGRESS' ORDER BY MODEL_START_TIME DESC \
    ''' % config.DB['input_schema'],name=config.MODEL_NAME)
    model_details = cursor_targetDB.fetchall()

    index, model_name, model_status, start_time, end_time = model_details[0]
    # for rows in model_details:
    #     if config.MODEL_NAME == rows[1]:# and time_start == rows[3]:
    #         index, model_name, model_status, start_time, end_time = rows
            #print('found')
    #print(rows)

    #######UPDATE##################
    parameters= dict(status1 = 'COMPLETED',end = datetime.now(), name =config.MODEL_NAME, start1 = start_time )
    sql_query_time = "Update %s.MLMODEL_DETAILS \
                    set MODEL_STATUS = :status1 ,MODEL_END_TIME = :end \
                    where MODEL_NAME = :name and MODEL_START_TIME = :start1 \
                    " % config.DB['input_schema']
    result = cursor_targetDB.execute(sql_query_time,parameters)
    conn_targetDB.commit()
    cursor_targetDB.close()

def backup (tablename):
    import os
    if not config.DB['instaclinetpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclinetpath']
    conn_targetDB = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service'] ))
    
    cursor_targetDB = conn_targetDB.cursor() 
    MODEL_NAME = config.MODEL_NAME
    cursor_targetDB.execute(
    '''
    Select *
    FROM %s.MLMODEL_DETAILS where MODEL_NAME = :name \
    AND MODEL_STATUS =  'COMPLETED' ORDER BY MODEL_START_TIME DESC''' % config.DB['input_schema'],name=config.MODEL_NAME)
    model_details = cursor_targetDB.fetchall()

    index, model_name, model_status, start_time, end_time = model_details[0]
    #backup_tablename = tablename +'_'+str(pd.to_datetime(start_time).strftime("%d/%m/%Y"))
    backup_tablename = tablename +'_'+str(start_time)
    
    query = '''CREATE TABLE ""."{}" AS (SELECT * FROM "{}"."{}")'''.format(config.DB["outputschema"],backup_tablename,config.DB["outputschema"],tablename)
    try:
        cursor_targetDB.execute(query)
    except cx_Oracle.DatabaseError as e:
        pass
    conn_targetDB.commit()
    cursor_targetDB.close()

def insert_df( df, tablename):
    import sqlalchemy
    import pandas as pd
    import cx_Oracle
    import config
    from sqlalchemy import create_engine
    import os
    if not config.DB['instaclinetpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclinetpath']
    
    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    cursor = conn.cursor()
    # UPDATE 1 ROW
    
    engine = create_engine(
    "oracle+cx_oracle://{}:{}@10.80.122.102/ORCLCDB".format(
        config.DB['user'], config.DB['password'], isolation_level="AUTOCOMMIT")
    )
    df_temp = df.head(1).astype(str)
    #df_new = df_new.astype(str)
    df_temp.head(1).to_sql(name='{}'.format(tablename.lower()), con=engine, if_exists='replace', schema=config.DB["outputschema"])
    
    # DELETE 1 ROW.. DONT DROP
    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    cursor = conn.cursor()
    query = '''
            SELECT * FROM "{}"."{}"
            '''.format(config.DB.outputschema , tablename)
    print(query)
    cursor.execute(query)

    rows = cursor.fetchall()
    #print(rows)
    col_names = [row[0] for row in cursor.description]

    query1 = '''
            TRUNCATE TABLE "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
    cursor.execute(query1)
    
    conn.commit()
    
    # FETCH TABLE SCHEMA
    query = '''
            SELECT * FROM "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
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
            num_str = num_str + ', :' + str(i1)
        else:
            schema_str = schema_str + '", "' + item
            num_str = num_str + ', :' + str(i1)
        i1 = i1 + 1

    # UPDATE ALL ROWS
    
    df_new = df#.where((pd.notnull(df)), None)
    df_new = df_new.astype(str)
    df_row_list = [tuple(x) for x in df_new.values.tolist()]
    insert_sql = '''
        INSERT INTO "{}"."{}" ({}) VALUES ({})'''.format(config.DB["outputschema"],tablename,schema_str,num_str )
    cursor.executemany(insert_sql,df_row_list, batcherrors=True)
    conn.commit()
    cursor.close()


def insert_df_Amanawise( df, tablename):
    import sqlalchemy
    import pandas as pd
    import cx_Oracle
    import config
    from sqlalchemy import create_engine
    if not config.DB['instaclinetpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclinetpath']
    
    # UPDATE 1 ROW
    
    engine = create_engine(
    "oracle+cx_oracle://{}:{}@10.80.122.102/ORCLCDB".format(
        config.DB['user'], config.DB['password'], isolation_level="AUTOCOMMIT")
    )
    df_temp = df.head(1).astype(str)
    #df_new = df_new.astype(str)
    df_temp.head(1).to_sql(name='{}'.format(tablename.lower()), con=engine, if_exists='replace', schema=config.DB["outputschema"])
    
    # DELETE 1 ROW.. DONT DROP
    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    cursor = conn.cursor()
    query = '''
            SELECT * FROM "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
    print(query)
    cursor.execute(query)

    rows = cursor.fetchall()
    #print(rows)
    col_names = [row[0] for row in cursor.description]

    query1 = '''
            TRUNCATE TABLE "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
    cursor.execute(query1)
    
    conn.commit()
    
    # FETCH TABLE SCHEMA
    query = '''
            SELECT * FROM "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
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
            num_str = num_str + ', :' + str(i1)
        else:
            schema_str = schema_str + '", "' + item
            num_str = num_str + ', :' + str(i1)
        i1 = i1 + 1
    #print(len(col_names))
    #print(schema_str)
    #print(num_str)

    # UPDATE ALL ROWS
    splits = list(df.groupby('AMANA'))
    for i in range(len(splits)):
        df_new = splits[i][1]#.where((pd.notnull(df)), None)
        df_new = df_new.astype(str)
        df_row_list = [tuple(x) for x in df_new.values.tolist()]
        insert_sql = '''
            INSERT INTO "{}"."{}" ({}) VALUES ({})'''.format(config.DB["outputschema"],tablename,schema_str,num_str )
        #print(insert_sql)
        cursor.executemany(insert_sql,df_row_list, batcherrors=True)
    conn.commit()
    cursor.close()

def insert_df_Batchwise( df, tablename, batch_size):
    import sqlalchemy
    import pandas as pd
    import cx_Oracle
    import config
    from sqlalchemy import create_engine
    if not config.DB['instaclinetpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclinetpath']
    
    # UPDATE 1 ROW
    
    engine = create_engine(
    "oracle+cx_oracle://{}:{}@{}/ORCLCDB".format(
        config.DB['user'], config.DB['password'],config.DB['host'], isolation_level="AUTOCOMMIT")
    )
    df_temp = df.head(1).astype(str)
    #df_new = df_new.astype(str)
    df_temp.head(1).to_sql(name='{}'.format(tablename.lower()), con=engine, if_exists='replace', schema=config.DB["outputschema"])
    
    # DELETE 1 ROW.. DONT DROP
    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    cursor = conn.cursor()
    query = '''
            SELECT * FROM "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
    print(query)
    cursor.execute(query)

    rows = cursor.fetchall()
    #print(rows)
    col_names = [row[0] for row in cursor.description]

    query1 = '''
            TRUNCATE TABLE "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
    cursor.execute(query1)
    
    conn.commit()
    
    # FETCH TABLE SCHEMA
    query = '''
            SELECT * FROM "{}"."{}"
            '''.format(config.DB["outputschema"],tablename)
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
            num_str = num_str + ', :' + str(i1)
        else:
            schema_str = schema_str + '", "' + item
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
        df_new = df_new.astype(str)
        df_row_list = [tuple(x) for x in df_new.values.tolist()]
        insert_sql = '''
            INSERT INTO "{}"."{}" ({}) VALUES ({})'''.format(config.DB["outputschema"],tablename,schema_str,num_str )
        #print(insert_sql)
        cursor.executemany(insert_sql,df_row_list, batcherrors=True)
    
    conn.commit()
    cursor.close()