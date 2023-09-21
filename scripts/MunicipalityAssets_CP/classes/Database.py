import pandas as pd
import config
import os.path as path
from classes.engines import Helper as Help
#from classes.engines.Helper import insert_df_Batchwise, backup
from abc import ABC
import sqlalchemy as sql
import cx_Oracle
import sys
Helper=Help.Helper()
class Database(ABC):
    def __init__(self, amanaCode=None):
        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
                                + cx_Oracle.makedsn(config.DB['host'], config.DB['port'],
                                                    service_name=config.DB['service'])
        self.connection = None
        self._connect()

        self.CRM_full: pd.DataFrame = None
        self.amanaCode = amanaCode

    def _connect(self):
        #lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])
        lib_dir = config.DB['instaclientpath']
        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)

        except Exception as error:
            print("Error handling cx_Oracle.init_oracle_client")
            print(error)
            #sys.exit(1)
            pass

        try:
            engine = sql.create_engine(self.connectionString)
            self.connection = engine.connect()
        except Exception as error:
            print("Error with creating connection")
            print(error)
            pass
            #sys.exit(1)

    def get_crm_data(self):

        sqlQuery="""SELECT 
            c.PYID AS "PYID",
            c.INTERACTIONTYPE AS "INTERACTIONTYPE",
            c.PXCREATEDATETIME AS "PXCREATEDATETIME",
            c.PYRESOLVEDTIMESTAMP AS "CLOSURE_DATE" ,
            c.PYSTATUSWORK AS "SHORT_STATUS" ,
            c.LATITUDE  AS "LATITUDE",
            c.LONGITUDE AS "LONGITUDE",
            c.MAINCLASSIFICATION AS "MAIN_Classificaion",
            c.SPLCLASSIFICATION AS "SP_Classificaion",
            c.CATEGORY AS "Category",
            c.PRIORITY AS "PRIORITY",
            c.EXTERNALCONTRACTOR AS "IS_Contractor",
            c.RESOLUTIONSATISFYBYPETITIONER AS  "Satisfaction",
            c.VISUAL_POLLUTION_CATEGORY AS "VISUAL POLLUTION CATEGORY",
            SUBMUNICIPALITYID AS "SUBMUNIC_3"
            FROM %s.CASES c""" % config.DB['input_schema']

        self.CRM_full = self._executeQuery(sqlQuery)
        return self.CRM_full


    def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)


def save_final(Final_df, output_table_name):
    Final_df.rename(columns={'caseid': 'CaseId'}, inplace=True)
    #print(Final_df.columns)
    #print(config.meta_data.columns)

    print("Final_df.shape: " + str(Final_df.shape))
    print("config.meta_data.shape: " + str(config.meta_data.shape))
    df_final1=pd.merge(Final_df, config.meta_data, on='CaseId', how='left')
    # df_final1 = df_final1.drop('CaseId', axis=1, inplace=True)
    print("print(df_final1.columns): " + df_final1.columns)
    print("df_final1.shape: " + str(df_final1.shape))
    Helper.backup(output_table_name)
    # insert_df( df, output_table_name)
    Helper.insert_df_Batchwise(df_final1, output_table_name, 50000)
