import pandas as pd
import config
import os.path as path
from classes.engines import Helper as Help
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

        self.crm_data: pd.DataFrame = None
        self.datareq_excavations: pd.DataFrame = None
        self.drilling_inspections: pd.DataFrame = None
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
            #sys.exit(1)
            pass

    def get_crm_data(self):
        
        sqlQuery = \
        f"SELECT DISTINCT PYID \"CaseId\", INTERACTIONTYPE, PXCREATEDATETIME,  \
            CLOSURE_DATE, SHORT_STATUS, LATITUDE \"LATITUDE\", LONGITUDE,  \
            MAIN_CLASSIFICATION \"MAIN_Classificaion\", SUB_CLASSIFICATION \"Sub_Classificaion\",  \
            SP_CLASSIFICATION \"SP_Classificaion\",\
            CATEGORY, PRIORITY \"Priority\", SATISFACTION \"Satisfaction\" \
        FROM %s.CRM_INSPECTION_CASES \
        WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL AND SHORT_STATUS <> 'Close'" % config.DB['input_schema']
        self.crm_data = self._executeQuery(sqlQuery)
        self.crm_data.columns = self.crm_data.columns.str.lower()
        print('CRM data records:', self.crm_data.shape)
        return self.crm_data
        #print( self.crm_data.columns)

    def get_datareq_excavations(self):
        sqlQuery = """SELECT LICENCES_ID "LIC_ID", REQUEST_ID "REQ_ID", LATITUDE "START_POINT_X", LONGITUDE "START_POINT_Y", AMANA "AMMANA", MUNICIPALITY "Municipality", SUB_MUNICIPALITY "Sub-Municipality", DISTRICT_ID "DISTRICT_ID", DISTRICT_NAME "DISTRICT_NAME", EMERGENCY_LICENSE "EMERGENCY_LICENSE", LICENSE_START_DATE "ISSUE_DATE", LICENSE_EXPIRY_DATE "EXPIRATION_GDATE", DIGGING_START_DATE "DIGGING_START_DATE", DIGGING_END_DATE "DIGGING_END_DATE", DIGGING_STATUS "DIGGING_STATUS", SITE_NAME "SITE_NAME", PROJECT_NAME "PROJECT_NAME", PROJECT_DESC "PROJECT_DESC", NAME "NAME", PROJECT_START_DATE "PROJECT_START_DATE", WORK_START_DATE "WORK_START_DATE", DIGGING_DURATION "DIGGING_DURATION", PROJECT_END_DATE "PROJECT_END_DATE", DIGGING_METHOD_ID "DIGGING_METHOD_ID", DIGGING_METHOD "DIGGING_METHOD", WORK_NATURE_ID "WORK_NATURE_ID", WORK_NATURE "WORK_NATURE", PATH_LENGTH_SUM "PATH_LENGTH_SUM", NETWORK_TYPE_ID "NETWORK_TYPE_ID", NETWORK_TYPE "NETWORK_TYPE", MAP_NO "MAP_NO", HEAVY_EQUIPMENT_PERMISSION "HEAVY_EQUIPMENT_PERMISSION", CAMPING_ROOM_COUNT "CAMPING_ROOM_COUNT", CONSULTANT_NAME "CONSULTANT_NAME", CONSULTANT_CR "CONSULTANT_CR", CONTRACTOR_NAME "CONTRACTOR_NAME", CONTRACTOR_CR "CONTRACTOR_CR", PATH_CODE "PATH_CODE", LENGTH "LENGTH", WIDTH "WIDTH", DEPTH "DEPTH", DIGGING_LATE_STATUS_ID "DIGGING_LATE_STATUS_ID", DIGGING_LATE_STATUS "DIGGING_LATE_STATUS", DIGGING_CATEGORY_ID "DIGGING_CATEGORY_ID", DIGGING_CATEGORY "DIGGING_CATEGORY", SERVICE_CONFIG "SERVICE_CONFIG", REQUEST_TYPE_ID "REQUEST_TYPE_ID", REQUEST_TYPE "REQUEST_TYPE"
        FROM %s.LICENSES_DATA_EXCAVATIONS""" % config.DB['input_schema']
        self.datareq_excavations = self._executeQuery(sqlQuery)
        #self.datareq_excavations.columns = self.datareq_excavations.columns.str.lower()
        print('Excavations data records:', self.datareq_excavations.shape)
        return self.datareq_excavations

    def get_drilling_inspections(self):
        sqlQuery = """SELECT  STATUS_OF_WORK "PYSTATUSWORK", LICENSE_NUMBER "LIC_ID", INSEPECTION_ID "INSPECTION_ID", INSPECTION_DATE "INSPECTION_DATE", PHASE_NUMBER "PHASE_NUMBER", PHASE_NAME "PHASE_NAME", STAGENAME "STAGENAME", STAGENO "STAGENO", NUMBEROFFAILEDCLAUSES "NUMBEROFFAILEDCLAUSES", COMPLIANCE "COMPLIANCE", COMPLYINGITEMS "COMPLYINGITEMS", WORKS_STOPPED "WORKS_STOPPED", NO_LICENSE "HASNOLICENSE"
        FROM %s.INSPECTION_DATA_EXCAVATIONS""" % config.DB['input_schema']
        self.drilling_inspections = self._executeQuery(sqlQuery)
        self.drilling_inspections.columns = self.drilling_inspections.columns.str.lower()
        print('Inspections data records:', self.drilling_inspections.shape)
        return self.drilling_inspections

    def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)



def save_final(Final_df, output_table_name):
    Helper.backup(output_table_name)
    Final_df=pd.merge(Final_df, config.meta_data, on='caseid', how='left')
    print('Final data records:', Final_df.shape)
    # insert_df( df, output_table_name)
    Helper.insert_df_Batchwise(Final_df, output_table_name, 50000)