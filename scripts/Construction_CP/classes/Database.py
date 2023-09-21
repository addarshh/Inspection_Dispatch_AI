import pandas as pd
import config
import os.path as path
import geopandas
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
            pass
            #sys.exit(1)

    def get_crm_data(self):

        sqlQuery = 'SELECT DISTINCT PYID "CaseId", INTERACTIONTYPE, PXCREATEDATETIME, CLOSURE_DATE, SHORT_STATUS, LATITUDE "LATITUDE", LONGITUDE, MAIN_CLASSIFICATION "MAIN_Classificaion", SUB_CLASSIFICATION "Sub_Classificaion", SP_CLASSIFICATION "SP_Classificaion", CATEGORY, PRIORITY "Priority", SATISFACTION "Satisfaction" FROM ' + config.DB['input_schema'] + '.CRM_INSPECTION_CASES WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL AND SHORT_STATUS <> \'Close\''
        self.crm_data = self._executeQuery(sqlQuery)
        self.crm_data.columns = self.crm_data.columns.str.lower()
        print('CRM data:', self.crm_data.shape)
        return self.crm_data


    def get_licencedata_constructions(self):
        sqlQuery = 'SELECT BUILDING_ID "Building ID", LICENCES_ID "License ID",REQUEST_ID "Request ID",AMANA "Amana",MUNICIPALITY "Municipality",SUB_MUNICIPALITY "Sub-Municipality",LICENSE_START_DATE "ISSUE_DATE",LICENSE_EXPIRY_DATE "EXPIRATION_DATE",LATITUDE "lat",LONGITUDE "long",CONSULTANT_NAME "consultant name",CONSULTANT_LICENSE_ID "Consultant license id",BUILDING_TYPE "BUILDING TYPE",BUILDING_TYPE_ID "BUILDING TYPE ID",BUILDING_MAIN_USE "Building main use",BUILDING_MAIN_USE_ID "Building main use id",BUILDING_SUB_USE "Building Sub use",BUILDING_SUB_USE_ID "Building sub use id",PARCEL_AREA "Parcel area",CONTRACTOR "Contractor",CONTRACTOR_LICENSE_ID "Contractor license ID",BUILDING_HEIGHT "Building height",OWNER_ID "owner_id" FROM ' + config.DB['input_schema'] + '.LICENSES_DATA_CONSTRUCTIONS'
        self.datareq_constructions = self._executeQuery(sqlQuery)
        print('License data:', self.datareq_constructions.shape)
        # self.datareq_excavations.columns = self.datareq_excavations.columns.str.lower()
        return self.datareq_constructions


    def get_inspectionsdata_construction(self):
        sqlQuery = 'SELECT  STATUS_OF_WORK "PYSTATUSWORK",LICENSE_NUMBER "LIC_ID",INSEPECTION_ID "INSPECTION_ID",INSPECTION_DATE "INSPECTION_DATE", PHASE_NUMBER "PHASE_NUMBER",PHASE_NAME "PHASE_NAME",STAGENAME "STAGENAME",STAGENO "STAGENO",NUMBEROFFAILEDCLAUSES "NUMBEROFFAILEDCLAUSES",COMPLIANCE "COMPLIANCE",COMPLYINGITEMS "COMPLYINGITEMS",WORKS_STOPPED "WORKS_STOPPED",NO_LICENSE "HASNOLICENSE" FROM ' + config.DB['input_schema'] + '.INSPECTION_DATA_CONSTRUCTIONS'
        self.drilling_inspections = self._executeQuery(sqlQuery)
        self.drilling_inspections.columns = self.drilling_inspections.columns.str.lower()
        print('Inspection data:', self.drilling_inspections.shape)
        return self.drilling_inspections

    def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)


def save_final(Final_df, output_table_name):
    Helper.backup(output_table_name)
    # insert_df( df, output_table_name)
    Helper.insert_df_Batchwise(Final_df, output_table_name, 50000)
