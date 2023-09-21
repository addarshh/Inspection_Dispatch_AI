from pyexpat import model
from statistics import multimode
from typing import Dict, List
import sqlalchemy as sql
import cx_Oracle
import pandas as pd
import config
import geopandas
import os.path as path
from shapely import wkt
import sys 
import numpy as np
from helpers import getStageNameDictionary, generatePOIFile
import logging
class Database():

    def __init__(self, amanaCode = None):

        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
            + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
        self.connection = None
        self.engine = None
        self._connect()

        self.inspectionsWithLicensesDf : pd.DataFrame = None
        self.licensesDf : pd.DataFrame = None
        self.licensesKeysDf : pd.DataFrame = None
        self.waterDf : pd.DataFrame = None
        self.priorityAreas : pd.DataFrame = None
        self.inspectionsDf : pd.DataFrame = None
        self.classConfig : pd.DataFrame = None
        self.amanaDataGdf = None
        self.amanaPopulationOverlay = None
        self.populationData = None
        self.amanaPopulationOverlay2 = None
        self.populationData2 = None
        self.amanaCode = amanaCode
        self.poiData = None
 
        self.excavationPhases = None
        self.drillingInspectionsDf = None
        self.drillingLicensesDf = None

        self.Construction_floors1_df : pd.DataFrame = None
        self.Construction_floors2_df : pd.DataFrame = None
        self.Construction_floors3_df : pd.DataFrame = None
        self.Construction_floors_df : pd.DataFrame = None

    def _connect(self):
        lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])

        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)

        except Exception as error:
            print("Error handling cx_Oracle.init_oracle_client")
            print(error)
            sys.exit(1)

        try:
            self.engine = sql.create_engine(self.connectionString)
            self.connection = self.engine.connect()
        except Exception as error:
            print("Error with creating connection")
            print(error)
            sys.exit(1)

    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None

    def getInspectionsData(self):

        filterStr = ''
        if self.amanaCode is not None:
            filterStr += f"AND AM.AMANACODE = '{self.amanaCode}'"
        # self.inspectionsDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['inspections']))
        sqlQuery = 'SELECT INSPECTION_ID "INSEPECTION ID", LICENSE_NUMBER "LICENSE NUMBER", INSPECTION_TYPE_ID "INSPECTYPE ID", BUSINESS_ACTIVITY_WEIGHT "Business Activity Weight", INSPECTION_DATE "Inspection Date", COMPLIANCE_DEGREE "Degree of Compliance", CLAUSES_NUMBER "Number of clauses", COMPLIANT_CLAUSES_NUMBER, NON_COMLIANT_CLAUSES_NUMBER "Number of non-compliant clauses", NON_COMPLIANT_CLAUSES_AND_HIGH_RISK "Number of non-compliant clauses and High risk", ISSUED_FINE_AMOUNT "Issued fine amount", FINE_PAYMENT_STATUS "Fine payment status" FROM "C##ACIO".INSPECTION_DATA ID JOIN "C##ACIO".AMANAS AM ON AM.AMANAARNAME = ID.Amana WHERE LICENSE_NUMBER IS NOT NULL AND INSPECTION_DATE IS NOT NULL AND COMPLIANCE_DEGREE IS NOT NULL {filterStr}'
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.inspectionsDf = self._executeQueryToDf(sqlQuery)
        self.inspectionsDf['Issued fine amount'] = self.inspectionsDf['Issued fine amount'].astype(float)


    def getLicensesData(self):
        sqlQuery = 'SELECT LICENCES_ID "License ID (MOMRAH)", LATITUDE "Latitude", LONGITUDE "Longitude ", FACILITY_TYPE "Facility type", BUSINESS_ACTIVITY "Business activity", LICENSE_START_DATE "License Start Date", LICENSE_EXPIRY_DATE "License Expiry Date", LAST_LICENSE_RENEWAL_DATE "Last License renewal date", AREA_OF_THE_RETAIL_OUTLET "Area of the retail outlet", TENANCY_OWN_OR_RENTED "Tenancy (Own/Rented)" FROM "C##ACIO".LICENSES_DATA WHERE LATITUDE NOT LIKE \'%،%\' AND LONGITUDE NOT LIKE \'%،%\' AND LONGITUDE NOT LIKE \'%\' || chr(10) ||\'%\' AND LATITUDE NOT LIKE \'%\' || chr(10) ||\'%\' AND LONGITUDE NOT LIKE \'%,%\' AND LATITUDE NOT LIKE \'%,%\''
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.licensesDf = self._executeQueryToDf(sqlQuery)


    def getLicensesKeysData(self):
        sqlQuery = 'SELECT D_ACTIVITIES_ID, D_ACTIVITIES_NAME, IS_ENABLE, ACTIVITIE_TYPE_ID, ACTIVITIE_TYPE_NAME, ACTIVITYNO AS "MOMTATHEL ACTIVITY NUMBER" FROM "C##ACIO".MOMTHATEL_DATA'

        self.licensesKeysDf = self._executeQueryToDf(sqlQuery)
        self.licensesKeysDf.columns = self.licensesKeysDf.columns.str.upper()
        

    def getWaterData(self):
        self.waterDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['water_data']))
        self.waterDf['X']=self.waterDf['XY Google'].str.split(' |\t|,').str[0]
        self.waterDf['Y']=self.waterDf['XY Google'].str.split(' |\t|,').str[1]

        self.waterDf=self.waterDf[~self.waterDf['Y'].isna()]

        self.waterDf=self.waterDf[self.waterDf['Water Agreement Status']=='Active']

        self.waterDf[['X','Y']]=self.waterDf[['X','Y']].apply(pd.to_numeric, errors='coerce')
        self.waterDf=self.waterDf[self.waterDf['X']<60]
        self.waterDf=self.waterDf[self.waterDf['Y']<60]

        self.waterDf[['new_X','new_Y']]=self.waterDf[['X','Y']].copy()

        self.waterDf.loc[self.waterDf['Y']<32, 'new_X']=self.waterDf[self.waterDf['Y']<33]['Y']
        self.waterDf.loc[self.waterDf['X']>33, 'new_Y']=self.waterDf[self.waterDf['X']>40]['X']

        self.waterDf[['X','Y']]=self.waterDf[['new_X','new_Y']].copy()

        self.waterDf = geopandas.GeoDataFrame(
            self.waterDf, geometry = geopandas.points_from_xy( self.waterDf['Y'], self.waterDf['X'],), crs="EPSG:4326")


    def getPopulationDataGdf(self):
        self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data']))

    def getPopulationDataGdf2(self):
        self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data2']))


    def getPriorityAreasData(self):
        self.priorityAreas = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['priority_areas']))
        self.priorityAreas = self.priorityAreas[self.priorityAreas['geometry'].notna()]
        self.priorityAreas = self._fromPdToGdf(self.priorityAreas)
        self.priorityAreas= self.priorityAreas.drop(columns=['Unnamed: 0'])


    def getCrmCases(self):
        sqlQuery = 'SELECT DISTINCT PYID "CaseId", INTERACTIONTYPE, PXCREATEDATETIME, CLOSURE_DATE, SHORT_STATUS, LATITUDE "LATITUDE", LONGITUDE, MAIN_CLASSIFICATION "MAIN_Classificaion", SUB_CLASSIFICATION "Sub_Classificaion", SP_CLASSIFICATION "SP_Classificaion",\CATEGORY, PRIORITY "Priority_Value", SATISFACTION "Satisfaction"FROM "C##ACIO".CRM_INSPECTION_CASESWHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL'
        # FETCH FIRST 2000 ROWS ONLY
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.crmData = self._executeQueryToDf(sqlQuery)
        self.crmData.rename(columns={'latitude' : 'LATITUDE', 'longitude' : 'LONGITUDE',
            'pxcreatedatetime' : 'PXCREATEDATETIME' }, inplace = True)
        #Dropping all the missing values present in Latitude and Longitude columns of the dataset
        # self.crmData = self.crmData.dropna(subset = ['LATITUDE','LONGITUDE'])
        self.crmData = self._fromPdToGdf(self.crmData, self.crmData.LONGITUDE, self.crmData.LATITUDE)
        self.crmData.Priority_Value.replace(
            {'حرج' : 3, 'عالي': 3, 'متوسط' : 2, 'عادي' : 1, 'High' : 3, 'high' : 3,'medium' : 2,  'Medium' : 2,'Low' : 1, 'low' : 1}, 
            inplace = True
        )
        
    def getAmanaDataGdf(self):
        self.amanaDataGdf = geopandas.read_file(path.join(path.dirname(__file__),config.PATHS['municipality']))

        if self.amanaCode is not None:
            self.amanaDataGdf = self.amanaDataGdf.loc[(self.amanaDataGdf['AMANACODE'] == self.amanaCode)]

    def getAmanaPopulationOverlay(self):
        if self.amanaPopulationOverlay is None:
            self.amanaPopulationOverlay = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data3']))


    def getAmanaPopulationOverlay2(self):
        if self.populationData2 is None:
            self.getPopulationDataGdf2()

        if self.amanaDataGdf is None:
            self.getAmanaDataGdf()

        self.amanaPopulationOverlay2 = geopandas.tools.sjoin(self.populationData, self.amanaDataGdf, how="inner", predicate="intersects")
        
        self.amanaPopulationOverlay2.drop(labels = self.amanaPopulationOverlay2.columns.difference(['GridNumber','geometry','DN']), axis = 1, inplace=True)
        self.amanaPopulationOverlay2.to_crs(epsg= 32637,inplace = True)

    
    def _fromPdToGdf(self, data, x = True,y = True):
        if 'geometry' in data.columns:
            try:
                gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
            except:
                data['geometry'] = data['geometry'].astype(str)
                data['geometry'] = data['geometry'].apply(wkt.loads)
                gpd = geopandas.GeoDataFrame(data,geometry = 'geometry', crs = 'epsg:4326')
                
        else:
             gpd = geopandas.GeoDataFrame(data,geometry = geopandas.points_from_xy(x,y), crs = 'epsg:4326')
        #gpd.to_crs('epsg:32637',inplace = True)
        return gpd 

    def getClassConfig(self):
        self.classConfig = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['class_config']))

    def getCommonData(self):
        self.getLicensesData()
        self.getLicensesKeysData()
        self.getInspectionsData()

    def insertModelStatus(self, modelName : str, startTimestamp : str, status : str) -> str:
        cursor = self.connection.connection.cursor()
        id = cursor.var(cx_Oracle.NUMBER)
        sqlQuery = 'INSERT INTO ' + config.DB['input_schema'] + '.MLMODEL_DETAILS (MODEL_NAME, MODEL_STATUS, MODEL_START_TIME) VALUES(:name, :status, :time) RETURNING ID into :id'
        parameters= dict( name = modelName, status = status, time = startTimestamp, id = id)
        
        cursor.execute(sqlQuery, parameters)
        self.connection.connection.commit()
        return str(id.getvalue()[0])
        
    def updateModelStatus(self, recordId : str, endTimestamp : str, status : str):
        sqlQuery = "UPDATE \"C##ACIO\".MLMODEL_DETAILS SET MODEL_STATUS = :status, MODEL_END_TIME = :time WHERE ID = :id"
        parameters= dict( status = status, time = endTimestamp, id = recordId)
        
        self.connection.execute(sqlQuery, parameters)


    def _executeQueryToDf(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)

    def deleteFromTable(self, tableName : str):
        tableName = tableName.upper()
        sqlQuery = f"DELETE FROM {config.DB['input_schema']}.{tableName}"

        self.connection.execute(sqlQuery)
   
    def getPoiDataGdf(self):
        file_exists = path.exists(path.join(path.dirname(__file__), config.FEATURE_CREATION_INPUT_FOLDER ,  "pois\pois_licenses_comparison.csv"))

        if file_exists is False:
            self.getAmanaPopulationOverlay()
            self.getLicensesData()
            pois1=pd.read_excel(path.join(path.dirname(__file__),config.PATHS['pois1']))
            pois2=pd.read_excel(path.join(path.dirname(__file__),config.PATHS['pois2']))
        
            self.poiData = generatePOIFile(self.amanaPopulationOverlay, self.licensesDf, pois1, pois2)
            pois1 = None
            pois2 = None
        else:
            self.poiData = pd.read_csv(path.join(path.dirname(__file__), config.FEATURE_CREATION_INPUT_FOLDER ,  "pois\pois_licenses_comparison.csv"))
            self.poiData.drop(columns=['Unnamed: 0'], inplace=True)

    def saveRBDOutputLicenses(self, dataframe : pd.DataFrame, table_name : str):
        dataframe.to_sql(name = table_name, con =  self.engine, schema = config.DB['input_schema'], if_exists='append', index =  False, chunksize = 10000, dtype = {'GEOMETRY' : sql.types.CLOB, 'LONGITUDE' : sql.types.FLOAT, 'LATITUDE' : sql.types.FLOAT,
            'LIC_ID' : sql.types.BIGINT, 'HASNOLICENSE' : sql.types.BIGINT, 'START_POINT_X' : sql.types.Float,
            'START_POINT_Y' : sql.types.Float }
        )

    
    def saveRBDOutput(self, dataframe : pd.DataFrame, table_name : str):

        splits = []
        batch_size = 10000
        cursor = self.connection.connection.cursor()

        for column in dataframe.columns:
            if dataframe[column].dtype == object:
                dataframe[column].fillna('', inplace = True)
            else:
                dataframe[column].replace({np.nan : None}, inplace = True)
        
        columns = ','.join(list(dataframe.columns)) 
        values = ','.join([':' + str(i + 1) for i in range(0, len(list(dataframe.columns)))])

        insert_sql = 'INSERT INTO {}.{} ({}) VALUES ({})'.format(config.DB['input_schema'], table_name.upper(), columns, values)
        for i in range(0, dataframe.shape[0], 10000):
            splits.append(dataframe[i:i+batch_size])
        
        for i in range(len(splits)):
            
            df_row_list = [tuple(x) for x in splits[i].values.tolist()]

            cursor.executemany(insert_sql, df_row_list, batcherrors=True)
            self.connection.connection.commit()
        cursor.close()


    def getDrillingInspections(self):
            sqlQuery = " \
                SELECT LIC_ID, INSPECTION_ID, TO_CHAR(INSPECTION_DATE, 'YYYY-MM-DD') INSPECTION_DATE, \
                CASE WHEN NUMBEROFFAILEDCLAUSES > 0 THEN 1 ELSE 0  END ISVIOLATION, STAGE_NAME \"STAGENAME\"\
                FROM C##ACIO.DRILLING_INSPECTION_DATA \
                WHERE PYSTATUSWORK IN ('Pending-ReceiveSample', 'Resolved-Completed', 'Under Review and Approval') \
            "
            # self.drillingInspectionsDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['drilling_inspections']), dtype = {'LIC_ID': str, 'INSPECTION_DATE' : str})
            self.drillingInspectionsDf = self._executeQueryToDf(sqlQuery)
            self.drillingInspectionsDf.rename(
                inplace=True,
                columns={'lic_id' : 'LIC_ID', 'inspection_id' : 'INSPECTION_ID', 'inspection_date' : 'INSPECTION_DATE',
                'isviolation' : 'ISVIOLATION','stagename' : 'STAGENAME'},
            )
            # self.drillingInspectionsDf['INSPECTION_DATE']=self.drillingInspectionsDf['INSPECTION_DATE'].astype(str).str.split(' ').str[0]
            self.drillingInspectionsDf['INSPECTION_DATE']=pd.to_datetime(self.drillingInspectionsDf['INSPECTION_DATE'])

            dictionary = getStageNameDictionary(self.drillingInspectionsDf['STAGENAME'].dropna().unique())
            self.drillingInspectionsDf=self.drillingInspectionsDf.replace({'STAGENAME':dictionary})
            #print()

    def getDrillingLicenses(self):
        sqlQuery = "\
            SELECT LIC_ID LIC_ID, START_POINT_X, START_POINT_Y, AMANA, MUNICIPALITY , \"SUB-MUNICIPALITY\",\
                DISTRICT_ID, DISTRICT_NAME, DIGGING_START_DATE, DIGGING_END_DATE, DIGGING_METHOD_ID, WORK_NATURE_ID, \
                NETWORK_TYPE_ID, HEAVY_EQUIPMENT_PERMISSION, CAMPING_ROOM_COUNT, CONTRACTOR_CR, PATH_CODE, \
                \"LENGTH\", WIDTH, \"DEPTH\",  \
                CASE REQUEST_TYPE WHEN 'اصدار تصريح حفرية طارئة' THEN 1 ELSE 0 END AS EMERGENCY_LICENSE, \
                CASE DIGGING_CATEGORY WHEN 'الأولى' THEN 1 \
                    WHEN 'الثانية' THEN 2 WHEN 'الثالثة' THEN 3 WHEN 'الرابعة' THEN 4 \
                    WHEN 'الخامسة' THEN 5 WHEN 'السادسة' THEN 6 WHEN 'السابعة' THEN 7 \
                    WHEN 'الثامنة' THEN 8 ELSE NULL END AS DIGGING_CATEGORY\
            FROM C##ACIO.DRILLING_LICENSES_DATA \
            ORDER BY LIC_ID ASC"
        
        self.drillingLicensesDf = self._executeQueryToDf(sqlQuery)
        self.drillingLicensesDf['lic_id'] = self.drillingLicensesDf['lic_id'].astype(str)
        self.drillingLicensesDf.rename(columns={'lic_id' : 'LIC_ID', 'start_point_x' : 'START_POINT_X',
            'start_point_y' : 'START_POINT_Y', 'depth':'DEPTH', 'path_code' : 'PATH_CODE', 
            'width': 'WIDTH', 'length' : 'LENGTH', 'district_id' : 'DISTRICT_ID', 'district_name' : 'DISTRICT_NAME',
            'amana' : 'AMMANA', 'municipality' : 'Municipality', 'SUB-MUNICIPALITY' : 'Sub-Municipality',
            'emergency_license' : 'EMERGENCY_LICENSE', 'digging_end_date' : 'DIGGING_END_DATE', 'digging_start_date' : 'DIGGING_START_DATE',
            'digging_category' : 'DIGGING_CATEGORY', 'contractor_cr' : 'CONTRACTOR_CR', 
            'heavy_equipment_permission' : 'HEAVY_EQUIPMENT_PERMISSION', 'camping_room_count' :  'CAMPING_ROOM_COUNT',
            'network_type_id' : 'NETWORK_TYPE_ID', 'digging_method_id' : 'DIGGING_METHOD_ID', 'work_nature_id' : 'WORK_NATURE_ID'
            }
            , inplace=True
        )
    
        #print(self.drillingLicensesDf.head())

    def getExcavationPhases(self):
        self.excavationPhases = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['excavation_phases']))

    def getExcavationData(self):
        self.getDrillingInspections()
        self.getDrillingLicenses()

## SEPERATE METHODS WRITTEN BELOW...

##1 getfromDBConstruction_floors - LICENCES_DATA_CONSTRUCTIONS_FLOORS
##2 getConstructionSetbackRebound - LICENCES_DATA_CONSTRUCTIONS_SETBACK
##3 getConstructionLicences - LICENCES_DATA_CONSTRUCTIONS
##4 getConstructionInspections - INSPECTION_DATA_CONSTRUCTION

    def getfromDBConstruction_floors(self):
        sqlQuery = 'SELECT LICENCES_ID "License id", REQUEST_ID "Request id", FLOOR_TYPE "Floor type", FLOOR_TYPE_ID "Floor type id", FLOOR_AREA "floor area", FLOOR_COUNT "floor count", UNITS_NO "units no", COMPONENT_USAGE "component usage", COMPONENT_USAGE_ID "component usage id", COMPONENT_PERCENTAGE "component percentage" FROM ' + config.DB['input_schema'] + '.LICENSES_DATA_CONSTRUCTIONS_FLOORS'
        self.Construction_floors_df = self._executeQueryToDf(sqlQuery)
        logging.info("shape of constructions floors df is :"+str(self.Construction_floors_df.shape))
        return self.Construction_floors_df

    def getConstructionSetbackRebound(self):
        sqlQuery = 'SELECT LICENCES_ID "License id", REQUEST_ID "Request id", DIRECTION "Direction", DIRECTION_ID "Direction id", SETBACK "Setback", REBOUND "Rebound" FROM ' + config.DB['input_schema'] + '.LICENSES_DATA_CONSTRUCTIONS_SETBACK'
        self.ConstructionSetbackRebound_df = self._executeQueryToDf(sqlQuery)
        logging.info("shape of constructions setback rebound is :"+str(self.ConstructionSetbackRebound_df.shape))
        return self.ConstructionSetbackRebound_df

    def getConstructionLicences(self):
        sqlQuery = 'SELECT BUILDING_ID "Building ID", LICENCES_ID "License ID", REQUEST_ID "Request ID", AMANA "Amana", MUNICIPALITY "Municipality", SUB_MUNICIPALITY "Sub-Municipality", LICENSE_START_DATE "ISSUE_DATE", LICENSE_EXPIRY_DATE "EXPIRATION_DATE", "LATITUDE" "lat", "LONGITUDE" "long", CONSULTANT_NAME "consultant name", CONSULTANT_LICENSE_ID "Consultant license id", BUILDING_TYPE "BUILDING TYPE", BUILDING_TYPE_ID "BUILDING TYPE ID", BUILDING_MAIN_USE "Building main use", BUILDING_MAIN_USE_ID "Building main use id", BUILDING_SUB_USE "Building Sub use", BUILDING_SUB_USE_ID "Building sub use id", PARCEL_AREA "Parcel area", CONTRACTOR "Contractor", CONTRACTOR_LICENSE_ID "Contractor license ID", BUILDING_HEIGHT "Building height",OWNER_ID "owner_id" FROM ' + config.DB['input_schema'] + '.LICENSES_DATA_CONSTRUCTIONS'
        self.ConstructionLicences_df=self._executeQueryToDf(sqlQuery)
        self.ConstructionLicences_df.rename(columns={'issue_date' : 'ISSUE_DATE',
          'expiration_date' : 'EXPIRATION_DATE' }, inplace = True)
        logging.info("shape of license data constructions is :"+str(self.ConstructionLicences_df.shape))
        return self.ConstructionLicences_df

    def getConstructionInspections(self):

        sqlQuery = 'SELECT STATUS_OF_WORK "PYSTATUSWORK", LICENSE_NUMBER "LIC_ID", INSEPECTION_ID "INSPECTION_ID", INSPECTION_DATE "INSPECTION_DATE", PHASE_NUMBER "PHASE_NUMBER", PHASE_NAME "PHASE_NAME", STAGENAME "STAGENAME", STAGENO "STAGENO", NUMBEROFFAILEDCLAUSES "NUMBEROFFAILEDCLAUSES", COMPLIANCE "COMPLIANCE", COMPLYINGITEMS "COMPLYINGITEMS", WORKS_STOPPED "WORKS_STOPPED", NO_LICENSE "HASNOLICENSE" FROM ' + config.DB['input_schema'] + '.INSPECTION_DATA_CONSTRUCTIONS'
        self.ConstructionInspections_df=self._executeQueryToDf(sqlQuery)
        self.ConstructionInspections_df.columns=[x.upper() for x in self.ConstructionInspections_df.columns]

        logging.info("shape of inspection data constructions is :"+str(self.ConstructionInspections_df.shape))
        return self.ConstructionInspections_df

        



    


        
