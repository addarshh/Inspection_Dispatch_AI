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

def getPopulationData():
    # sqlQuery = \
    #     f"SELECT g.GRIDUNIQUECODE GridNumber,g.MUNICIPALITY MUNICIPA_1, g.AMANACODE AMANACODE, g.DN DN, SDE.ST_ASTEXT(SHAPE) geometry \
    #     FROM  GISOWNER.GGMUNICIPALITYGRID g "
    # self.shpGrid = self._executeQuery(sqlQuery)
    # self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
    # self.shpGrid = geopandas.GeoDataFrame(self.shpGrid , geometry=geopandas.GeoSeries.from_wkt(self.shpGrid.geometry, crs = 'epsg:4269'))
    # self.shpGrid.dn = self.shpGrid.dn.astype('float')
    # logging.info("shape of population data is :"+str(self.shpGrid.shape))
    # return self.shpGrid

    shpGrid=pd.read_csv(config.population_grids_path,dtype={"amanacode":"str","municipalitycode":"str","regioncode":"str"})
    #self.shpGrid.geometry = self.shpGrid.geometry.astype('str')
    shpGrid = geopandas.GeoDataFrame(shpGrid , geometry=geopandas.GeoSeries.from_wkt(shpGrid.geometry, crs = 'epsg:4326'))
    shpGrid.columns  = map(str.upper, shpGrid.columns)
    shpGrid = shpGrid.rename(columns={'GEOMETRY':'geometry'})
    shpGrid = shpGrid.rename(columns={'GRIDUNIQUECODE':'GridNumber'})
    shpGrid = shpGrid.rename(columns={'GRIDNAME':'GridName','MUNICIPALITY':'MUNICIPALI', 'MUNICIPALITYCODE':'MUNICIPA_1' })
    print('self.shpGrid.shape')
    print(shpGrid.shape)
    return shpGrid

def get_grid_zone():
    # if not config.DB['instaclientpath'] in os.environ['PATH']:
    #     os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclientpath']

    # #conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    # conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
    # cursor = conn.cursor()
    # #sql_query = """SELECT * FROM "C##MOMRAH".INSPECTION_DATA"""
    
    # sql_query = """SELECT * FROM (SELECT g.*, ROW_NUMBER() OVER (PARTITION BY g.GRIDUNIQUECODE 
    #                     ORDER BY to_number(substr(g.GRID_COVERAGE_PERC,1,LENGTH(g.GRID_COVERAGE_PERC) - 1)) DESC) AS rnk
    #                     FROM "C##MOMRAH".GGGRIDINSPECTIONZONEST g WHERE g.INSPECTIONTYPE=1
    #                     ) WHERE RNK =1"""
    # #zone_data = self._executeQueryToDf(sqlQuery)
    # cursor.execute(sql_query)
    # rows = cursor.fetchall()
    # zone_data = pd.DataFrame(rows)
    # #zone_data = self._executeQuery(sqlQuery)
    # return zone_data

    grid_file =  path.join(config.GISPATH,'GGGRIDINSPECTIONZONEST.csv')
    GridZones = pd.read_csv(grid_file)
    GridZones.columns = map(str.upper, GridZones.columns)
    #self.GridZones.drop(columns=['RNK'], inplace=True)
    GridZones = GridZones.loc[GridZones['INSPECTIONTYPE'] == 6]
    logging.info("Function:priorityAreas df:GridZones Shape: {}".format(GridZones.shape))


    return GridZones

def getMUNICIPALITY():
    # sqlQuery = \
    #     """SELECT b.OBJECTID, \
    #     b.REGION, \
    #     b.AMANACODE, \
    #     b.AMANAARNAME AMANAARNAM, \
    #     b.AMANAENAME, \
    #     SDE.ST_AREA(b.SHAPE) SHAPE_AREA	, \
    #     SDE.ST_LENGTH(b.SHAPE) SHAPE_LEN, \
    #     SDE.ST_ASTEXT(b.SHAPE) geometry \
    #     FROM GISOWNER.BBAMANABOUNDARYS b  \
    #     """
    # self.AMANA = self._executeQuery(sqlQuery)
    MUNICIPALITY=pd.read_csv(config.municipality_path, dtype={"amanacode":"str"})
    #self.AMANA.geometry = self.AMANA.geometry.astype('str')
    MUNICIPALITY = geopandas.GeoDataFrame(MUNICIPALITY , geometry=geopandas.GeoSeries.from_wkt(AMANA.geometry, crs = 'epsg:4326'))
    MUNICIPALITY.columns  = map(str.upper, MUNICIPALITY.columns)
    MUNICIPALITY = MUNICIPALITY.rename(columns={'GEOMETRY':'geometry'})
    print('self.MUNICIPALITY.shape')
    print(MUNICIPALITY.shape)
    return MUNICIPALITY

class Database():

    def __init__(self, amanaCode = None):
        self.AmanaMunicipality = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['amana_municipality']))
        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
            + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
        self.connection = None
        self.engine = None
        self._connect()

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
        self.poiData = None
 
        self.excavationPhases = None
        self.drillingInspectionsDf = None
        self.drillingLicensesDf = None

        self.groupHousingLicenses = None

        self.amanaCode = amanaCode

    def _connect(self):
        lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])

        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)

        except Exception as error:
            print("Error handling cx_Oracle.init_oracle_client")
            print(error)
            #sys.exit(1)

        try:
            self.engine = sql.create_engine(self.connectionString)
            self.connection = self.engine.connect()
        except Exception as error:
            print("Error with creating connection")
            print(error)
            #sys.exit(1)

    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None

    def getInspectionsData(self):
        if self.inspectionsDf is None:
            filterStr = ''
            # if self.amanaCode is not None:
            #     filterStr += f"AND AM.AMANACODE = '{self.amanaCode}'"
            # self.inspectionsDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['inspections']))
            sqlQuery = \
                f"SELECT INSEPECTION_ID \"INSPECTION ID\", \
                    LICENSE_NUMBER \"LICENSE NUMBER\", \
                    STATUS_OF_WORK \"Status of Work\", \
                    INSPECTYPE_TYPE_ID \"INSPECTYPE ID\",  \
                    BUSINESS_ACTIVITY_WEIGHT \"Business Activity Weight\", INSPECTION_DATE \"Inspection Date\", \
                    DEGREE_OF_COMPLIANCE \"Degree of Compliance\", \
                    NUMBER_OF_CLAUSES \"Number of clauses\", \
                    NUMBER_OF_COMPLIANT_CLAUSES \"COMPLIANT_CLAUSES_NUMBER\", \
                    NUMBER_OF_NONCOMPLIANT_CLAUSES \"Number of non-compliant clauses\", \
                    NUMBER_OF_NONCOMPLIANT_CLAUSES_AND_HIGH_RISK \"Number of non-compliant clauses and High risk\",\
                    ISSUED_FINE_AMOUNT \"Issued fine amount\", FINE_PAYMENT_STATUS \"Fine payment status\" \
                FROM {config.DB['input_schema']}.INSPECTION_DATA INSP\
                WHERE LICENSE_NUMBER IS NOT NULL \
                "
                #TODO CHECK WHERE CONDITION " AND INSPECTION_DATE IS NOT NULL {filterStr}"
                #TODO CHECK WHERE CONDITION "AND DEGREE_OF_COMPLIANCE IS NOT NULL" IS USED IN THE ENGINES
                #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
                # JOIN {config.DB['input_schema']}.AMANA AM ON AM.AMANAARNAME = INSP.Amana\
            self.inspectionsDf = self._executeQueryToDf(sqlQuery)
            self.inspectionsDf['Issued fine amount'] = self.inspectionsDf['Issued fine amount'].astype(float)
            print('self.inspectionsDf')
            print(self.inspectionsDf.shape)


    def getLicensesData(self):
        sqlQuery = \
            f"SELECT LICENCES_ID \"License ID (MOMRAH)\", LATITUDE \"Latitude\", LONGITUDE \"Longitude\", \
                FACILITY_TYPE \"Facility type\", BUSINESS_ACTIVITY \"Business activity\", \
                LICENSE_START_DATE \"License Start Date\", LICENSE_EXPIRY_DATE \"License Expiry Date\", \
                LAST_LICENSE_RENEWAL_DATE \"Last License renewal date\", \
                AREA_OF_THE_RETAIL_OUTLET \"Area of the retail outlet\", \
                TENANCY_OWN_OR_RENTED \"Tenancy (Own/Rented)\" \
            FROM {config.DB['input_schema']}.LICENSES_DATA \
            WHERE LATITUDE NOT LIKE '%،%' AND LONGITUDE NOT LIKE '%،%' AND LONGITUDE NOT LIKE '%' || chr(10) ||'%' \
            AND LATITUDE NOT LIKE '%' || chr(10) ||'%' AND LONGITUDE NOT LIKE '%,%' AND LATITUDE NOT LIKE '%,%'"
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.licensesDf = self._executeQueryToDf(sqlQuery)
        print('self.licensesDf')
        print(self.licensesDf.shape)



    def getLicensesKeysData(self):
        sqlQuery = \
        f"SELECT D_ACTIVITIES_ID, D_ACTIVITIES_NAME, IS_ENABLE, ACTIVITIE_TYPE_ID, \
            ACTIVITIE_TYPE_NAME, ACTIVITYNO AS \"MOMTATHEL ACTIVITY NUMBER\"\
        FROM {config.DB['input_schema']}.MOMTHATEL_DATA"

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
        sqlQuery = \
        f"SELECT DISTINCT PYID \"CaseId\", INTERACTIONTYPE, PXCREATEDATETIME,  \
            CLOSURE_DATE, SHORT_STATUS, LATITUDE \"LATITUDE\", LONGITUDE,  \
            MAIN_CLASSIFICATION \"MAIN_Classificaion\", SUB_CLASSIFICATION \"Sub_Classificaion\",  \
            SP_CLASSIFICATION \"SP_Classificaion\",\
            CATEGORY, PRIORITY \"Priority_Value\", SATISFACTION \"Satisfaction\" \
        FROM {config.DB['input_schema']}.CRM_INSPECTION_CASES \
        WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL \
        "
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
        print('self.crmData')
        print(self.crmData.shape)
        
    # def getAmanaDataGdf(self):
    #     self.amanaDataGdf = geopandas.read_file(path.join(path.dirname(__file__),config.PATHS['municipality']))

    #     if self.amanaCode is not None:
    #         self.amanaDataGdf = self.amanaDataGdf.loc[(self.amanaDataGdf['AMANACODE'] == self.amanaCode)]

    # def getAmanaPopulationOverlay(self):
    #     if self.amanaPopulationOverlay is None:
    #         self.amanaPopulationOverlay = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data3']))
 


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
        sqlQuery = f"INSERT INTO {config.DB['input_schema']}.MLMODEL_DETAILS \
            (MODEL_NAME, MODEL_STATUS, MODEL_START_TIME)\
            VALUES(:name, :status, :time) \
            RETURNING ID into :id"
        parameters= dict( name = modelName, status = status, time = startTimestamp, id = id)
        
        cursor.execute(sqlQuery, parameters)
        self.connection.connection.commit()
        return str(id.getvalue()[0])
        
    def updateModelStatus(self, recordId : str, endTimestamp : str, status : str):
        sqlQuery = f"UPDATE {config.DB['input_schema']}.MLMODEL_DETAILS \
            SET MODEL_STATUS = :status, MODEL_END_TIME = :time \
            WHERE ID = :id"
        parameters= dict( status = status, time = endTimestamp, id = recordId)
        
        self.connection.execute(sqlQuery, parameters)


    def _executeQueryToDf(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)

    def deleteFromTable(self, tableName : str):
        tableName = tableName.upper()
        sqlQuery = f"TRUNCATE TABLE {config.DB['outputschema']}.{tableName}"

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

        insert_sql = 'INSERT INTO {}.{} ({}) VALUES ({})'.format(config.DB['outputschema'], table_name.upper(), columns, values)
        for i in range(0, dataframe.shape[0], 10000):
            splits.append(dataframe[i:i+batch_size])
        
        for i in range(len(splits)):
            
            df_row_list = [tuple(x) for x in splits[i].values.tolist()]

            cursor.executemany(insert_sql, df_row_list, batcherrors=True)
            self.connection.connection.commit()
        cursor.close()


    def getExcavationInspections(self):
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

    def getExcavationLicenses(self):
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
    

    def getExcavationPhases(self):
        self.excavationPhases = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['excavation_phases']))

    def getExcavationData(self):
        self.getExcavationInspections()
        self.getExcavationLicenses()


    def getLicensesGroupHousingData(self):
        sqlQuery = f" \
            SELECT ORDER_NUMBER \"Order Number\", TO_CHAR(APPLICATION_DATE, 'YYYY-MM-DD') \"Application Date\", \
            LICENSE_ID , ISSUE_DATE \"ISSUE_DATE_new\", \
            TO_CHAR(LICENSE_END_DATE, 'YYYY-MM-DD') \"License_end_date_new\", BENEFICIARY_ID \"Beneficiary ID\",  \
            AREA, X, Y, PROPERTY_RATE, \
            ESTIMATED_CAPACITY, ROOMS_COUNT, TOILETS_COUNT, ACCOMODATION_TYPE_INT \"Accomodation Type\", \
            HR_PATH_TYPE_INT \"HR_Path_Type\", AMANA  \
        FROM {config.DB['input_schema']}.LICENSES_DATA_GROUPHOUSING \
        "
        self.groupHousingLicenses = self._executeQueryToDf(sqlQuery)
        self.groupHousingLicenses.rename(columns={'license_id' : 'LICENSE_ID', 'x' : 'LATITUDE', 'y' : 'LONGITUDE',
            'estimated_capacity':'ESTIMATED_CAPACITY', 'rooms_count': 'ROOMS_COUNT', 'toilets_count' :'TOILETS_COUNT', 
            'area' : 'AREA', 'amana' : 'AMANA'}, inplace=True)


    def getConstructionLicenses(self):
        self.constructionLicenses = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_licenses']), thousands=r',', low_memory=False)
        self.constructionLicenses.drop(columns=['1_Project_start-up_report_date',
            '2_The_date_of_the_report_prior_to_pouring_the_foundations',
            '3_Report_date_after_foundation_casting',
            '4_Date_of_pre-molding_report', '5_Report_date_after_casting_medets',
            '6_Report_date_before_casting_columns',
            '7_Report_date_after_pouring_the_roof_of_each_role',
            '8_Report_date_after_casting_each_turn',
            '9_The_date_of_the_end_of_the_bone_and_the_beginning_of_the_electromechanical_stage_report',
            '10_The_date_of_the_end-of-destinations_stage_report_and_the_beginning_of_isolation_work',
            '11_Electromechanical_Extension_End_Report_Date',
            '12_Date_of_the_tiling_and_window_installation_stage_report',
            '13_Date_of_the_final_inspection_stage_report',
            '14_Date_of_issuance_of_the_work_certificate'], inplace=True)
        buildingInfo = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['building_type']), thousands=r',')
        self.constructionLicenses = pd.merge(left=self.constructionLicenses, right = buildingInfo, on='BUILDING TYPE ID', how='left')
        
        buildingInfo = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['building_use']), thousands=r',')
        self.constructionLicenses = pd.merge(left=self.constructionLicenses, right = buildingInfo, on='Building main use id', how='left')
        
        buildingInfo = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['building_subuse']), thousands=r',')
        self.constructionLicenses = pd.merge(left=self.constructionLicenses, right = buildingInfo, on='Building sub use id', how='left')

        amanaNameMap = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['amana_name_map']))
        self.constructionLicenses = pd.merge(left=self.constructionLicenses, right = amanaNameMap, on='Amana', how='left')

        # print(self.constructionLicenses.columns)
        # print(self.constructionLicenses)
        # print()

    def getConstsructionInspections(self):
        self.constructionInspection = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_inspections']), thousands=r',', parse_dates=['INSPECTION_DATE'], low_memory=False)
        self.constructionInspection['License ID']=self.constructionInspection['LIC_ID']
        # self.constructionInspection.rename(columns={'LIC_ID' : 'License ID'}, inplace=True)

    def getConstsructionInspectionsFloors(self):
        if self.constructionInspection is None:
            self.getConstsructionInspections()
        Construction_floors1_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_floor1']), thousands=r',', dtype={'License id' : np.int64 })
        Construction_floors2_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_floor2']), thousands=r',',dtype={'License id' : np.int64})
        Construction_floors3_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_floor3']), thousands=r',',dtype={'License id' : np.int64})
        Construction_floors_df = pd.concat([Construction_floors1_df,Construction_floors2_df,Construction_floors3_df])

        FloorTypeEngMap_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['floor_type']))
        component_usage_eng_map_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['component_usage']))

        Construction_floors_df = pd.merge(left=Construction_floors_df, right = FloorTypeEngMap_df, on='Floor type id', how='left')
        Construction_floors_df = pd.merge(left=Construction_floors_df, right = component_usage_eng_map_df, on='component usage id', how='left')
        # Construction_floors_df.rename(columns = {'License id': 'License ID'}, inplace=True)
        Construction_floors_df['License ID'] = Construction_floors_df['License id']
        
        # print(self.constructionInspection.dtypes)
        # for columns in self.constructionInspection.columns:
        #     print(columns)
        #     print(self.constructionInspection[columns].unique())
        # print()
        self.constructionInspectionFloor = pd.merge(left=self.constructionInspection, right = Construction_floors_df, on='License ID', how='left')


        self.constructionInspectionFloor.loc[self.constructionInspectionFloor['floor count'].isna(),'floor count']=0
        self.constructionInspectionFloor['floor count'] = self.constructionInspectionFloor['floor count'].astype(np.int64)


        
    def getConstructionSetbackRebound(self):
        Construction_SetbackRebound1_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_setback_rebound1']), thousands=r',')
        Construction_SetbackRebound2_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_setback_rebound2']), thousands=r',')
        Construction_SetbackRebound3_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_setback_rebound3']), thousands=r',')

        self.ConstructionSetbackReboundDf = pd.concat([Construction_SetbackRebound1_df,Construction_SetbackRebound2_df,Construction_SetbackRebound3_df])
        
        self.ConstructionSetbackReboundDf['Setback'] = self.ConstructionSetbackReboundDf['Setback'].astype('float')
        self.ConstructionSetbackReboundDf['Rebound'] = self.ConstructionSetbackReboundDf['Rebound'].astype('float')
        
        self.ConstructionSetbackReboundDf = self.ConstructionSetbackReboundDf.groupby(['License id']).agg({
            'Setback' : [ 'mean'],
            'Rebound' : [ 'mean'] }).reset_index()

        self.ConstructionSetbackReboundDf.columns = self.ConstructionSetbackReboundDf.columns.map(''.join)
        self.ConstructionSetbackReboundDf.rename(columns = {'License id': 'License ID'}, inplace=True)

    def getConstructionData(self):
        self.getConstructionLicenses()
        self.getConstsructionInspections()
        self.getConstsructionInspectionsFloors()
        self.getConstructionSetbackRebound()
    


        
