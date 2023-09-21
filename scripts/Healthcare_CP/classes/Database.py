import sqlalchemy as sql
import cx_Oracle
import pandas as pd
import config
import geopandas
import os.path as path
from shapely import wkt
import sys 
from classes.engines import Helper as Help
Helper=Help.Helper()
from classes import GISDatabase as GDB
gdata=GDB.GISDatabase()
from abc import ABC
class Database(ABC):

    def __init__(self, amanaCode = None):
        

        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' \
            + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
        self.connection = None
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

    def _connect(self):
        lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])

        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)

        except Exception as error:
            #print("Error handling cx_Oracle.init_oracle_client")
            #print(error)
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

    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None

    def getInspectionsData(self):

        #self.inspectionsDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['inspections']))
        # self.inspectionsDf['INSPECTYPE ID'] = self.inspectionsDf['INSPECTYPE ID'].astype(str)
        # self.inspectionsDf['Inspection DateTime'] = self.inspectionsDf['Inspection Date']
        # self.inspectionsDf['Inspection Date'] = pd.to_datetime(self.inspectionsDf['Inspection Date'].str.split(' ').str[0])

        # self.inspectionsDf.loc[self.inspectionsDf['Degree of Compliance'].isna(), 'compliance_score_available'] = False
        # self.inspectionsDf['compliance_score_available'].fillna(True, inplace=True)
        
        # self.inspectionsDf = self.inspectionsDf[~self.inspectionsDf['LICENSE NUMBER'].isna()]
        # self.inspectionsDf = self.inspectionsDf[~self.inspectionsDf['Inspection Date'].isna()]
        # self.inspectionsDf = self.inspectionsDf[~self.inspectionsDf['Degree of Compliance'].isna()]
        sqlQuery = """
        SELECT LICENSE_NUMBER "LICENSE NUMBER" , INSEPECTION_ID "INSEPECTION ID" , INSPECTYPE_TYPE_ID "INSPECTYPE ID" ,\
                INSPECTION_NAME "INSPECTION NAME" , ESTABLISHMENT_NAME "Establishment Name" , BUSINESS_ACTIVITY_DESCRIPTION "Business Activity Description" , STATUS_OF_WORK "Status of Work" , TYPE_OF_VISIT "TYPE OF VISIT" , BUSINESS_ACTIVITY_NUMBER "Business Activity Number" , BUSINESS_ACTIVITY_WEIGHT "Business Activity Weight" , INSPECTION_DATE "Inspection Date" , DEGREE_OF_COMPLIANCE "Degree of Compliance" , NUMBER_OF_CLAUSES "Number of clauses" , NUMBER_OF_COMPLIANT_CLAUSES "Number of compliant clauses" , NUMBER_OF_NONCOMPLIANT_CLAUSES "Number of non-compliant clauses" , NUMBER_OF_NONCOMPLIANT_CLAUSES_AND_HIGH_RISK "Number of non-compliant clauses and High risk" , NUMBER_OF_NONCOMPLIANT_CLAUSES_AND_MEDIUM_RISK "Number of non-compliant clauses and medium risk" , ISSUED_FINE_AMOUNT "Issued fine amount" , SADAD_NO "SADAD NO" , FINE_PAYMENT_STATUS "Fine payment status" , SADAD_PAYMENT_DATE "SADAD PAYMENT DATE" , INSPECTOR_ACTION "Inspector_Action" , APPROVER_CONFISCATION "APPROVER CONFISCATION" , APPROVER_FOLLOWUP "APPROVER FOLLOWUP" , APPROVER_DESTROY "APPROVER DESTROY" , APPROVER_SAMPLE "APPROVER SAMPLE" , APPROVER_CLOSE "APPROVER CLOSE" , NO_LICENSE "NO LICENSE"
            FROM %s.INSPECTION_DATA""" % config.DB['input_schema']
        self.inspectionsDf = self._executeQuery(sqlQuery)
        print('Inspections data records:', self.inspectionsDf.shape)

    def getLicensesData(self):
        sqlQuery = """SELECT LICENCES_ID \"License ID (MOMRAH)\", LATITUDE \"Latitude\", LONGITUDE \"Longitude\", \
                FACILITY_TYPE \"Facility type\", BUSINESS_ACTIVITY \"Business activity\", \
                LICENSE_START_DATE \"License Start Date\", LICENSE_EXPIRY_DATE \"License Expiry Date\", \
                LAST_LICENSE_RENEWAL_DATE \"Last License renewal date\", \
                AREA_OF_THE_RETAIL_OUTLET \"Area of the retail outlet\", \
                TENANCY_OWN_OR_RENTED \"Tenancy (Own/Rented)\" \
            FROM "{}".LICENSES_DATA \
            WHERE LATITUDE NOT LIKE '%،%' AND LONGITUDE NOT LIKE '%،%'""".format(config.DB['input_schema'])
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.licensesDf = self._executeQuery(sqlQuery)
        print('License data records:', self.licensesDf.shape)


    def getLicensesKeysData(self):
        sqlQuery = "SELECT D_ACTIVITIES_ID, D_ACTIVITIES_NAME, IS_ENABLE, ACTIVITIE_TYPE_ID, \
            ACTIVITIE_TYPE_NAME, ACTIVITYNO AS \"MOMTATHEL ACTIVITY NUMBER\"\
        FROM %s.MOMTHATEL_DATA" % config.DB['input_schema']

        self.licensesKeysDf = self._executeQuery(sqlQuery)
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
        #self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data']))
        self.populationData = gdata.getPOPULATION()

    def getPopulationDataGdf2(self):
        #self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data2']))
        self.populationData = gdata.getPOPULATION()

    def getPriorityAreasData(self):
        #self.priorityAreas = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['priority_areas']))
        self.priorityAreas=gdata.getPriorityAreasData()
        self.priorityAreas = self.priorityAreas[self.priorityAreas['geometry'].notna()]
        self.priorityAreas = self._fromPdToGdf(self.priorityAreas)
        self.priorityAreas= self.priorityAreas.drop(columns=['Unnamed: 0'])


    def getCrmCases(self):
        sqlQuery = """SELECT DISTINCT PYID \"CaseId\", INTERACTIONTYPE, PXCREATEDATETIME,
            CLOSURE_DATE, SHORT_STATUS, LATITUDE \"LATITUDE\", LONGITUDE,
            MAIN_CLASSIFICATION \"MAIN_Classificaion\", SUB_CLASSIFICATION \"Sub_Classificaion\",
            SP_CLASSIFICATION \"SP_Classificaion\",
            CATEGORY, PRIORITY \"Priority_Value\", SATISFACTION \"Satisfaction\"
        FROM %s.CRM_INSPECTION_CASES
        WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL AND SHORT_STATUS <> 'Close'""" % config.DB['input_schema']
        # FETCH FIRST 2000 ROWS ONLY
            #AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME 
        self.crmData = self._executeQuery(sqlQuery)
        self.crmData.rename(columns={'latitude' : 'LATITUDE', 'longitude' : 'LONGITUDE',
            'pxcreatedatetime' : 'PXCREATEDATETIME' }, inplace = True)
        #Dropping all the missing values present in Latitude and Longitude columns of the dataset
        # self.crmData = self.crmData.dropna(subset = ['LATITUDE','LONGITUDE'])
        self.crmData = self._fromPdToGdf(self.crmData, self.crmData.LONGITUDE, self.crmData.LATITUDE)
        self.crmData.Priority_Value.replace(
            {'حرج' : 3, 'عالي': 3, 'متوسط' : 2, 'عادي' : 1, 'High' : 3, 'high' : 3,
                'medium' : 2,  'Medium' : 2,'Low' : 1, 'low' : 1, 'منخفض' : 1, 'عالي جداً' :3, 'عالٍ' : 3, 
                'متوسط' :2, 'منخفضه': 1}, 
            inplace = True
        )

        print('CRM cases records:', self.crmData.shape)
        
    def getAmanaDataGdf(self):
        #self.amanaDataGdf = geopandas.read_file(path.join(path.dirname(__file__),config.PATHS['municipality']))
        self.amanaDataGdf=gdata.getAMANA()

        # if self.amanaCode is not None:
        #     self.amanaDataGdf = self.amanaDataGdf.loc[(self.amanaDataGdf['AMANACODE'] == self.amanaCode)]

    def getAmanaPopulationOverlay(self):
        if self.populationData is None:
            self.getPopulationDataGdf()

        if self.amanaDataGdf is None:
            self.getAmanaDataGdf()

        self.amanaPopulationOverlay = geopandas.tools.sjoin(self.populationData, self.amanaDataGdf, how="inner", predicate="intersects")
        
        self.amanaPopulationOverlay.drop(labels = self.amanaPopulationOverlay.columns.difference(['GridNumber','geometry','DN']), axis = 1, inplace=True)


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

    def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)





        
