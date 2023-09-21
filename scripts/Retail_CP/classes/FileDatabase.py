import config
import geopandas
import pandas as pd
from shapely import wkt
import os.path as path
import re
from statistics import mean

class fileDatabase():

    def __init__(self, amanaCode = None):

        self.inspectionsWithLicensesDf : pd.DataFrame = None
        self.licensesDf : pd.DataFrame = None
        self.licensesKeysDf : pd.DataFrame = None
        self.waterDf : pd.DataFrame = None
        self.priorityAreas : pd.DataFrame = None
        self.inspectionsDf : pd.DataFrame = None
        self.classConfig : pd.DataFrame = None
        # self.regionDataGdf = None
        self.amanaDataGdf = None
        # self.poiDataGdf = None
        # self.clusteredDataGdf = None
        # self.clusterData = None
        # self.clusteredData = None
        # self.streetWidth = None
        # self.priorityAreas = None
        # self.priorityDistricts = None
        # self.crmData = None
        self.amanaPopulationOverlay = None
        self.populationData = None
        self.amanaPopulationOverlay2 = None
        self.populationData2 = None
        # self.streetlightsData = None
        # self.sewerHolesData = None
        # self.buildingsData = None
        # self.constructionLicenseData = None
        # self.pavementsData = None
        
        self.amanaCode = amanaCode


    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None

    def getInspectionsData(self):

        self.inspectionsDf= pd.read_excel(path.join(path.dirname(__file__),config.PATHS['inspections']))
        # self.inspectionsDf['INSPECTYPE ID'] = self.inspectionsDf['INSPECTYPE ID'].astype(str)
        # self.inspectionsDf['Inspection DateTime'] = self.inspectionsDf['Inspection Date']
        # self.inspectionsDf['Inspection Date'] = pd.to_datetime(self.inspectionsDf['Inspection Date'].str.split(' ').str[0])

        # self.inspectionsDf.loc[self.inspectionsDf['Degree of Compliance'].isna(), 'compliance_score_available'] = False
        # self.inspectionsDf['compliance_score_available'].fillna(True, inplace=True)
        
        # self.inspectionsDf = self.inspectionsDf[~self.inspectionsDf['LICENSE NUMBER'].isna()]
        # self.inspectionsDf = self.inspectionsDf[~self.inspectionsDf['Inspection Date'].isna()]
        # self.inspectionsDf = self.inspectionsDf[~self.inspectionsDf['Degree of Compliance'].isna()]


    def getLicensesData(self):
        # if self.licensesKeysDf is None:
        #     self.getLicensesKeysData()
        self.licensesDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['licenses']))
        # self.licensesDf.drop_duplicates('License ID (MOMRAH)', inplace=True)
        # self.licensesDf = self.licensesDf.merge(self.licensesKeysDf, how='left', left_on='Business activity', right_on=['D_ACTIVITIES_NAME']).drop_duplicates(['License ID (MOMRAH)','Business activity'])
        # self.licensesDf.drop_duplicates(['License ID (MOMRAH)','MOMTATHEL ACTIVITY NUMBER'], inplace=True)


    def getLicensesKeysData(self):
        self.licensesKeysDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['licenses_keys']))

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
        #Loading all the CRM cases and filtering for excavation barrier VP cases
        #AllCases_df = pd.read_csv(r'Z:\MOMRAH_WORKING\5. Smart Inspection\Special Classification ID to Case Prioritization codes\ExcavationBarrierCRMCases.xlsx')
        #excavation_crm = AllCases_df.loc[(AllCases_df.strVPCategory == "حواجز الحفريات")]

        self.crmData = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['all_crm']))

        #Renaming the PYID column as Case Id
        self.crmData.rename(columns = {'PYID':'CaseId'},inplace = True)
        self.crmData.drop_duplicates(inplace=True)

        #Dropping all the missing values present in Latitude and Longitude columns of the dataset
        self.crmData = self.crmData.dropna(subset = ['LATITUDE','LONGITUDE'])
        self.crmData = self._fromPdToGdf(self.crmData, self.crmData.LONGITUDE, self.crmData.LATITUDE)
        self.crmData.rename(columns={"PRIORITY": "Priority_Value"}, inplace = True)
        self.crmData.Priority_Value.replace(
            {'حرج' : 3, 'High' : 3, 'high' : 3,
            'متوسط' : 2, 'medium' : 2,  'Medium' : 2,
            'عادي' : 1, 'Low' : 1, 'low' : 1}, 
            inplace = True
        )
        
    def getAmanaDataGdf(self):
        self.amanaDataGdf = geopandas.read_file(path.join(path.dirname(__file__),config.PATHS['municipality']))

        if self.amanaCode is not None:
            self.amanaDataGdf = self.amanaDataGdf.loc[(self.amanaDataGdf['AMANACODE'] == self.amanaCode)]

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
        self.getInspectionsData()
        self.getLicensesKeysData()

