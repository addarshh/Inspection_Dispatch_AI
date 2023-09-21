import config
import geopandas
import pandas as pd
from shapely import wkt
import os.path as path
from helpers import getDateString, getStageNameDictionary

class fileDatabase():

    def __init__(self, amanaCode = None):

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

        self.excavationPhases = None
        self.drillingInspectionsDf = None
        self.drillingLicensesDf = None

        self.amanaCode = amanaCode


    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None

    def getInspectionsData(self):
        print("filedatabse HERE")
        self.inspectionsDf= pd.read_excel(path.join(path.dirname(__file__),config.PATHS['inspections']))


    def getLicensesData(self):
        self.licensesDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['licenses']))
        

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

    def insertModelStatus(self, modelName : str, startTimestamp : str, status : str) -> str:
        filename = getDateString(format="%Y-%m-%d_%H-%M-%S")
        with open(path.join(path.dirname(__file__),'..\output\logs', filename + '.txt'), 'a') as file:
            file.write("{0} \n{1} {2} \n".format(modelName, startTimestamp, status))
        return filename

    def updateModelStatus(self, recordId : str, endTimestamp : str, status : str):
        if endTimestamp is None:
            endTimestamp = getDateString()
        with open(path.join(path.dirname(__file__),'..\output\logs' , recordId + '.txt' ),  'a') as file:
            file.write("{0} {1} \n".format(endTimestamp, status))


    def saveRBDOutputLicenses(self, dataframe : pd.DataFrame, table_name : str):
        inspectionType = dataframe['INSPECTION_TYPE'][0]
        dataframe.to_csv(path.join(path.dirname(__file__),config.MODEL_SCORING_OUTPUT_FOLDER + '/rbd/' + inspectionType +'_points_for_qgis.csv'), index = False)

    
    def saveRBDOutputGrids(self, dataframe : pd.DataFrame, table_name : str):
        inspectionType = dataframe['INSPECTION_TYPE'][0]
        dataframe.to_csv(path.join(path.dirname(__file__),config.MODEL_SCORING_OUTPUT_FOLDER + '/rbd/' + inspectionType +'_qgis_grids.csv'), index = False)

    def deleteRBDOutputGrids(self, inspection, name):
        pass
    
    def deleteRBDOutputLicenses(self, inspection, name):
        pass

    def getDrillingInspections(self):
        self.drillingInspectionsDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['drilling_inspections']), dtype = {'LIC_ID': str, 'INSPECTION_DATE' : str})
        self.drillingInspectionsDf['INSPECTION_DATE']=self.drillingInspectionsDf['INSPECTION_DATE'].astype(str).str.split(' ').str[0]
        self.drillingInspectionsDf['INSPECTION_DATE']=pd.to_datetime(self.drillingInspectionsDf['INSPECTION_DATE'])
        self.drillingInspectionsDf.loc[self.drillingInspectionsDf['NUMBEROFFAILEDCLAUSES']>0, 'ISVIOLATION']=True
        self.drillingInspectionsDf['ISVIOLATION']=self.drillingInspectionsDf['ISVIOLATION'].fillna(False)
        self.drillingInspectionsDf=self.drillingInspectionsDf[self.drillingInspectionsDf['PYSTATUSWORK']
            .isin(['Pending-ReceiveSample', 'Resolved-Completed', 'Under Review and Approval'])]

        dictionary = getStageNameDictionary(self.drillingInspectionsDf['STAGENAME'].dropna().unique())
        self.drillingInspectionsDf=self.drillingInspectionsDf.replace({'STAGENAME':dictionary})
        #print()

    def getDrillingLicenses(self):
        self.drillingLicensesDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['drilling_licenses']), dtype = {'LIC_ID': str})
        # self.drillingLicensesDf['LIC_ID'] = self.drillingLicensesDf['LIC_ID'].astype(str)
        self.drillingLicensesDf.sort_values('LIC_ID', inplace=True)
        self.drillingLicensesDf.loc[self.drillingLicensesDf['REQUEST_TYPE']=='اصدار تصريح حفرية طارئة', 'EMERGENCY_LICENSE']=True
        self.drillingLicensesDf['EMERGENCY_LICENSE'].fillna(False, inplace=True)
        digging_category_dict={'الأولى':1,
                      'الثانية':2,
                      'الثالثة':3,
                      'الرابعة':4,
                      'الخامسة':5,
                      'السادسة':6,
                      'السابعة':7,
                      'الثامنة':8}
        
        self.drillingLicensesDf.replace({'DIGGING_CATEGORY':digging_category_dict}, inplace=True)
        # #print(self.drillingLicensesDf.head())

    def getExcavationPhases(self):
        self.excavationPhases = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['excavation_phases']))

    def getExcavationData(self):
        self.getDrillingInspections()
        self.getDrillingLicenses()