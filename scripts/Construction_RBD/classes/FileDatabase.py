import config
import geopandas
import pandas as pd
import numpy as np
from shapely import wkt
import os.path as path
from helpers import getDateString, getStageNameDictionary, generatePOIFile
from classes import Database as Data
DB = Data.Database()
import logging


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
        self.poiData = None

        self.excavationPhases = None
        self.drillingInspectionsDf = None
        self.drillingLicensesDf = None

        self.constructionInspectionFloor = None
        self.constructionLicenses = None
        self.constructionInspection = None
        self.ConstructionSetbackReboundDf = None

        #self.Construction_floors_df : pd.DataFrame = None

        self.amanaCode = amanaCode


    def wkt_loads(self, x):
        try:
            return wkt.loads(x)
        except Exception:
            return None

    def getInspectionsData(self):
        self.inspectionsDf= pd.read_excel(path.join(path.dirname(__file__),config.PATHS['inspections']))


    def getLicensesData(self):
        if self.licensesDf is None:
            self.licensesDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['licenses']))
        

    def getLicensesKeysData(self):
        self.licensesKeysDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['licenses_keys']))

    def getWaterData(self):
        self.waterDf = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['water_data']))
        logging.info("getWaterData shape is :"+str(self.waterDf.shape))
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
        logging.info("PopulationDataGdf shape is :" + str(self.populationData.shape))
    def getPopulationDataGdf2(self):
        self.populationData = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data2']))
        logging.info("PopulationDataGdf2 shape is :" + str(self.populationData.shape))
    def getPriorityAreasData(self):
        self.priorityAreas = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['priority_areas']))
        self.priorityAreas = self.priorityAreas[self.priorityAreas['geometry'].notna()]
        self.priorityAreas = self._fromPdToGdf(self.priorityAreas)
        self.priorityAreas= self.priorityAreas.drop(columns=['Unnamed: 0'])
        logging.info("priorityAreas shape is :" + str(self.priorityAreas.shape))

    def getCrmCases(self):
        #Loading all the CRM cases and filtering for excavation barrier VP cases
        #AllCases_df = pd.read_csv(r'Z:\MOMRAH_WORKING\5. Smart Inspection\Special Classification ID to Case Prioritization codes\ExcavationBarrierCRMCases.xlsx')
        #excavation_crm = AllCases_df.loc[(AllCases_df.strVPCategory == "حواجز الحفريات")]

        self.crmData = pd.read_excel(path.join(path.dirname(__file__),config.PATHS['all_crm']))
        logging.info("crmData shape is :" + str(self.crmData.shape))
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
        logging.info("amanaDataGdf shape is :" + str(self.amanaDataGdf.shape))
        if self.amanaCode is not None:
            self.amanaDataGdf = self.amanaDataGdf.loc[(self.amanaDataGdf['AMANACODE'] == self.amanaCode)]

    def getAmanaPopulationOverlay(self):
        if self.amanaPopulationOverlay is None:
            self.amanaPopulationOverlay = geopandas.read_file(path.join(path.dirname(__file__), config.PATHS['population_data3']))
        logging.info("amanaPopulationOverlay shape is :" + str(self.amanaPopulationOverlay.shape))
        if self.amanaCode is not None:
            self.amanaPopulationOverlay= self.amanaPopulationOverlay.loc[(self.amanaPopulationOverlay['AMANACODE'].str.startswith(self.amanaCode))]
        # if self.populationData is None:
        #     self.getPopulationDataGdf()

        # if self.amanaDataGdf is None:
        #     self.getAmanaDataGdf()

        # self.amanaPopulationOverlay = geopandas.tools.sjoin(self.populationData, self.amanaDataGdf, how="inner", predicate="intersects")
        
        # self.amanaPopulationOverlay.drop(labels = self.amanaPopulationOverlay.columns.difference(['GridNumber','geometry','DN']), axis = 1, inplace=True)


    def getAmanaPopulationOverlay2(self):
        if self.populationData2 is None:
            self.getPopulationDataGdf2()

        if self.amanaDataGdf is None:
            self.getAmanaDataGdf()

        self.amanaPopulationOverlay2 = geopandas.tools.sjoin(self.populationData, self.amanaDataGdf, how="inner", predicate="intersects")
        logging.info("amanaPopulationOverlay2 shape is :" + str(self.amanaPopulationOverlay2.shape))
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
        logging.info("classConfig shape is :" + str(self.classConfig.shape))
    def getCommonData(self):
        self.getLicensesData()
        self.getInspectionsData()
        self.getLicensesKeysData()

    def insertModelStatus(self, modelName : str, startTimestamp : str, status : str) -> str:
        filename = getDateString(format="%Y-%m-%d_%H-%M-%S")
        # with open(path.join(path.dirname(__file__), 'output', 'logs', filename + '.txt'), 'a') as file:
        #     file.write("{0} \n{1} {2} \n".format(modelName, startTimestamp, status))
        return filename

    def updateModelStatus(self, recordId : str, endTimestamp : str, status : str):
        # if endTimestamp is None:
            endTimestamp = getDateString()
        # with open(path.join(path.dirname(__file__), 'output', 'logs' , recordId + '.txt' ),  'a') as file:
        #     file.write("{0} {1} \n".format(endTimestamp, status))

    def saveRBDOutput(self, dataframe : pd.DataFrame, table_name : str):
        dataframe.to_csv(path.join(path.dirname(__file__),config.MODEL_SCORING_OUTPUT_FOLDER, 'rbd', table_name +'.csv'), index = False)


    def deleteRBDOutputGrids(self, inspection, name):
        pass
    
    def deleteFromTable(self, inspection):
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

    def getPoiDataGdf(self):
        file_exists = path.exists(path.join(path.dirname(__file__), config.FEATURE_CREATION_INPUT_FOLDER ,  "pois\pois_licenses_comparison.csv"))

        if file_exists is False:
            self.getAmanaPopulationOverlay()
            self.getLicensesData()
            pois1=pd.read_excel(path.join(path.dirname(__file__),config.PATHS['pois1']))
            pois2=pd.read_excel(path.join(path.dirname(__file__),config.PATHS['pois2']))
        
            self.poiData = generatePOIFile(self.amanaPopulationOverlay, self.licensesDf, pois1, pois2)
        else:
            self.poiData = pd.read_csv(path.join(path.dirname(__file__), config.FEATURE_CREATION_INPUT_FOLDER ,  "pois", "pois_licenses_comparison.csv"))
            logging.info("poiData shape is :" + str(self.poiData.shape))
            self.poiData.drop(columns=['Unnamed: 0'], inplace=True)

    def getConstructionLicenses(self):
        self.constructionLicenses=DB.getConstructionLicences()
        #self.constructionLicenses = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_licenses']), thousands=r',', low_memory=False)
        # self.constructionLicenses.drop(columns=['1_Project_start-up_report_date',
        #     '2_The_date_of_the_report_prior_to_pouring_the_foundations',
        #     '3_Report_date_after_foundation_casting',
        #     '4_Date_of_pre-molding_report', '5_Report_date_after_casting_medets',
        #     '6_Report_date_before_casting_columns',
        #     '7_Report_date_after_pouring_the_roof_of_each_role',
        #     '8_Report_date_after_casting_each_turn',
        #     '9_The_date_of_the_end_of_the_bone_and_the_beginning_of_the_electromechanical_stage_report',
        #     '10_The_date_of_the_end-of-destinations_stage_report_and_the_beginning_of_isolation_work',
        #     '11_Electromechanical_Extension_End_Report_Date',
        #     '12_Date_of_the_tiling_and_window_installation_stage_report',
        #     '13_Date_of_the_final_inspection_stage_report',
        #     '14_Date_of_issuance_of_the_work_certificate'], inplace=True)
        buildingInfo = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['building_type']), thousands=r',')
        self.constructionLicenses['BUILDING TYPE ID']=pd.to_numeric(self.constructionLicenses['BUILDING TYPE ID'], errors='coerce')
        self.constructionLicenses['Building main use id'] = pd.to_numeric(self.constructionLicenses['Building main use id'],errors='coerce')
        self.constructionLicenses['Building sub use id'] = pd.to_numeric(self.constructionLicenses['Building sub use id'], errors='coerce')

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
        #self.constructionInspection = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_inspections']), thousands=r',', parse_dates=['INSPECTION_DATE'], low_memory=False)
        self.constructionInspection=DB.getConstructionInspections()
        #self.constructionInspection['License ID']=self.constructionInspection['LIC_ID']
        self.constructionInspection.rename(columns={'lic_id' : 'License ID'}, inplace=True)

    def getConstsructionInspectionsFloors(self):
        if self.constructionInspection is None:
            self.getConstsructionInspections()
        Construction_floors_df=DB.getfromDBConstruction_floors()
        FloorTypeEngMap_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['floor_type']))
        component_usage_eng_map_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['component_usage']))

        Construction_floors_df = pd.merge(left=Construction_floors_df, right = FloorTypeEngMap_df, on='Floor type id', how='left')
        Construction_floors_df = pd.merge(left=Construction_floors_df, right = component_usage_eng_map_df, on='component usage id', how='left')
        Construction_floors_df.rename(columns = {'License id': 'License ID'}, inplace=True)
        #self.constructionInspection['License ID'] = self.constructionInspection['LICENSE ID']
        self.constructionInspection.rename(columns = {'LIC_ID': 'License ID'}, inplace=True)
        # print(self.constructionInspection.columns)
        # print(Construction_floors_df.columns)
        self.constructionInspectionFloor = pd.merge(left=self.constructionInspection, right = Construction_floors_df, on='License ID', how='left')
        self.constructionInspectionFloor.loc[self.constructionInspectionFloor['floor count'].isna(),'floor count']=0
        self.constructionInspectionFloor['floor count'] = self.constructionInspectionFloor['floor count'].astype(np.int64)


        
    def getConstructionSetbackRebound(self):
        # Construction_SetbackRebound1_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_setback_rebound1']), thousands=r',')
        # Construction_SetbackRebound2_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_setback_rebound2']), thousands=r',')
        # Construction_SetbackRebound3_df = pd.read_csv(path.join(path.dirname(__file__),config.PATHS['construction_setback_rebound3']), thousands=r',')
        #
        # self.ConstructionSetbackReboundDf = pd.concat([Construction_SetbackRebound1_df,Construction_SetbackRebound2_df,Construction_SetbackRebound3_df])
        self.ConstructionSetbackReboundDf=DB.getConstructionSetbackRebound()
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