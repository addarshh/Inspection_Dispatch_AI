import numpy as np
import pandas as pd
import logging
from ...FileDatabase import fileDatabase
from ...InspectionRBD import InspectionRBD


class Excavation(InspectionRBD):
    def __init__(self, data : fileDatabase):
        super().__init__(data)
   
        self._licenseDataModification()
        self.featuresColumns = ['EMERGENCY_LICENSE','DIGGING_METHOD_ID','WORK_NATURE_ID','NETWORK_TYPE_ID','HEAVY_EQUIPMENT_PERMISSION',
            'CAMPING_ROOM_COUNT','DIGGING_CATEGORY','LENGTH_sum', 'WIDTH_min',
            'WIDTH_mean', 'WIDTH_max', 'DEPTH_min', 'DEPTH_mean', 'DEPTH_max',
            'N_PATHS_count', 'AREA_min', 'AREA_mean', 'AREA_max', 'AREA_sum',
            'VOLUME_min', 'VOLUME_mean', 'VOLUME_max', 'VOLUME_sum','license_inspection_number', 'contractor_inspection_number',
            'TIME_SINCE_WORK_START', 'TIME_UNTIL_WORK_END',
            'PERCENTAGE_OF_COMPLETION', 'contractor_last_inspection_violation',
            'contractor_last_5_inspections_violations',
            'contractor_last_15_inspections_violations',
            'contractor_n_past_violations', 'contractor_n_past_inspections',
            'contractor_percent_violations', 'license_last_inspection_violation',
            'license_last_3_inspections_violations', 'license_last_15_inspections_violations',
            'contractor_last_50_inspections_violations'
        ]

        self.labelsColumn=['ISVIOLATION']
        self.methods = {'trainTestModel' : 'trainTestModelExcavation', 'processModel' : 'processModelExcavation', 'getModelResults' : 'getModelResultsExcavation'}

  

    def _licenseDataModification(self):
        
        path_dimensions=self.data.drillingLicensesDf.drop_duplicates(['PATH_CODE','LIC_ID'], keep='first')[['LIC_ID','PATH_CODE','LENGTH','WIDTH','DEPTH']]
        path_dimensions['AREA']=path_dimensions['LENGTH']*path_dimensions['WIDTH']
        path_dimensions['VOLUME']=path_dimensions['AREA']*path_dimensions['DEPTH']
        license_dimensions = path_dimensions.groupby('LIC_ID').agg({'LENGTH':'sum',
                                                       'WIDTH':['min','mean','max'],
                                                      'DEPTH':['min','mean','max'],
                                                      'PATH_CODE':'count',
                                                      'AREA':['min','mean','max','sum'],
                                                      'VOLUME':['min','mean','max','sum']})

        license_dimensions=license_dimensions.rename(columns={'PATH_CODE':'N_PATHS'})

        license_dimensions.columns = license_dimensions.columns.map('_'.join).str.strip('_')

        license_dimensions=license_dimensions.reset_index()
        license_info = self.data.drillingLicensesDf.groupby('LIC_ID').agg({'START_POINT_X':'first',
                                                      'START_POINT_Y':'first',
                                                       'AMMANA':'first',
                                                       'Municipality':'first',
                                                       'Sub-Municipality':'first',
                                                       'DISTRICT_ID':'first',
                                                       'DISTRICT_NAME':'first'
                                                      }).reset_index()
        cols_to_remove=['LENGTH','WIDTH','DEPTH','PATH_CODE', 'START_POINT_X','START_POINT_Y',
               'AMMANA', 'Municipality','Sub-Municipality','DISTRICT_ID','DISTRICT_NAME','REQUEST_TYPE','REQ_ID','REQUEST_TYPE_ID']
        self.data.drillingLicensesDf=self.data.drillingLicensesDf.drop(cols_to_remove, axis=1, errors='ignore')

        #print(self.data.drillingLicensesDf.isna().sum())
        logging.info("Shape of drillingLicensesDf Before dropping duplicate : " + str(self.data.drillingLicensesDf.shape) )
        self.data.drillingLicensesDf.drop_duplicates(inplace=True)
        logging.info("Shape of drillingLicensesDf After dropping duplicate : " + str(self.data.drillingLicensesDf.shape) )

        #print(self.data.drillingLicensesDf[self.data.drillingLicensesDf['LIC_ID'].duplicated()])
        self.data.drillingLicensesDf=self.data.drillingLicensesDf.merge(license_dimensions, how='left',on='LIC_ID')
        logging.info("Shape of drillingLicensesDf after merging with  license_dimensions : " + str(self.data.drillingLicensesDf.shape) )
        self.data.drillingLicensesDf=self.data.drillingLicensesDf.merge(license_info, how='left',on='LIC_ID')
        logging.info("Shape of drillingLicensesDf after merging with  license_info : " + str(self.data.drillingLicensesDf.shape) )




    def buildArtificialDf(self):
        logging.info("Building Artificial DataFrame" )
        artificialDate = pd.to_datetime("today").date()
        print(self.data.drillingLicensesDf.columns)

        self.resultDf = self.data.drillingLicensesDf.merge(self.data.drillingInspectionsDf, how='left', on='LIC_ID')
        self.resultDf= self.resultDf[ self.resultDf['INSPECTION_DATE']>= self.resultDf['DIGGING_START_DATE']].copy()
        #print(self.resultDf['LIC_ID'].nunique())
       
        self.resultDf=self.resultDf[self.resultDf['INSPECTION_DATE'].dt.date < artificialDate]

        self.resultDf.sort_values(['LIC_ID','INSPECTION_DATE'], ascending=True, inplace=True)
        
        new_inspection_by_license=self.resultDf.copy().drop_duplicates('LIC_ID', keep='last')
        new_inspection_by_license['INSPECTION_DATE'] = artificialDate
        
        self.resultDf=pd.concat([self.resultDf, new_inspection_by_license])
        self.resultDf['INSPECTION_DATE']=pd.to_datetime(self.resultDf['INSPECTION_DATE'])
        #print(self.resultDf['INSPECTION_DATE'].max())

        self.resultDf.sort_values(['CONTRACTOR_CR','INSPECTION_DATE'], ascending=True, inplace=True)

        self.getContractorInspectionNumberFeature()
        self.getContractorLastInspectionIsViolationFeature()
        self.getContractorLastInspectionsViolationsFeature(5)
        self.getContractorLastInspectionsViolationsFeature(15)
        self.getContractorLastInspectionsViolationsFeature(50)
        self.getContractorPastViolationsFeature()
        self.resultDf['contractor_percent_violations']=100*self.resultDf['contractor_n_past_violations'].divide(self.resultDf['contractor_n_past_inspections'].where(self.resultDf['contractor_n_past_inspections'] != 0, np.nan))
        self.resultDf['contractor_percent_violations'].replace(np.inf, np.nan, inplace=True)
        
        self.resultDf.sort_values(['LIC_ID','INSPECTION_DATE'], ascending=True, inplace=True)
        self.getLicenseInspectionNumberFeature()
        self.getTimeSinceWorkStartFeature()
        self.getTimeUntilWorkEndFeature()
        self.getPercentageOfCompletionFeature()
        self.getLicenseLastInspectionIsViolationFeature()
        self.getLicenseLastInspectionsViolationsFeature(3)
        self.getLicenseLastInspectionsViolationsFeature(15)

        self.resultDf.sort_values('INSPECTION_DATE', ascending=True, inplace=True)
        #print(len(self.resultDf))
        self.resultDf.drop_duplicates('LIC_ID', keep='last', inplace=True)

        self.resultDf=self.resultDf[self.resultDf['DIGGING_END_DATE'] > self.resultDf['INSPECTION_DATE']]
        self.resultDf=self.resultDf[self.resultDf['DIGGING_START_DATE']<=self.resultDf['INSPECTION_DATE']]

        self.resultDf['TIME_SINCE_WORK_START']=self.resultDf['TIME_SINCE_WORK_START'].dt.days#.fillna(9999)
        self.resultDf['TIME_UNTIL_WORK_END']=self.resultDf['TIME_UNTIL_WORK_END'].dt.days#.fillna(9999)
        self.resultDf['contractor_n_past_violations'].replace({'False':0, 'True':1})

        #print(self.resultDf['LIC_ID'].nunique())
        #print(self.resultDf.isna().sum())
        logging.info("Shape of  Artificial DataFrame after dropping duplicates based on 'LIC_ID' : " + str(self.resultDf.shape) )
        return self.resultDf


    
    def getContractorInspectionNumberFeature(self):
        self.resultDf['contractor_n_past_inspections']=self.resultDf.groupby('CONTRACTOR_CR').cumcount()
        self.resultDf['contractor_inspection_number']=self.resultDf['contractor_n_past_inspections']+1
        logging.info("contractor_inspection_number : " + str(self.resultDf['contractor_inspection_number']) )

    def getContractorLastInspectionIsViolationFeature(self):
        self.resultDf['contractor_last_inspection_violation']=self.resultDf.groupby('CONTRACTOR_CR')['ISVIOLATION'].shift()
    
    def getContractorLastInspectionsViolationsFeature(self, n : int):
        featureName = 'contractor_last_' + str(n) + '_inspections_violations'
        self.resultDf[featureName]=self.resultDf.groupby('CONTRACTOR_CR')['ISVIOLATION'].rolling(n, min_periods=1).mean().reset_index(0,drop=True)
        self.resultDf[featureName]=self.resultDf.groupby('CONTRACTOR_CR')[featureName].shift()

    def getContractorPastViolationsFeature(self):
        self.resultDf['contractor_n_past_violations']=self.resultDf.groupby('CONTRACTOR_CR')['ISVIOLATION'].transform(lambda x: x.cumsum().shift())

    def getContractorPercentViolationsFeature(self):
        self.resultDf['contractor_percent_violations']=100*self.resultDf['contractor_n_past_violations']/self.resultDf['contractor_n_past_inspections']
        self.resultDf['contractor_percent_violations'].replace(np.inf, np.nan, inplace=True)
    
    def getLicenseInspectionNumberFeature(self):
        self.resultDf['license_inspection_number']=self.resultDf.groupby('LIC_ID').cumcount()
        self.resultDf['license_inspection_number']=self.resultDf['license_inspection_number']+1

    def getTimeSinceWorkStartFeature(self):
        self.resultDf['TIME_SINCE_WORK_START']=self.resultDf['INSPECTION_DATE'] - self.resultDf['DIGGING_START_DATE']

    def getTimeUntilWorkEndFeature(self):
        self.resultDf['TIME_UNTIL_WORK_END']=self.resultDf['DIGGING_END_DATE'] - self.resultDf['INSPECTION_DATE']
    
    def getPercentageOfCompletionFeature(self):
        self.resultDf['PERCENTAGE_OF_COMPLETION']=self.resultDf['TIME_SINCE_WORK_START']/(self.resultDf['TIME_SINCE_WORK_START']+self.resultDf['TIME_UNTIL_WORK_END'])

    def getLicenseLastInspectionIsViolationFeature(self):
        self.resultDf['license_last_inspection_violation']=self.resultDf.groupby('LIC_ID')['ISVIOLATION'].shift() 

    def getLicenseLastInspectionsViolationsFeature(self, n : int):
            featureName = 'license_last_' + str(n) + '_inspections_violations'
            self.resultDf[featureName]=self.resultDf.groupby('LIC_ID')['ISVIOLATION'].rolling(n, min_periods=1).mean().reset_index(0,drop=True)
            self.resultDf[featureName]=self.resultDf.groupby('LIC_ID')[featureName].shift()

    def buildFeatureDf(self):
        self.resultDf = self.data.drillingLicensesDf.merge(self.data.drillingInspectionsDf, how='inner', on='LIC_ID')
        self.resultDf=self.resultDf[self.resultDf['INSPECTION_DATE']>=self.resultDf['DIGGING_START_DATE']].copy()
        self.resultDf.sort_values(['CONTRACTOR_CR','INSPECTION_DATE'], ascending=True, inplace=True)

        self.getContractorInspectionNumberFeature()
        self.getContractorLastInspectionIsViolationFeature()
        self.getContractorLastInspectionsViolationsFeature(5)
        self.getContractorLastInspectionsViolationsFeature(15)
        self.getContractorLastInspectionsViolationsFeature(50)
        self.getContractorPastViolationsFeature()
        self.getContractorPercentViolationsFeature()
        
        self.resultDf.sort_values(['LIC_ID','INSPECTION_DATE'], ascending=True, inplace=True)
        self.getLicenseInspectionNumberFeature()
        self.getTimeSinceWorkStartFeature()
        self.getTimeUntilWorkEndFeature()
        self.getPercentageOfCompletionFeature()
        self.getLicenseLastInspectionIsViolationFeature()
        self.getLicenseLastInspectionsViolationsFeature(3)
        self.getLicenseLastInspectionsViolationsFeature(15)

        self.resultDf.sort_values('INSPECTION_DATE', ascending=True, inplace=True)

        self.resultDf.set_index('LIC_ID', inplace=True)
        self.resultDf = self.resultDf[['INSPECTION_ID'] + self.featuresColumns + self.labelsColumn]
        self.resultDf['TIME_SINCE_WORK_START']=self.resultDf['TIME_SINCE_WORK_START'].dt.days#.fillna(9999)
        self.resultDf['TIME_UNTIL_WORK_END']=self.resultDf['TIME_UNTIL_WORK_END'].dt.days#.fillna(9999)

        self.resultDf[['contractor_last_inspection_violation', 'license_last_inspection_violation']]=self.resultDf[['contractor_last_inspection_violation', 'license_last_inspection_violation']].apply(pd.to_numeric)
        
        return self.resultDf