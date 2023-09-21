import numpy as np
import pandas as pd

from ...FileDatabase import fileDatabase
from ...InspectionRBD import InspectionRBD


class Construction(InspectionRBD):
    def __init__(self, data : fileDatabase):
        super().__init__(data)
        self.inspectionConstructionFloorsDf = None
        self.floorsCountGroupDf = None
        self.getFloorsCountGroupDf()

        self.featuresColumns = ['Log_Parcel_Area', 'LicenseDuration','Building height', 'Prev_Consultant license id_AvgViolationRate', 'Prev_Consultant license id_AvgFailedClauses','Prev_Contractor license ID_AvgViolationRate', 'Prev_Contractor license ID_AvgFailedClauses', 'Prev_Amana Name English_AvgViolationRate',
            'Prev_Amana Name English_AvgFailedClauses', 'Prev_BUILDING TYPE_eng_AvgViolationRate',
            'Prev_BUILDING TYPE_eng_AvgFailedClauses', 'Prev_Building_Tag_AvgViolationRate',
            'Prev_Building_Tag_AvgFailedClauses', 'Prev_Floor_Type_english_AvgViolationRatemean',
            'Prev_Floor_Type_english_AvgViolationRatesum', 'Prev_Floor_Type_english_AvgFailedClausesmean',
            'Prev_Floor_Type_english_AvgFailedClausessum','Prev_component usage_english_AvgViolationRatemean', 'Prev_component usage_english_AvgViolationRatesum',
            'Prev_component usage_english_AvgFailedClausesmean', 'Prev_component usage_english_AvgFailedClausessum','Setbackmean',#'Setbackmax', 'Setbackmin', 
            'Reboundmean','LIC_IDcount', 'floor countsum','License idcount'
        ]



        self.labelsColumn=['Violated']
        self.methods = {'trainTestModel' : 'trainTestModelConstruction', 'processModel' : 'processModel', 'getModelResults' : 'getModelResultsConstruction'}


    def buildArtificialDf(self):
        self.resultDf = None
        if self.inspectionConstructionFloorsDf is None:
            self.inspectionConstructionFloorsDf = self.getInspectionsConstructionFloorsDf()
            self.resultDf = None
        self.data.constructionInspection.sort_values(['LIC_ID', 'INSPECTION_DATE'], ascending=[True, True], inplace=True)
        inspections = pd.merge(left= self.data.constructionInspection, right = self.data.constructionLicenses , on='License ID', how='outer')
        inspectionsArtificialDf = inspections.drop_duplicates(subset='License ID', keep='first').copy()
        inspectionsArtificialDf['INSPECTION_DATE'] = pd.to_datetime('today')
        #print(inspections["NUMBEROFFAILEDCLAUSES"].sum())
        #print(inspectionsArtificialDf["NUMBEROFFAILEDCLAUSES"].sum())
        self.resultDf = pd.concat([inspections, inspectionsArtificialDf], ignore_index=True)
        #print(self.resultDf.shape)
        self.resultDf ['ISSUE_DATE'] = pd.to_datetime(self.resultDf ['ISSUE_DATE'])
        self.resultDf ['EXPIRATION_DATE'] = pd.to_datetime(self.resultDf ['EXPIRATION_DATE'])

        self.getLogParcelAreaFeature()
        self.getBuildingTagFeature()

        self.resultDf.columns = self.resultDf.columns.map(''.join)

        self.getIsViolatedFeature()
        

        for column in ['Consultant license id','Contractor license ID', 'Amana Name English',
            'BUILDING TYPE_eng', 'Building_Tag']:
            #print(column)
                
            self.resultDf.sort_values([column, 'INSPECTION_DATE'], ascending=[True, True], inplace=True)
            self.getInspectionCountFeatureGroupedBy(column)
            self.getAverageFeature('_AvgViolationRate', 'Violated', column)
            self.getPreviousFeature(column, '_AvgViolationRate')
            self.getAverageFeature('_AvgFailedClauses', 'NUMBEROFFAILEDCLAUSES',column)
            self.getPreviousFeature(column, '_AvgFailedClauses')

        self.resultDf.reset_index(drop=True)
        self.resultDf['LicenseDuration'] = (self.resultDf['EXPIRATION_DATE'] - self.resultDf['ISSUE_DATE'] )/ np.timedelta64(1,'D')
        
        self.resultDf = pd.merge(left=self.resultDf, right = self.data.ConstructionSetbackReboundDf, on='License ID', how='left')

        Component_Usage_final_grouped_df = self.inspectionConstructionFloorsDf.groupby(['component usage id']).agg({
            'Prev_component usage_english_AvgViolationRate' : [ 'mean','count','sum'],
            'Prev_component usage_english_AvgFailedClauses' : [ 'mean','count','sum']}).reset_index()
        Component_Usage_final_grouped_df.columns = Component_Usage_final_grouped_df.columns.map(''.join)
        All_Construction_floors_componentusage_df = pd.merge(left=self.inspectionConstructionFloorsDf, right = 
                   Component_Usage_final_grouped_df, on='component usage id', how='left')
        All_Construction_floors_grouped_componentusage_df = All_Construction_floors_componentusage_df.groupby(['License ID']).agg({
            'Prev_component usage_english_AvgViolationRatemean' : [ 'mean'],
            'Prev_component usage_english_AvgViolationRatesum': [ 'sum'],
            'Prev_component usage_english_AvgFailedClausesmean': [ 'mean'],
            'Prev_component usage_english_AvgFailedClausessum': [ 'sum']   
        }).reset_index()
        All_Construction_floors_grouped_componentusage_df.columns = All_Construction_floors_grouped_componentusage_df.columns.map(''.join)

        Construction_floors_final_grouped_df = self.inspectionConstructionFloorsDf.groupby(['Floor_Type_english']).agg({
            'Prev_Floor_Type_english_AvgViolationRate' : [ 'mean','count','sum'],
            'Prev_Floor_Type_english_AvgFailedClauses' : [ 'mean','count','sum']}).reset_index()
        Construction_floors_final_grouped_df.columns = Construction_floors_final_grouped_df.columns.map(''.join)
        All_Construction_floors_floortype_df = pd.merge(left=self.inspectionConstructionFloorsDf, right = 
                   Construction_floors_final_grouped_df, on='Floor_Type_english', how='left')
        All_Construction_floors_grouped_floortype_df =  All_Construction_floors_floortype_df.groupby(['License ID']).agg({
            'Prev_Floor_Type_english_AvgViolationRatemean': [ 'mean'],
            'Prev_Floor_Type_english_AvgViolationRatesum': [ 'sum'],
            'Prev_Floor_Type_english_AvgFailedClausesmean': [ 'mean'],
            'Prev_Floor_Type_english_AvgFailedClausessum': [ 'sum']   
        }).reset_index()
        All_Construction_floors_grouped_floortype_df.columns = All_Construction_floors_grouped_floortype_df.columns.map(''.join)

        self.resultDf = pd.merge(left=self.resultDf, right = 
                   All_Construction_floors_grouped_componentusage_df, on='License ID', how='left')
        self.resultDf = pd.merge(left=self.resultDf, right = 
                   All_Construction_floors_grouped_floortype_df, on='License ID', how='left')
        self.resultDf.columns = self.resultDf.columns.map(''.join)
        self.resultDf = pd.merge(left=self.resultDf, right = self.floorsCountGroupDf, on='License ID', how='left')
        
        self.resultDf.sort_values(['License ID', 'INSPECTION_DATE'], ascending=[True, True], inplace=True)
        self.resultDf['count'] = self.resultDf.groupby('License ID').cumcount() + 1
        df_counts = self.resultDf.groupby(['License ID'])['count'].max()
        self.resultDf = pd.merge(left=self.resultDf, right = df_counts, on='License ID')
        self.resultDf =  self.resultDf[ self.resultDf['count_x'] ==  self.resultDf['count_y']]
        self.resultDf['maxcount'] = self.resultDf.groupby('License ID').cumcount() + 1
        self.resultDf['floor countsum'] = self.resultDf['floor countsum'].apply(lambda x: 1 if x < 1 else x)
        self.featuresColumns = [
            # 'Amana', 'Municipality', 'Sub-Municipality', 'ISSUE_DATE',
            # 'EXPIRATION_DATE', 'lat', 'long', 'consultant name',
            # 'Consultant license id', 'BUILDING TYPE ID',
            # 'Building main use id', 'License ID',
            # 'Building sub use id', 'Parcel area', 'Contractor',
            # 'Contractor license ID', 'owner_id', 'owner_name',
            # 'BUILDING TYPE_eng', 'Building main use_eng',
            # 'Building Sub use_eng', 'Amana Name English',
            # 'Building_Tag',
            'Log_Parcel_Area',
            'LicenseDuration','Building height', 
            # 'Violated',
            'Prev_Consultant license id_AvgViolationRate',
            'Prev_Consultant license id_AvgFailedClauses',
            'Prev_Contractor license ID_AvgViolationRate',
            'Prev_Contractor license ID_AvgFailedClauses',
            'Prev_Amana Name English_AvgViolationRate',
            'Prev_Amana Name English_AvgFailedClauses',
            'Prev_BUILDING TYPE_eng_AvgViolationRate',
            'Prev_BUILDING TYPE_eng_AvgFailedClauses',
            'Prev_Building_Tag_AvgViolationRate',
            'Prev_Building_Tag_AvgFailedClauses', 
            'Prev_Floor_Type_english_AvgViolationRatemeanmean',
            'Prev_Floor_Type_english_AvgViolationRatesumsum',
            'Prev_Floor_Type_english_AvgFailedClausesmeanmean',
            'Prev_Floor_Type_english_AvgFailedClausessumsum',
            'Prev_component usage_english_AvgViolationRatemeanmean',
            'Prev_component usage_english_AvgViolationRatesumsum',
            'Prev_component usage_english_AvgFailedClausesmeanmean',
            'Prev_component usage_english_AvgFailedClausessumsum',
            'Setbackmean', 'Reboundmean','LIC_IDcount','floor countsum','License idcount'
        ]
        
        self.data.licensesDf = self.resultDf[['Amana', 'Violated', 'Municipality', 'Sub-Municipality', 'ISSUE_DATE',
            'EXPIRATION_DATE', 'lat', 'long', 'consultant name','Violated',
            'Consultant license id', 'BUILDING TYPE ID',
            'Building main use id', 'License ID',
            'Building sub use id', 'Parcel area', 'Contractor',
            'Contractor license ID', 'owner_id', 'owner_name',
            'BUILDING TYPE_eng', 'Building main use_eng',
            'Building Sub use_eng', 'Amana Name English',
            'Building_Tag']].copy()
        self.resultDf = self.resultDf[self.featuresColumns]
        self.resultDf['Building height'] = self.resultDf['Building height'].astype('float')
        self.resultDf.loc[self.resultDf['Building height'] > 100, 'Building height'] = np.nan

        self.resultDf.replace({pd.NA: np.nan}, inplace=True)
        self.resultDf = self.resultDf.apply(pd.to_numeric)


        return self.resultDf


        

    def buildFeatureDf(self):
        if self.inspectionConstructionFloorsDf is None:
            self.inspectionConstructionFloorsDf = self.getInspectionsConstructionFloorsDf()
            self.resultDf = None


        Component_Usage_final_grouped_df = self.inspectionConstructionFloorsDf.groupby(['License ID']).agg({
            'Prev_component usage_english_AvgViolationRate' : [ 'mean','count','sum'],
            'Prev_component usage_english_AvgFailedClauses' : [ 'mean','count','sum']}).reset_index()
        Component_Usage_final_grouped_df.columns = Component_Usage_final_grouped_df.columns.map(''.join)

        Construction_floors_final_grouped_df = self.inspectionConstructionFloorsDf.groupby(['License ID']).agg({
            'Prev_Floor_Type_english_AvgViolationRate' : [ 'mean','count','sum'],
            'Prev_Floor_Type_english_AvgFailedClauses' : [ 'mean','count','sum']}).reset_index()
        Construction_floors_final_grouped_df.columns = Construction_floors_final_grouped_df.columns.map(''.join)

        self.resultDf = pd.merge(left=self.data.constructionInspection, right = self.data.constructionLicenses, on='License ID', how='left')

        self.resultDf.drop(self.resultDf[self.resultDf["NUMBEROFFAILEDCLAUSES"].isna()].index, inplace=True)
        self.resultDf['ISSUE_DATE'] = pd.to_datetime(self.resultDf['ISSUE_DATE'])
        self.resultDf['EXPIRATION_DATE'] = pd.to_datetime(self.resultDf['EXPIRATION_DATE'])

        self.getLogParcelAreaFeature()
        self.getBuildingTagFeature()
        # self.resultDf['Building_Tag'] = self.resultDf['BUILDING TYPE_eng'].fillna('') + '-' + self.resultDf['Building Sub use_eng'].fillna('')

        self.resultDf.columns = self.resultDf.columns.map(''.join)

        self.getIsViolatedFeature()

        for column in ['Consultant license id','Contractor license ID', 'Amana Name English',
            'BUILDING TYPE_eng', 'Building_Tag']:
                
            self.resultDf.sort_values([column, 'INSPECTION_DATE'], ascending=[True, True], inplace=True)
            self.getInspectionCountFeatureGroupedBy(column)
            self.getAverageFeature('_AvgViolationRate', 'Violated', column)
            self.getPreviousFeature(column, '_AvgViolationRate')
            self.getAverageFeature('_AvgFailedClauses', 'NUMBEROFFAILEDCLAUSES',column)
            self.getPreviousFeature(column, '_AvgFailedClauses')

        self.resultDf.reset_index(drop=True)
        self.resultDf['LicenseDuration'] = (self.resultDf['EXPIRATION_DATE'] - self.resultDf['ISSUE_DATE'] )/ np.timedelta64(1,'D')

        self.resultDf = pd.merge(left=self.resultDf, right = 
            Construction_floors_final_grouped_df, on='License ID', how='left')
        self.resultDf = pd.merge(left=self.resultDf, right = 
            Component_Usage_final_grouped_df, on='License ID', how='left')
        self.resultDf = pd.merge(left=self.resultDf, right = 
            self.data.ConstructionSetbackReboundDf, on='License ID', how='left')

        self.resultDf = pd.merge(left=self.resultDf, right = self.floorsCountGroupDf, on='License ID', how='left')

        self.resultDf.sort_values(['License ID', 'INSPECTION_DATE'], ascending=[True, True], inplace=True)
        self.resultDf['count'] = self.resultDf.groupby('License ID').cumcount() + 1
        df_counts = self.resultDf.groupby(['License ID'])['count'].max()
        self.resultDf = pd.merge(left=self.resultDf, right = df_counts, on='License ID')
        self.resultDf =  self.resultDf[ self.resultDf['count_x'] ==  self.resultDf['count_y']]
        self.resultDf['maxcount'] = self.resultDf.groupby('License ID').cumcount() + 1
        self.resultDf['floor countsum'] = self.resultDf['floor countsum'].apply(lambda x: 1 if x < 1 else x)

        self.resultDf = self.resultDf[self.featuresColumns + self.labelsColumn]

        self.resultDf['Building height'] = self.resultDf['Building height'].astype('float')
        self.resultDf.loc[self.resultDf['Building height'] > 100, 'Building height'] = np.nan

        self.resultDf.replace([np.inf, -np.inf], np.nan, inplace=True)

        return self.resultDf

    def getLogParcelAreaFeature(self):
        self.resultDf['Parcel area'] = self.resultDf['Parcel area'].astype('float64')
        self.resultDf['Parcel area'] = self.resultDf['Parcel area'].apply(lambda x: 10 if x < 10 else x)
        self.resultDf['Log_Parcel_Area'] = round(np.log10(self.resultDf['Parcel area']),2)
        self.resultDf['Log_Parcel_Area'] = self.resultDf['Log_Parcel_Area'].fillna(self.resultDf['Log_Parcel_Area'].mean())

    def getBuildingTagFeature(self):
        self.resultDf['Building_Tag'] = self.resultDf['BUILDING TYPE_eng'].fillna('') + '-' + self.resultDf['Building Sub use_eng'].fillna('')

    def getIsViolatedFeature(self):
        self.resultDf['Violated'] = self.resultDf['NUMBEROFFAILEDCLAUSES'].apply(lambda x : 1 if x>0.0 else 0 )

    def getInspectionCountFeatureGroupedBy(self, groupBy : str):
        self.resultDf[groupBy + '_InspectionCount'] = self.resultDf.groupby(groupBy).cumcount() + 1
        self.resultDf.loc[self.resultDf[groupBy].isna(),groupBy + '_InspectionCount']=1
        #print(self.resultDf[groupBy + '_InspectionCount'].sum())

    def getAverageFeature(self, name : str, column : str, groupBy : str):
        self.resultDf[groupBy + name] = self.resultDf.groupby(groupBy)[column].cumsum() / self.resultDf[groupBy + '_InspectionCount']
        #print(self.resultDf[groupBy + name].sum())

    def getPreviousFeature(self, column : str, name : str):
        self.resultDf['Prev_' + column + name] = self.resultDf.loc[self.resultDf[column + '_InspectionCount'].shift(-1)>1, column + name]
        self.resultDf['Prev_' + column + name] = self.resultDf['Prev_' + column + name].shift()
        self.resultDf.loc[self.resultDf[column].isna(),'Prev_' + column + name]=-1


    def getInspectionsConstructionFloorsDf(self):
        self.resultDf = self.data.constructionInspectionFloor.copy()
        self.getIsViolatedFeature()

        for column in ['component usage_english', 'Floor_Type_english']:
                
            self.resultDf.sort_values([column, 'INSPECTION_DATE'], ascending=[True, True], inplace=True)
            self.getInspectionCountFeatureGroupedBy(column)
            self.getAverageFeature('_AvgViolationRate', 'Violated', column)
            self.getPreviousFeature(column, '_AvgViolationRate')
            self.getAverageFeature('_AvgFailedClauses', 'NUMBEROFFAILEDCLAUSES',column)
            self.getPreviousFeature(column, '_AvgFailedClauses')

        return self.resultDf

    def getFloorsCountGroupDf(self):
        floors_count_grouped_df =  self.data.constructionInspectionFloor.groupby(['LIC_ID']).agg({
            'LIC_ID' :['count'],
            'License id' : ['count'],
            'floor count' : ['sum']}).reset_index()
        floors_count_grouped_df.columns = floors_count_grouped_df.columns.map(''.join)
        floors_count_grouped_df['License ID'] = floors_count_grouped_df['LIC_ID']
        self.floorsCountGroupDf = floors_count_grouped_df