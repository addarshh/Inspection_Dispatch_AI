import numpy as np
import pandas as pd
import geopandas

from ...InspectionRBD import InspectionRBD


class GroupHousing(InspectionRBD):
    def __init__(self, data):
        super().__init__(data)
        self._inspectionDataModification()
        self._licensesDataModification()

        self.featuresColumns = [ 'AREA', 'ESTIMATED_CAPACITY',
            'Accomodation Type', 'HR_Path_Type',
            'License Active Duration days', 'Persons_PER_Room', 'Persons_PER_Toilet','LICENSE Inspection No',
            'PERCENTAGE of Beneficiary NonCompliance_last5', 'PERCENTAGE of LICENSE NonCompliance_last5','InsPostExpiry days',
            'License Prev Violation'
        ]
        self.labelsColumn = ['Violation Event']

        print('self.featuresColumns')
        print(self.featuresColumns)
      
        print('self.labelsColumn')
        print(self.labelsColumn)
    
        self.methods = {'trainTestModel' : 'trainTestModelGroupHousing', 'processModel' : 'processModelExcavation', 'getModelResults' : 'getModelResultsGroupHousing'}

    def _inspectionDataModification(self):
        Status = ['Resolved-Completed', 'Under Review and Approval', 'Resolved-Withdrawn','Resolved-NoViolations']
        
        self.data.inspectionsDf = self.data.inspectionsDf.loc[
            (self.data.inspectionsDf['INSPECTYPE ID'] == '6') 
            & (self.data.inspectionsDf['INSPECTION ID'].str.startswith('VIO') == False) 
            & (self.data.inspectionsDf['INSPECTION ID'].str.startswith('OBJ') == False)
            & self.data.inspectionsDf['Status of Work'].isin(Status)
        ].copy()

        self.data.inspectionsDf.rename(columns={'INSPECTION ID' : 'INSEPECTION ID', 'INSPECTYPE ID' : "INSPECTYPE TYPE ID",
            'compliant_clauses_number' : "Number of compliant clauses"}, inplace=True)
        
        self.data.inspectionsDf = self.data.inspectionsDf[['LICENSE NUMBER', 'INSEPECTION ID', 
            'Status of Work', 'Business Activity Weight',
            'Inspection Date',  'Number of clauses',
            'Number of compliant clauses', 'Number of non-compliant clauses', 
            'Issued fine amount', 'Fine payment status']]
        self.data.inspectionsDf.drop_duplicates(inplace=True)

        self.data.inspectionsDf['Violation Event'] = self.data.inspectionsDf['Number of non-compliant clauses'].apply(lambda x: '1' if x > 0 else '0').astype(int)


    def _licensesDataModification(self):
        #add column renaming 
        self.data.groupHousingLicenses['ISSUE_DATE_new'].fillna('0', inplace = True)
        self.data.groupHousingLicenses['ISSUE_DATE_new']  = self.data.groupHousingLicenses['ISSUE_DATE_new'].apply(lambda x:pd.to_datetime(x,format="%Y-%m-%d").date() if x!='0' else np.datetime64('nat'))
        
        ##1) License active duration
        self.data.groupHousingLicenses['License_end_date_new'].fillna('0', inplace = True)
        self.data.groupHousingLicenses['License_end_date_new']  = self.data.groupHousingLicenses['License_end_date_new'].apply(lambda x:pd.to_datetime(x) if x!='0' else None)
        self.data.groupHousingLicenses['Application Date']  = self.data.groupHousingLicenses['Application Date'].apply(lambda x:pd.to_datetime(x) if x!='0' else None)
        
        self.data.groupHousingLicenses['LicenseExpired'] = self.data.groupHousingLicenses['License_end_date_new'].apply(lambda x: 1 if (x<=pd.Timestamp('now'))&(~pd.isnull(x)) else 0)
    

    def getInspectionPostExpiryFeature(self):
        self.resultDf['InsPostExpiry days'] = self.resultDf['Inspection Date']-self.resultDf['License_end_date_new']
        self.resultDf['InsPostExpiry days'] = self.resultDf['InsPostExpiry days'].apply(lambda x: x.days if x!=0 else 0)

    def getLicenseActiveDurationfeature(self):
        print("Issue_Date_new Dataype check")
        print(self.resultDf['ISSUE_DATE_new'].dtypes)
        #print(pd.Timestamp('now').date().dtypes)
        if 'datetime' in str(self.resultDf['ISSUE_DATE_new'].dtypes):
            print("conversion time")
            self.resultDf['ISSUE_DATE_new'] = pd.to_datetime(self.resultDf['ISSUE_DATE_new'])
            print(self.resultDf['ISSUE_DATE_new'].dtypes)
            self.resultDf['License Active Duration days'] = self.resultDf['ISSUE_DATE_new'].apply(lambda x: pd.to_datetime(pd.Timestamp('now').date()) - x if x!=pd.to_datetime("1970-01-01").date() else 0)
        
        else:
            self.resultDf['License Active Duration days'] = self.resultDf['ISSUE_DATE_new'].apply(lambda x: pd.Timestamp('now') - pd.to_datetime(x) if x!=pd.to_datetime("1970-01-01").date() else 0)
        self.resultDf['License Active Duration days'] = self.resultDf['License Active Duration days'].apply(lambda x: x.days if x!=0 else 0)
        
    def getPersonsPerRoomFeature(self):
        self.resultDf['Persons_PER_Room'] = self.resultDf['ESTIMATED_CAPACITY']/self.resultDf['ROOMS_COUNT']

    def getPersonsPerToiletFeature(self):
        self.resultDf['Persons_PER_Toilet'] = self.resultDf['ESTIMATED_CAPACITY']/self.resultDf['TOILETS_COUNT']

    def getBeneficiaryNonCompliancePercentageInLast5InspectionFeature(self):
        self.resultDf.sort_values(by=['Beneficiary ID','Inspection Date'],ascending=True, inplace=True)

        self.resultDf['Beneficiary_Cumsum5'] = self.resultDf.groupby('Beneficiary ID')['Violation Event'].rolling(5,min_periods=1).sum().reset_index(0,drop=True)
        self.resultDf['Beneficiary_Cumsum5'] =self.resultDf.groupby('Beneficiary ID')['Beneficiary_Cumsum5'].shift()
        self.resultDf['Beneficiary_Cumcount5'] = self.resultDf.groupby('Beneficiary ID')['Violation Event'].rolling(5,min_periods=1).count().reset_index(0,drop=True)
        self.resultDf['Beneficiary_Cumcount5'] =self.resultDf.groupby('Beneficiary ID')['Beneficiary_Cumcount5'].shift()
        self.resultDf['PERCENTAGE of Beneficiary NonCompliance_last5'] = (self.resultDf['Beneficiary_Cumsum5']/self.resultDf['Beneficiary_Cumcount5'])

        ##Treat missing values for first inspection as separate category
        self.resultDf['PERCENTAGE of Beneficiary NonCompliance_last5'].fillna(-1,inplace=True)
        self.resultDf.drop(columns=['Beneficiary_Cumcount5', 'Beneficiary_Cumsum5'], axis=1, inplace =True)
    
    def getBeneficiaryNonCompliancePercentageInLast5InspectionArtificialFeature(self):
        df1 = self.resultDf[self.resultDf['INSPECTION ID'] != 'NEWINS-' + self.resultDf['LICENSE_ID'].astype(str)]
        df2 = self.resultDf[self.resultDf['INSPECTION ID'] == 'NEWINS-' + self.resultDf['LICENSE_ID'].astype(str)]

        df3 = df2.sort_values(by=['Violation Event'], ascending=True)
        df3.drop_duplicates(['Beneficiary ID','Inspection Date'], keep='last', inplace=True)

        df4 = pd.concat([df1, df3])
        df4.sort_values(by=['Beneficiary ID','Inspection Date'], ascending=True, inplace=True)

        df4['Beneficiary Inspection#'] = df4.groupby(['Beneficiary ID'])['Inspection Date'].cumcount()+1
        df4['Beneficiary_Cumsum5'] = df4.groupby('Beneficiary ID')['Violation Event'].rolling(5,min_periods=1).sum().reset_index(0,drop=True)
        df4['Beneficiary_Cumsum5'] =df4.groupby('Beneficiary ID')['Beneficiary_Cumsum5'].shift()
        df4['Beneficiary_Cumcount5'] = df4.groupby('Beneficiary ID')['Violation Event'].rolling(5,min_periods=1).count().reset_index(0,drop=True)
        df4['Beneficiary_Cumcount5'] =df4.groupby('Beneficiary ID')['Beneficiary_Cumcount5'].shift()
        df4['PERCENTAGE of Beneficiary NonCompliance_last5'] =  (df4['Beneficiary_Cumsum5']/df4['Beneficiary_Cumcount5'])
        df4['PERCENTAGE of Beneficiary NonCompliance_last5'].fillna(-1,inplace=True)

        LatestInspection = df4.drop_duplicates(keep='last')
        LatestInspection = LatestInspection[['Beneficiary ID','Inspection Date','Beneficiary Inspection#','Beneficiary_Cumsum5','Beneficiary_Cumcount5','PERCENTAGE of Beneficiary NonCompliance_last5']]
        LatestInspection.drop_duplicates(inplace=True)

        df2=pd.merge(df2,LatestInspection,how='left',left_on=['Beneficiary ID','Inspection Date'],right_on=['Beneficiary ID','Inspection Date'])

        self.resultDf = pd.concat([df4, df2])
        self.resultDf.drop_duplicates(inplace = True)
        self.resultDf.drop(columns=['Beneficiary_Cumcount5', 'Beneficiary_Cumsum5'], axis=1, inplace =True)

    def getLicensesNonCompliancePercentageInLast5InspectionFeature(self):
        self.resultDf.sort_values(by=['LICENSE_ID','Inspection Date'],ascending=True, inplace=True)

        self.resultDf['License_Cumsum5'] = self.resultDf.groupby('LICENSE_ID')['Violation Event'].rolling(5,min_periods=1).sum().reset_index(0,drop=True)
        self.resultDf['License_Cumsum5'] =self.resultDf.groupby('LICENSE_ID')['License_Cumsum5'].shift()
        self.resultDf['License_Cumcount5'] = self.resultDf.groupby('LICENSE_ID')['Violation Event'].rolling(5,min_periods=1).count().reset_index(0,drop=True)
        self.resultDf['License_Cumcount5'] =self.resultDf.groupby('LICENSE_ID')['License_Cumcount5'].shift()
        self.resultDf['PERCENTAGE of LICENSE NonCompliance_last5'] = (self.resultDf['License_Cumsum5']/self.resultDf['License_Cumcount5'])
        self.resultDf['PERCENTAGE of LICENSE NonCompliance_last5'].fillna(-1, inplace=True)
        self.resultDf.drop(columns=['License_Cumcount5', 'License_Cumsum5'], inplace =True, axis =1 )

    def getLicenseNumberFeature(self):
        self.resultDf['LICENSE Inspection No'] = self.resultDf.groupby('LICENSE_ID')['Inspection Date'].cumcount()+1
    
    def getLicensePreviousViolationFeature(self):
        self.resultDf['License CumSum-1'] = self.resultDf.groupby('LICENSE_ID')['Violation Event'].transform(lambda x: x.shift().cumsum())
        self.resultDf['License Prev Violation'] = self.resultDf['License CumSum-1'].apply(lambda x: 1 if x>0 else 0)

    def buildFeatureDf(self):
        self.resultDf = pd.merge(self.data.groupHousingLicenses, self.data.inspectionsDf,how='inner',left_on='LICENSE_ID',right_on='LICENSE NUMBER')

        self.getInspectionPostExpiryFeature()
        self.getLicenseActiveDurationfeature()
        self.getPersonsPerRoomFeature()
        self.getPersonsPerToiletFeature()
        self.getBeneficiaryNonCompliancePercentageInLast5InspectionFeature()
        self.getLicensesNonCompliancePercentageInLast5InspectionFeature()
        self.getLicensePreviousViolationFeature()
        self.getLicenseNumberFeature() 

        return self.resultDf[['LATITUDE', 'LONGITUDE', 'LICENSE_ID'] + self.featuresColumns + self.labelsColumn]
        
    def buildArtificialDf(self):
        self.resultDf = None

        artificial_date = pd.to_datetime("today").date()
        artificial_id = 'NEWINS-'

        licenses = self.data.groupHousingLicenses[self.data.groupHousingLicenses['LicenseExpired']==0].copy()
        
        licenses = licenses[~pd.isnull(licenses['ISSUE_DATE_new'])]
        self.resultDf = pd.merge(licenses, self.data.inspectionsDf, how='left',left_on='LICENSE_ID',right_on='LICENSE NUMBER')
        self.resultDf_gpd = geopandas.GeoDataFrame(
            self.resultDf, geometry = geopandas.points_from_xy( self.resultDf['LATITUDE'], self.resultDf['LONGITUDE'],), crs="EPSG:4326")
        Amana_Municipality = self.data.AmanaMunicipality[['AMANACODE','MUNICIPALI','geometry']]
        Amana_Municipality = Amana_Municipality.drop_duplicates()
        self.resultDf = geopandas.sjoin(self.resultDf_gpd,Amana_Municipality,how='left', predicate='intersects')
        self.resultDf = self.resultDf.drop(['index_right','geometry'],axis=1)



        self.resultDf.sort_values(['LICENSE_ID','Inspection Date'], ascending=True, inplace=True)
        new_inspection_by_license = self.resultDf.drop_duplicates('LICENSE_ID', keep='last', inplace=False)
        new_inspection_by_license['Inspection Date'] = artificial_date
        new_inspection_by_license['INSPECTION ID'] = artificial_id + new_inspection_by_license['LICENSE_ID'].astype(str)
        
        self.resultDf = pd.concat([self.resultDf, new_inspection_by_license])
        self.resultDf['Inspection Date'] = self.resultDf['Inspection Date'].apply(lambda x: pd.to_datetime(x))
        self.resultDf.dropna(subset=['Inspection Date'], inplace=True)
        self.resultDf['Violation Event'].fillna(0,inplace=True)
        self.resultDf['Violation Event'] = self.resultDf['Violation Event'].astype('int')

        self.getInspectionPostExpiryFeature()
        self.getLicenseActiveDurationfeature()
        self.getPersonsPerRoomFeature()
        self.getPersonsPerToiletFeature()

        self.getBeneficiaryNonCompliancePercentageInLast5InspectionArtificialFeature()
        self.getLicensesNonCompliancePercentageInLast5InspectionFeature()
        self.getLicensePreviousViolationFeature()
        self.getLicenseNumberFeature() 

        self.resultDf.sort_values(['LICENSE_ID','Inspection Date'], ascending=True, inplace=True)
        self.resultDf.drop_duplicates('LICENSE_ID', keep='last', inplace=True)

        return self.resultDf[['LATITUDE', 'LONGITUDE', 'LICENSE_ID', 'AMANA','AMANACODE','MUNICIPALI'] + self.featuresColumns + self.labelsColumn]