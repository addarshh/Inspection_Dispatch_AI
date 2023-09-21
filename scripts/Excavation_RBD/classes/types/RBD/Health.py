import pandas as pd

from ...FileDatabase import fileDatabase
from ...InspectionRBD import InspectionRBD


class Health(InspectionRBD):
    def __init__(self, data : fileDatabase):
        super().__init__(data)
        # self.retailLicenseData : pd.DataFrame = None
        self._inspectionDataModification()
        self._licensesDataModification()
        self.filterByInspectionType()

        
    def filterByInspectionType(self):
        
        self.data.licensesDf = self.data.licensesDf[self.data.licensesDf['ACTIVITIE_TYPE_ID']==1].copy()

    
    
    def buildFeatureDf(self) -> pd.DataFrame:
        self.filterByInspectionType()
        self.inspectionsWithLicensesDf = self.data.inspectionsDf.merge(self.data.licensesDf, how='inner',left_on='LICENSE NUMBER', right_on='License ID (MOMRAH)')
        self.inspectionsWithLicensesDf = self.inspectionsWithLicensesDf[~self.inspectionsWithLicensesDf['Latitude'].isna()]
        self.inspectionsWithLicensesDf.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=True, inplace=True)

        self.resultDf = self.inspectionsWithLicensesDf
        self.resultDf = self.resultDf.set_index('LICENSE NUMBER')
        groupBy = 'LICENSE NUMBER'
        self.getInspectionNumberFeature(groupBy)
        self.getPreviouslyIssuedFinesAmountFeature(groupBy)
        self.getIsFinePaidFeature()
        self.getPaidFinesAmountFeature()
        self.getCumulativePaidFinesAmountFeature(groupBy)
        self.getIsFineIssuedFeature()
        self.getIsNonCompliantFeature()
        self.getLastInspectionPercentageOfNonCompliantClausesFeature(groupBy)
        self.getPreviouslyIssuedFinesCountFeature(groupBy)
        self.getDaysSinceLastInspectionFeature(groupBy)
        self.getDaysSinceLicenseRenewalFeature()
        self.getDaysSinceLicenseEstablishmentFeature()
        self.getLast3InspectionsAverageComplianceFeature(groupBy)
        self.getLast3InspectionsPercentageOfComplianceFeature(groupBy)
        self.getLastInspectionComplianceFeature(groupBy)
        self.getIsNewBusinessFeature()
        self.getPaidFinesAmountPercentageFeature()
        self.getLastInspectionHighRiskViolationsFeature(groupBy)
        self.getLastInspectionFineIssued(groupBy)
        

        self.extractFeaturesFromDf()

        self.getFacilityTypeFeature()
        self.getBusinessActivityFeature()
        self.getTenancyFeature()
        # self.resultDf['days_since_last_inspection'] = self.resultDf['days_since_last_inspection'].dt.days#.fillna(9999)
        # self.resultDf['days_since_establishment'] = self.resultDf['days_since_establishment'].dt.days#.fillna(9999)

        self.resultDf[['Facility type', 'Business activity', 'Tenancy (Own/Rented)']] = self.resultDf[['Facility type', 'Business activity', 'Tenancy (Own/Rented)']].apply(pd.to_numeric)

        return self.resultDf

    def buildArtificialDf(self) -> pd.DataFrame:

        self.inspectionsWithLicensesDf = self.data.inspectionsDf.merge(self.data.licensesDf, how='right',left_on='LICENSE NUMBER', right_on='License ID (MOMRAH)')
        self.inspectionsWithLicensesDf = self.inspectionsWithLicensesDf[~self.inspectionsWithLicensesDf['Latitude'].isna()]
        self.inspectionsWithLicensesDf.sort_values(['LICENSE NUMBER', 'Inspection Date'], ascending=True, inplace=True)
        self.resultDf = self.inspectionsWithLicensesDf
        groupBy = 'License ID (MOMRAH)'
        self.getInspectionNumberFeature(groupBy)
        self.resultDf['Inspection Date'] = self.resultDf['Inspection Date'].dt.date
        self.resultDf.loc[self.resultDf['LICENSE NUMBER'].isna(), 'inspection number'] = 0

        last_inspection_by_license = self.resultDf.drop_duplicates('License ID (MOMRAH)', keep='last').copy()
        last_inspection_by_license.loc[:,'inspection number'] += 1
        last_inspection_by_license.loc[:, 'Inspection Date'] = pd.to_datetime("today").date()

        self.resultDf = pd.concat([self.resultDf, last_inspection_by_license])
        self.resultDf['Inspection Date'] = pd.to_datetime(self.resultDf['Inspection Date'])
        
        self.getIsNewBusinessFeature()
        self.getPreviouslyIssuedFinesAmountFeature(groupBy)
        self.getIsFinePaidFeature()
        self.getPaidFinesAmountFeature()
        self.getCumulativePaidFinesAmountFeature(groupBy)
        self.getIsFineIssuedFeature()
        self.getIsNonCompliantFeature()
        self.getLastInspectionPercentageOfNonCompliantClausesFeature(groupBy)
        self.getPreviouslyIssuedFinesCountFeature(groupBy)

        self.resultDf.sort_values(['License ID (MOMRAH)','inspection number'], ascending=True, inplace=True)
        self.resultDf['last_inspection_date'] = self.resultDf.groupby('License ID (MOMRAH)')['Inspection Date'].shift()
        self.resultDf['days_since_last_inspection'] = self.resultDf['Inspection Date'] - self.resultDf['last_inspection_date']
        self.resultDf['days_since_last_inspection'] = self.resultDf['days_since_last_inspection'].dt.days

        self.getDaysSinceLicenseRenewalFeature()
        self.getDaysSinceLicenseEstablishmentFeature()
        self.getLast3InspectionsAverageComplianceFeature(groupBy)
        self.getLast3InspectionsPercentageOfComplianceFeature(groupBy)
        self.getLastInspectionComplianceFeature(groupBy)
        self.getIsNewBusinessFeature()
        self.getPaidFinesAmountPercentageFeature()
        self.getLastInspectionHighRiskViolationsFeature(groupBy)
        self.getLastInspectionFineIssued(groupBy)
          
        self.extractFeaturesFromDf()

        self.getFacilityTypeFeature()
        self.getBusinessActivityFeature()
        self.getTenancyFeature()

        self.resultDf = self.resultDf.sort_values(['License ID (MOMRAH)','inspection number'], ascending=True)
        self.resultDf = self.resultDf.drop_duplicates(['License ID (MOMRAH)'], keep='last')
        
        active_licenses = self.data.licensesDf.loc[self.data.licensesDf['License Expiry Date'] > pd.to_datetime("today")]

        self.resultDf[['Facility type', 'Business activity', 'Tenancy (Own/Rented)']] = self.resultDf[['Facility type', 'Business activity', 'Tenancy (Own/Rented)']].apply(pd.to_numeric)
        self.resultDf =  self.resultDf[ self.resultDf['License ID (MOMRAH)'].isin(active_licenses['License ID (MOMRAH)'])]
        
        return self.resultDf