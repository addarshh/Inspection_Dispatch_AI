import pandas as pd
import geopandas
import re
import copy
from os import path
import config
from .Inspection import Inspection
from typing import Dict
from abc import ABC, abstractmethod

class InspectionRBD(Inspection):
		def __init__(self, data):
			super().__init__(data)
			self.featuresColumns =[
				'Business activity', 'Business Activity Weight', 'Facility type',
				'inspection number', 'previously issued fines amount',
				'cumulative_paid_fines', 'previously issued fines count',
				'days_since_last_inspection', 'days_since_establishment',
				'last_inspection_compliance','last_3_inspections_average_compliance',
				'paid_fines_percentage_amount', 'new_business', 'last_inspection_high_risk_violations',
				'last_inspection_fine_issued','last_3_inspections_percentage_of_compliance',
				'last_inspection_clauses_non_compliance_percentage', 'Tenancy (Own/Rented)'
			]
			self.labelsColumn = ['non_compliant']
			self.methods = {'trainTestModel' : 'trainTestModel', 'processModel' : 'processModel', 'getModelResults' : 'getModelResults'}


		def _inspectionDataModification(self):
			self.data.inspectionsDf['INSPECTYPE ID'] = self.data.inspectionsDf['INSPECTYPE ID'].astype(str)
			self.data.inspectionsDf['Inspection DateTime'] = self.data.inspectionsDf['Inspection Date']
			self.data.inspectionsDf['Inspection Date'] = pd.to_datetime(self.data.inspectionsDf['Inspection Date'])

			self.data.inspectionsDf.loc[self.data.inspectionsDf['Degree of Compliance'].isna(), 'compliance_score_available'] = False
			self.data.inspectionsDf['compliance_score_available'].fillna(True, inplace=True)
			
			self.data.inspectionsDf = self.data.inspectionsDf[~self.data.inspectionsDf['LICENSE NUMBER'].isna()]
			self.data.inspectionsDf = self.data.inspectionsDf[~self.data.inspectionsDf['Inspection Date'].isna()]
			self.data.inspectionsDf = self.data.inspectionsDf[~self.data.inspectionsDf['Degree of Compliance'].isna()]
		
		def _licensesDataModification(self):
			self.data.licensesDf.drop_duplicates('License ID (MOMRAH)', inplace=True)
			self.data.licensesDf = self.data.licensesDf.merge(self.data.licensesKeysDf, how='left', left_on='Business activity', right_on=['D_ACTIVITIES_NAME']).drop_duplicates(['License ID (MOMRAH)','Business activity'])
			self.data.licensesDf.drop_duplicates(['License ID (MOMRAH)','MOMTATHEL ACTIVITY NUMBER'], inplace=True)
		
		def getInspectionNumberFeature(self, groupBy : str) -> None:
			self.resultDf['inspection number'] = self.resultDf.groupby(groupBy).cumcount()
			self.resultDf.loc[:, 'inspection number'] += 1 
		
		def getPreviouslyIssuedFinesAmountFeature(self, groupBy : str)-> None:
				self.resultDf['previously issued fines amount'] = self.resultDf.groupby(groupBy)['Issued fine amount'].transform(lambda x: x.cumsum().shift())

		def getIsFinePaidFeature(self)-> None:
				self.resultDf.loc[self.resultDf['Fine payment status']=='bill has been paid', 'fine_payment']=1
				self.resultDf['fine_payment'].fillna(0, inplace=True)

		def getPaidFinesAmountFeature(self)-> None:
				self.resultDf['paid_fines_amount']=self.resultDf['fine_payment']*self.resultDf['Issued fine amount']

		def getCumulativePaidFinesAmountFeature(self, groupBy)-> None:
				self.resultDf['cumulative_paid_fines'] = self.resultDf.groupby(groupBy)['paid_fines_amount'].transform(lambda x: x.cumsum().shift())

		def getIsFineIssuedFeature(self)-> None:
				self.resultDf.loc[self.resultDf['Issued fine amount']>0, 'fine_issued']=1
				self.resultDf['fine_issued'].fillna(0, inplace=True)
				
		
		def getIsNonCompliantFeature(self) -> None:
				self.resultDf.loc[(self.resultDf['Issued fine amount']>0)
								| (self.resultDf['Number of non-compliant clauses']>0)
								| (self.resultDf['Degree of Compliance']<100)
								, 'non_compliant'] = 1
				self.resultDf['non_compliant'] = self.resultDf['non_compliant'].fillna(0)
		
		def getLastInspectionPercentageOfNonCompliantClausesFeature(self, groupBy : str)-> None:
				self.resultDf['percentage_non_compliant_clauses'] = 100 * self.resultDf['Number of non-compliant clauses'] / self.resultDf['Number of clauses']
				self.resultDf['last_inspection_clauses_non_compliance_percentage'] = self.resultDf.groupby(groupBy)['percentage_non_compliant_clauses'].shift()
		
		def getPreviouslyIssuedFinesCountFeature(self, groupBy : str)-> None:
				self.resultDf['previously issued fines count'] = self.resultDf.groupby(groupBy)['fine_issued'].transform(lambda x: x.cumsum().shift())

		def getDaysSinceLastInspectionFeature(self, groupBy : str)-> None:
				self.resultDf['days_since_last_inspection'] = self.resultDf.groupby(groupBy)['Inspection Date'].diff()
				self.resultDf['days_since_last_inspection'] = self.resultDf['days_since_last_inspection'].dt.days
		
		def getDaysSinceLicenseRenewalFeature(self) -> None:
				self.resultDf['days_since_license_renewal'] = self.resultDf['Inspection Date'] - self.resultDf['Last License renewal date']
				

		def getDaysSinceLicenseEstablishmentFeature(self) -> None:
				self.resultDf['days_since_establishment'] = self.resultDf['Inspection Date'] - self.resultDf['License Start Date']
				self.resultDf['days_since_establishment'] = self.resultDf['days_since_establishment'].dt.days

		def getLast3InspectionsAverageComplianceFeature(self, groupBy : str)-> None:
				self.resultDf['last_3_inspections_average_compliance'] = self.resultDf.groupby(groupBy)['Degree of Compliance'].rolling(3, min_periods=1).mean().reset_index(0, drop=True)
				self.resultDf['last_3_inspections_average_compliance'] = self.resultDf.groupby(groupBy)['last_3_inspections_average_compliance'].shift()

		def getLast3InspectionsPercentageOfComplianceFeature(self, groupBy : str)-> None:
				self.resultDf['last_3_inspections_percentage_of_compliance'] = self.resultDf.groupby(groupBy)['Degree of Compliance'].rolling(3, min_periods=1).mean().reset_index(0,drop=True)
				self.resultDf['last_3_inspections_percentage_of_compliance'] = self.resultDf.groupby(groupBy)['last_3_inspections_percentage_of_compliance'].shift()

		def getLastInspectionComplianceFeature(self, groupBy : str)-> None:
				self.resultDf['last_inspection_compliance'] = self.resultDf.groupby(groupBy)['Degree of Compliance'].shift()

		def getIsNewBusinessFeature(self) -> None:
				self.resultDf.loc[self.resultDf['inspection number']==1,'new_business'] = 1
				self.resultDf['new_business']=self.resultDf['new_business'].fillna(0)

		def getPaidFinesAmountPercentageFeature(self) -> None:
				self.resultDf['paid_fines_percentage_amount'] = 100*self.resultDf['cumulative_paid_fines']/self.resultDf['previously issued fines amount']

		def getLastInspectionHighRiskViolationsFeature(self, groupBy : str)-> None:
				self.resultDf['last_inspection_high_risk_violations'] = self.resultDf.groupby(groupBy)['Number of non-compliant clauses and High risk'].shift()

		def getLastInspectionFineIssued(self, groupBy : str)-> None:
				self.resultDf['last_inspection_fine_issued'] = self.resultDf.groupby(groupBy)['fine_issued'].shift()

		def getFacilityTypeFeature(self)->None:

				dict = self._getColumnDictionary('Facility type')
				self.resultDf.replace({'Facility type': dict}, inplace=True)  

				facility_types_mapping={
				0:'Issuing licenses to establish or operate ATMs',
				1:'other activities',
				2:'educational activities',
				3:'medical activities',
				4:'restrooms',
				5:'roving carts',
				6:'hotels, hotel apartments, resorts and the like',
				7:'shops',
				8:'Warehouses',
				9:'Kitchens, restaurants and the like',
				10:'Professional workshops',
				11:'Palaces of weddings',
				12:'Gas stations (within the urban area)',
				13:'Application shops',
				14:'Oil and grease changing shops and car washes',
				15:'Amusement parks and entertainment'}

				self.resultDf['Facility type (English)'] = self.resultDf['Facility type'].map(facility_types_mapping)
				
				# self.resultDf.replace({'Facility type': facility_types_mapping}, inplace=True)  ?



		def getBusinessActivityFeature(self) -> None:
				dict = self._getColumnDictionary('Business activity')
				self.resultDf.replace({'Business activity':dict}, inplace=True)

		def getTenancyFeature(self) -> None:
				dict = self._getColumnDictionary('Tenancy (Own/Rented)')
				self.resultDf.replace({'Tenancy (Own/Rented)':dict}, inplace=True)

		def _getColumnDictionary(self, columnName : str) -> Dict[int, str]:
				self.data.licensesDf.loc[:,columnName] = self.data.licensesDf[columnName].astype('category')
				dictionary = dict( enumerate(self.data.licensesDf[columnName].cat.categories ) )
				dictionary = {v: k for k, v in dictionary.items()}
				return dictionary


		def extractFeaturesFromDf(self):
				# features_columns=[
				#     'Business activity', 'Business Activity Weight', 'Facility type',
				#     'inspection number', 'previously issued fines amount',
				#     'cumulative_paid_fines', 'previously issued fines count',
				#     'days_since_last_inspection', 'days_since_establishment',
				#     'last_inspection_compliance','last_3_inspections_average_compliance',
				#     'paid_fines_percentage_amount', 'new_business', 'last_inspection_high_risk_violations',
				#     'last_inspection_fine_issued','last_3_inspections_percentage_of_compliance',
				#     'last_inspection_clauses_non_compliance_percentage', 'Tenancy (Own/Rented)'
				# ]
				# labels_column=['non_compliant']
				self.resultDf = self.resultDf[['License ID (MOMRAH)', 'INSEPECTION ID', 'Latitude', 'Longitude'] + config.features_columns + config.labels_column]
		def dropUnnecessaryColumns(self):
				self.resultDf.drop(['INSPECTYPE ID', 'INSPECTION NAME', 'Establishment Name', 
				'Business Activity Description', 'Status of Work', 'TYPE OF VISIT', 'Business Activity Number', 
				'Number of non-compliant clauses', 'Number of non-compliant clauses and medium risk', 'SADAD NO', 
				'SADAD PAYMENT DATE', 'Inspector_Action', 'APPROVER CONFISCATION', 'APPROVER FOLLOWUP', 'APPROVER DESTROY', 
				'APPROVER SAMPLE', 'APPROVER CLOSE', 'NO LICENSE', 'Inspection DateTime', 'compliance_score_available', 
				'Commercial Reg. Number', '700 - number', 'List of activities', 
				'Operating hours', 'Commercial Building ID', 'D_ACTIVITIES_ID', 'D_ACTIVITIES_NAME', 'MASTER_ACTIVITIES_ID', 
				'ISIC_NEW_ID', 'ISIC_DESC', 'ACTIVITIE_TYPE_ID', 'ACTIVITIE_TYPE_NAME', 'MOMTATHEL ACTIVITY NUMBER'], inplace=True
				)
		@abstractmethod
		def buildFeatureDf():
				pass

		@abstractmethod
		def buildArtificialDf():
				pass