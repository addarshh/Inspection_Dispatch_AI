import pandas as pd
import geopandas
import re
import copy
from os import path
import numpy as np
import datetime

import config
from .Inspection import Inspection
from .FileDatabase import fileDatabase


class InspectionCP(Inspection):
		def __init__(self, data : fileDatabase, type : str, activity_type_id : str):
			super().__init__(data)

			self.casesGeometryWithBuffer = None
			self.caseIdToLicenseId = None
			self.crmBaladyMom : pd.DataFrame = None

			self._filterByInspectionType(type, activity_type_id)
			self._initResultDf()
			self._licensesDataModification()
			self._getCasesGeometryWithBuffer()
			self._mapCrmCaseIdToBaladyLicenseId()
			self._inspectionDataModification()
			self.crmBaladyMom = self.caseIdToLicenseId[['CaseId','LicenseId']].copy()

		def _mapCrmCaseIdToBaladyLicenseId(self):
			self.caseIdToLicenseId = geopandas.sjoin(self.casesGeometryWithBuffer, self.data.licensesDf, how = 'left', predicate = 'intersects')
			self.caseIdToLicenseId.drop(columns = ['index_right'], inplace = True )
			self.caseIdToLicenseId = self.caseIdToLicenseId[['CaseId','geometry','LicenseId']]

		def _inspectionDataModification(self):
			self.data.inspectionsDf.dropna(subset = ['LICENSE NUMBER'], inplace = True)
			self.data.inspectionsDf['LICENSE NUMBER'] = self.data.inspectionsDf['LICENSE NUMBER'].astype(int)
			self.data.inspectionsDf['Inspection Date'] = pd.to_datetime(self.data.inspectionsDf['Inspection Date'])


		def _getCasesGeometryWithBuffer(self):
			#Adding buffer to geometries of CRM data
			self.casesGeometryWithBuffer = self.resultData[['CaseId', 'geometry']].copy()
			# self.casesGeometryWithBuffer.to_crs(epsg= 32637,inplace = True)
			self.casesGeometryWithBuffer['geometry'] = self.casesGeometryWithBuffer.geometry.buffer(100)
			

		def _licensesDataModification(self) -> None:
			
			self.data.licensesDf = self.data.licensesDf[['License ID (MOMRAH)','Latitude','Longitude','Business activity']]
			self.data.licensesKeysDf = self.data.licensesKeysDf[['D_ACTIVITIES_ID','D_ACTIVITIES_NAME','ACTIVITIE_TYPE_ID','ACTIVITIE_TYPE_NAME']]

			self.data.licensesDf = pd.merge(self.data.licensesDf, self.data.licensesKeysDf, left_on = 'Business activity',right_on = 'D_ACTIVITIES_NAME')

			self.data.licensesDf = self.data.licensesDf[['License ID (MOMRAH)', 'ACTIVITIE_TYPE_ID', 'Latitude','Longitude']]
			self.data.licensesDf = self.data.licensesDf.dropna(subset = ['Latitude','Longitude'])

			# self.data.licensesDf = self.data.licensesDf.loc[self.data.licensesDf.Latitude != "|||"]
			# self.data.licensesDf = self.data.licensesDf[self.data.licensesDf['Latitude'].str.contains('ا') == False]
			self.data.licensesDf.drop_duplicates(inplace = True)

			self.data.licensesDf = self.data._fromPdToGdf(self.data.licensesDf, x = self.data.licensesDf.Latitude,y = self.data.licensesDf.Longitude)
			self.data.licensesDf = self.data.licensesDf[['License ID (MOMRAH)','geometry', 'ACTIVITIE_TYPE_ID']]
			self.data.licensesDf.rename(columns = {'License ID (MOMRAH)':'LicenseId'}, inplace = True)
			self.data.licensesDf.to_crs(epsg= 32637,inplace = True)



		def _filterByInspectionType(self, censorship_type : str, activity_type_id : str) :
			#Joining to extract all the CRM cases within Medinah
			filtered = self.data.classConfig.loc[self.data.classConfig['censorship type'] == censorship_type].copy()

			#Filtering for all the Main classification, Subcategory and Specialty classification adjoining to that censorship type
			self.data.crmData = self.data.crmData.loc[
				self.data.crmData.MAIN_Classificaion.isin(filtered['main classification']) 
			 	& self.data.crmData.Sub_Classificaion.isin(filtered['Subcategory'] )
				& self.data.crmData.SP_Classificaion.isin(filtered['Specialty Classification'])
			]

			self.data.licensesKeysDf = self.data.licensesKeysDf[self.data.licensesKeysDf['ACTIVITIE_TYPE_ID'] == activity_type_id].copy()	
				
		def _initResultDf(self):
			self.resultData = geopandas.sjoin(self.data.crmData, self.data.amanaDataGdf, how = "inner", predicate = "intersects")
			self.resultData.drop_duplicates(inplace = True)
			self.resultData.drop(['OBJECTID', 'REGION', 'AMANACODE', 'AMANAARNAM', 'AMANAENAME',
			 	'SHAPE_AREA', 'SHAPE_LEN','index_right'],axis = 1, inplace = True)
			self.resultData = self.resultData[['CaseId','geometry','LATITUDE','LONGITUDE','Priority_Value', 'PXCREATEDATETIME', 'Satisfaction']]
			self.resultData.to_crs(epsg= 32637, inplace = True)


		def _getDaysElapsedFeature(self):
			self.resultData['days_elapsed'] = (pd.to_datetime(datetime.datetime.now()) - pd.to_datetime(self.resultData['PXCREATEDATETIME'])).dt.days
			self.resultData.drop(columns=['PXCREATEDATETIME'], inplace=True)

		def _getSatisfactionLevelFeature(self):
			self.resultData['satisfaction_level'] = np.where(self.resultData.Satisfaction == 'Dissatisfied' ,-1, np.where(self.resultData.Satisfaction == 'Satisfied',1,0))
			self.resultData.drop(columns=['Satisfaction'], inplace=True)

		def _getDNFeature(self):
			self.resultData = geopandas.tools.sjoin(self.resultData, self.data.amanaPopulationOverlay2, how="left", predicate="intersects")

			self.resultData.drop(columns = ['index_right','GridNumber'], inplace = True)

		def _getNumberOfPriorityAreasFeature(self):
			self.data.priorityAreas.to_crs(epsg= 32637, inplace = True)

			#Calculating the number of priority areas for each case
			prarea = geopandas.sjoin(self.casesGeometryWithBuffer, self.data.priorityAreas, how = 'left', predicate = 'intersects')
			prarea.drop(columns = ['index_right'],inplace = True )
			#getting all the priority areas within the vicinity of a case
			preadf = prarea.groupby('CaseId')['Name'].nunique().to_frame(name = 'no_of_priority_areas').reset_index()


			self.resultData = pd.merge(self.resultData, preadf, on = 'CaseId')
			self.resultData.drop_duplicates(inplace = True)

		def _getNumberOfLicensesInVicinityFeature(self):

			retail_countdf = self.caseIdToLicenseId.groupby('CaseId')['LicenseId'].nunique().to_frame(name = 'no_of_retail_licenses_vicinity').reset_index()
			# retail_countdf.head()

			self.resultData = pd.merge(self.resultData, retail_countdf, on = 'CaseId')
			self.resultData.drop_duplicates(inplace = True)

		def _getDaysSinceLastInspectionFeature(self):
			#To get the latest inspection date for each license number 
			insp_datedf = self.data.inspectionsDf.groupby('LICENSE NUMBER')['Inspection Date'].max().to_frame(name = 'last_inspection_date').reset_index()
			insp_datedf['last_inspection_date'] = pd.to_datetime(insp_datedf['last_inspection_date']).dt.date
			insp_datedf['days_elapsed_last_inspection'] = (pd.to_datetime(datetime.datetime.now()) - pd.to_datetime(insp_datedf['last_inspection_date'])).dt.days
		
		
			#Joining it with balady_crm dataset to get days_elapsed_since_last_inspection and average_compliance_score correspondig to eah case
			self.crmBaladyMom = pd.merge(self.crmBaladyMom, insp_datedf[['days_elapsed_last_inspection', 'LICENSE NUMBER']], right_on = 'LICENSE NUMBER', left_on = 'LicenseId')
			self.crmBaladyMom.drop(columns=['LICENSE NUMBER'], inplace=True)
			# crm_balady_mom = crm_balady_mom[['CaseId','days_elapsed_last_inspection']]
			
			# self.resultData = pd.merge(self.resultData, crm_balady_mom, on = 'CaseId', how = 'left')
			# self.resultData.drop_duplicates(inplace = True)
		
		
		def _getAverageComplianceScore(self):
			#To get average compliance score of each license
			avg_compdf = self.data.inspectionsDf.groupby('LICENSE NUMBER')['Degree of Compliance'].mean().to_frame(name = 'avg_compliance_percent').reset_index()
			avg_compdf['avg_compliance_percent'].fillna(50.0, inplace=True)
			# avg_compdf.head()
			
			#Joining it with balady_crm dataset to get days_elapsed_since_last_inspection and average_compliance_score correspondig to eah case
			self.crmBaladyMom = pd.merge(self.crmBaladyMom, avg_compdf, right_on = 'LICENSE NUMBER', left_on = 'LicenseId')
			self.crmBaladyMom.drop(columns=['LICENSE NUMBER'], inplace=True)
			# crm_balady_mom = crm_balady_mom[['CaseId','avg_compliance_percent']]
			
			# self.resultData = pd.merge(self.resultData, crm_balady_mom, on = 'CaseId', how = 'left')
			# self.resultData.drop_duplicates(inplace = True)
			# print('alalalal')
		
		
			# #Joining it with balady_crm dataset to get days_elapsed_since_last_inspection and average_compliance_score correspondig to eah case
			# crm_balady_mom = pd.merge(self.caseIdToLicenseId, avg_compdf, left_on = 'LICENSE NUMBER', right_on = 'LicenseId')
			# crm_balady_mom = crm_balady_mom[['CaseId','days_elapsed_last_inspection','avg_compliance_percent']]
			
			# self.resultData = pd.merge(self.resultData, crm_balady_mom, on = 'CaseId')
			# self.resultData.drop_duplicates(inplace = True)
		
		def prepareFeatureDataset(self):

			if len(self.crmBaladyMom.columns) > 2:
				self.resultData = pd.merge(self.resultData, self.crmBaladyMom[self.crmBaladyMom.columns.difference(['LicenseId'])], on = 'CaseId', how = 'left')
			self.resultData.drop_duplicates(inplace = True)
			self.resultData['avg_compliance_percent'] = 100 - self.resultData['avg_compliance_percent']
			featuresColumns = ['DN','satisfaction_level','avg_compliance_percent', 'days_elapsed','no_of_priority_areas','no_of_retail_licenses_vicinity','days_elapsed_last_inspection']
			# self.resultData = self.resultData[featuresColumns]
			# self.resultData.drop_duplicates(inplace = True)

			self.resultData[featuresColumns] = self.resultData[featuresColumns].fillna(0)
			self.resultData['Priority_Value'] = self.resultData['Priority_Value'].fillna(1)
		
		def _outputToFile(self, name = 'common'):
			self.resultData.to_excel(path.join(path.dirname(__file__),config.FEATURE_CREATION_OUTPUT_FOLDER, 'cp', name + '.xlsx'), index = False)



		

			
