import os
import sys
import config
import classes.Database as DB
import pandas as pd
import traceback
import numpy as np
from classes.types.RBD.Retail import Retail as RetailRBD
from classes.engines.RiskBasedDispatch import RiskBasedDispatch
from classes.engines.CasePrioritization import CasePrioritization
import classes.engines.Helper as Help
import logging
import datetime
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
Helper=Help.Helper()
Helper.Engine_Start_Metadata_Update()

if __name__ == "__main__":

    if len(config.MODELS_TO_RUN) and len(config.INSPECTION_CATEGORIES):
        logging.basicConfig(filename="log.txt", level=logging.INFO)
        logging.basicConfig(filename="log.txt", level=logging.ERROR)
        engines = {}
        logging.info("###########################################")
        logging.info("Started getting data from data sources")
        data = DB.Database(config.AMANA_CODE)

        #recordId = data.insertModelStatus('DATA_FETCHING', getDateString(), 'In progress')
        
        #data.updateModelStatus(recordId, None, 'common data fetched')
   
        if 'RBD' in config.MODELS_TO_RUN:
            engines['RBD'] = RiskBasedDispatch()
            if 'Excavation' in config.INSPECTION_CATEGORIES and len(config.INSPECTION_CATEGORIES) == 1:
                data.getExcavationData()
            else :
                data.getPoiDataGdf()
                data.getCommonData()
                data.getAmanaPopulationOverlay()
                data.getPriorityAreasData()
                data.getGridZones()
                #data.updateModelStatus(recordId, None, 'rbd data fetched')
                logging.info("Started getting data from data sources")
                if 'Excavation' in config.INSPECTION_CATEGORIES:
                    data.getExcavationData()

        
        if 'CP' in config.MODELS_TO_RUN:
            data.getCommonData()
            data.getCrmCases()
            data.getClassConfig()
            data.getAmanaDataGdf()
            data.getPopulationDataGdf()
            data.getAmanaPopulationOverlay2()
            data.getPriorityAreasData()
            engines['CP'] = CasePrioritization()
            #data.updateModelStatus(recordId, None, 'CP data fetched')

        #data.updateModelStatus(recordId, getDateString(), 'Finished')

        for engine in config.MODELS_TO_RUN:
            if engine == 'CP':
                for inspection in config.INSPECTION_CATEGORIES:
                    #recordId = data.insertModelStatus(engine + '_Inspection_' + inspection, getDateString(), 'In progress')

                    try :
                        inspectionName = globals()[inspection + engine]
                        inspectionInstance = inspectionName(data)
                        
                        #data.updateModelStatus(recordId, getDateString(), 'building features')
                        inspectionInstance.getFeatures()
                        engines[engine].setInspectionType(inspectionInstance)
                        #data.updateModelStatus(recordId, getDateString(), 'running model')
                        engines[engine].process()
                        #data.updateModelStatus(recordId, getDateString(), 'Finished')
                        del inspectionInstance
                    except Exception as e:
                        print(e)
                        traceback.print_exc()
                        #data.updateModelStatus(recordId, getDateString(), 'Error occured')
            if engine == 'RBD':

                for inspection in config.INSPECTION_CATEGORIES:
                    # data.deleteFromTable('INSPECTIONS_RBD_' + inspection + '_OUTPUT_GRIDS')
                    # data.deleteFromTable('INSPECTIONS_RBD_' + inspection + '_OUTPUT_LICENSES')
                    # print('delete from table')
                    #recordId = data.insertModelStatus(engine + '_Inspection_' + inspection, getDateString(), 'In progress')
                        
                    try:
                        print('inspection')
                        print(inspection)
                        print('engine')
                        print(engine)

                        inspectionName = globals()[inspection + engine]
                    
                        inspectionInstance = inspectionName(data)
                        print('inspectionInstance')
                        print(inspectionInstance)
                        
                        #data.updateModelStatus(recordId, None, 'building feature DF')
                        trainDataset : pd.DataFrame = inspectionInstance.buildFeatureDf()

                        trainTestModelFunc = getattr(engines[engine], inspectionInstance.methods['trainTestModel'])
                        trainTestModelFunc(trainDataset, inspectionInstance.featuresColumns, inspectionInstance.labelsColumn)
                        trainDataset = None
                        
                        #data.updateModelStatus(recordId, None, 'building artifical DF')
                        artificialDataset : pd.DataFrame = inspectionInstance.buildArtificialDf()

                        if artificialDataset is not None:
                            output_table_name = config.FEATURES_OUTPUT_TABLE_NAME
                            Helper.backup(output_table_name)
                            Helper.insert_df_Batchwise( artificialDataset, output_table_name, 50000)
                            logging.info("OUTPUT:FEATURES df:artificialDataset Shape: {}".format(artificialDataset.shape))
        
                        processModelFunc = getattr(engines[engine], inspectionInstance.methods['processModel'])
                        predictedResult = processModelFunc(artificialDataset, inspectionInstance.featuresColumns, inspectionInstance.labelsColumn)

                        #data.updateModelStatus(recordId, None, 'running model')
                        getModelResultsFunc = getattr(engines[engine], inspectionInstance.methods['getModelResults'])
                        tooltip_df, points_for_qgis = getModelResultsFunc(data, predictedResult, inspection,  inspectionInstance.data.licensesDf)

                        #data.updateModelStatus(recordId, None, 'saving to the database')
 
                        if tooltip_df is not None:
                            #data.updateModelStatus(recordId, None, 'saving to the database grids')
                            #data.saveRBDOutput(tooltip_df, table_name = f"inspections_rbd_{inspection}_output_grids".lower())
                            
                            output_table_name = config.GRIDS_OUTPUT_TABLE_NAME
                            Helper.backup(output_table_name)
                            #print(tooltip_df.head(2))
                            tooltip_df_subset = tooltip_df[tooltip_df['NUMBER_OF_LICENSES'] > 0]
                            tooltip_df_subset.rename(columns = {'GUID':'GRID_ZONE'},inplace = True)
                            print(tooltip_df_subset.shape)
                            print(tooltip_df.shape)
                            #print(tooltip_df_subset.head(2))
                            Helper.insert_df_Batchwise( tooltip_df_subset, output_table_name, 10000)
                            logging.info("OUTPUT:LICENCES df:tooltip_df Shape: {}".format(tooltip_df.shape))
        
                        if points_for_qgis is not None:
                            
                            points_for_qgis['FACILITY_TYPE_ENGLISH'] = points_for_qgis['FACILITY_TYPE_ENGLISH'].astype(str)

                            #data.updateModelStatus(recordId, None, 'saving to the database licenses')
                            #data.saveRBDOutput(points_for_qgis.iloc[:, :], table_name = f"inspections_rbd_{inspection}_output_licenses".lower())
                            print("point for qgis", points_for_qgis.columns)
                            logging.info("OUTPUT:GRIDS df:points_for_qgis Shape: {}".format(points_for_qgis.shape))

                            points_for_qgis.rename(columns = {'GUID':'GRID_ZONE'},inplace = True)
        
                            points_for_qgis_subset = points_for_qgis[['GRID_NUMBER', 'LICENSE_ID', 
                            'LATITUDE', 'LONGITUDE', 'status_id',
                            # 'BUSINESS_ACTIVITY', 'BUSINESS_ACTIVITY_WEIGHT', 'FACILITY TYPE',
                            # 'INSPECTION_NUMBER', 'PREVIOUSLY_ISSUED_FINES_COUNT',
                            # 'CUMULATIVE_PAID_FINES', 'PREVIOUSLY_ISSUED_FINES_COUNT',
                            # 'DAYS_SINCE_LAST_INSPECTION', 'DAYS_SINCE_ESTABLISHMENT',
                            # 'LAST_INSPECTION_COMPLIANCE', 'LAST_3_INSPECTIONS_AVERAGE_COMPLIANCE',
                            # 'PAID_FINES_PERCENTAGE_AMOUNT', 
                            'NEW_BUSINESS',#"Inspection Date","License Start Date","License Expiry Date",
                            # 'LAST_INSPECTION_HIGH_RISK_VIOLATIONS', 'LAST_INSPECTION_FINE_ISSUED',
                            # 'LAST_3_INSPECTIONS_PERCENTAGE_OF_COMPLIANCE',
                            # 'LAST_INSPECTION_CLAUSES_NON_COMPLIANCE_PERCENTAGE',
                            #'TENANCY (OWN/RENTED)', 'NON_COMPLIANT', 'FACILITY_TYPE_ENGLISH',
                            #'RUN_TIME', 
                            'PROBABILITY_OF_VIOLATION', #'PROBABILITY_OF_NO_VIOLATION',
                            'AREA_OF_THE_RETAIL_OUTLET', #'SCALED_AREA_OF_THE_RETAIL_OUTLET',
                            'PROBABILITY_OF_VIOLATION_DISCRETE', 'BUSINESS_RISK', #'NEVER_INSPECTED',
                            'NUMBER_OF_PAST_INSPECTIONS', #'GRID_ID', 
                            'MUNICIPA_1', 'AMANA', 'AMANACODE',
                            'GridName', 'MUNICIPALI', 'REGION', 'REGIONCODE', 'DN',
                            'GRIDUNIQUECODE','GRID_ZONE']]


                            #Extracting last inspection date and adding last inspection date, license start and expiry dates to output
                            inspections_date = data.inspectionsDf.loc[:, ['LICENSE NUMBER', 'Inspection Date']]
                            license_date = data.licensesDf.loc[:,['License ID (MOMRAH)', 'License Start Date', 'License Expiry Date']]
                            inspections_date['Inspection Date'] = pd.to_datetime(inspections_date['Inspection Date'])
                            inspections_last_date = inspections_date.groupby(['LICENSE NUMBER']).agg(LastInspectiondate=('Inspection Date', np.max))
                            date_data = license_date.merge(inspections_last_date, how='left',left_on='License ID (MOMRAH)', right_on='LICENSE NUMBER')
                            points_for_qgis_subset_dates=points_for_qgis_subset.merge(date_data,how='left',right_on='License ID (MOMRAH)', left_on='LICENSE_ID')
                            points_for_qgis_subset_dates = points_for_qgis_subset_dates.rename(columns={'License Start Date': 'License_Start_Date','License Expiry Date':'License_Expiry_Date','LastInspectiondate':'Last_Inspection_date'})
                            points_for_qgis_subset_dates.drop(columns=['License ID (MOMRAH)'], inplace=True)


                            output_table_name = config.LICENSES_OUTPUT_TABLE
                            Helper.backup(output_table_name)
                            Helper.insert_df_Batchwise( points_for_qgis_subset_dates, output_table_name, 10000)
                            logging.info("OUTPUT:GRIDS after saving df:points_for_qgis Shape: {}".format(points_for_qgis.shape))
                            Helper.Engine_End_Metadata_Update()

        

                        #data.updateModelStatus(recordId, getDateString(), 'Finished')

                        del inspectionInstance
                    except Exception as e:
                        print(e)
                        traceback.print_exc()
                        #data.updateModelStatus(recordId, getDateString(), 'Error occured')
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)
                    
