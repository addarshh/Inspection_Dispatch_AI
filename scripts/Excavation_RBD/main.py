import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
from inspect import trace
import classes.FileDatabase as fDB
import warnings
warnings.filterwarnings("ignore")
from classes import Database as DB
# from classes.types.RBD.Retail import Retail as RetailRBD
# from classes.types.RBD.Health import Health as HealthRBD
from classes.types.RBD.Excavation import Excavation as ExcavationRBD
# from classes.types.CP.Retail import Retail as RetailCP
# from classes.types.CP.Health import Health as HealthCP
import pandas as pd
import geopandas as gpd
import traceback
import time
import datetime
# from datetime import datetime
from helpers import getDateString
import classes.engines.Helper as Help
from classes.engines.RiskBasedDispatch import RiskBasedDispatch
from classes.engines.CasePrioritization import CasePrioritization
from classes import GISDatabase as GDB
import logging
#gdata=GDB.GISDatabase()
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
Helper=Help.Helper()
data=DB.Database()

if __name__ == "__main__":

    if len(config.MODELS_TO_RUN) and len(config.INSPECTION_CATEGORIES):
        logging.basicConfig(filename="excavationlog.txt", level=logging.INFO)
        logging.basicConfig(filename="excavationlog.txt", level=logging.ERROR)
        start_time_final = datetime.datetime.now()
        engines = {}
        Helper.Engine_Start_Metadata_Update()
        # recordId = data.insertModelStatus('DATA_FETCHING', datetime.now(), 'In progress')
        # data.updateModelStatus(recordId, None, 'common data fetched')
        if 'RBD' in config.MODELS_TO_RUN:
            logging.info("Config Models to Run is set to RBD")
            #data = DB.Database(config.AMANA_CODE)
            #data = DB.Database()
            
            logging.info("Started getting data from data sources")
            print("Started getting data from data sources")
            engines['RBD'] = RiskBasedDispatch()
            if 'Excavation' in config.INSPECTION_CATEGORIES and len(config.INSPECTION_CATEGORIES) == 1:
                logging.info("Reading Excavation data..")
                data.getExcavationData()
                logging.info("Reading Excavation data.. Completed")

            else :
                start_time1  = datetime.datetime.now()
      
                logging.info("Reading Common data..")
                data.getCommonData()
                logging.info("Reading Common data..Completed")


                logging.info("Reading Population data..")
                data.getPopulationDataGdf()
                logging.info("Reading Population data..Completed")

                logging.info("Reading Amana Population Overlay data..")
                data.getAmanaPopulationOverlay()
                logging.info("Reading Amana Population Overlay data..Completed")

                logging.info("Reading Priority Area data..")
                # data.getWaterData()
                gdata.getPriorityAreasData()
                logging.info("Reading Priority Area data..Completed")
                #data.updateModelStatus(recordId, None, 'rbd data fetched')
                
                if 'Excavation' in config.INSPECTION_CATEGORIES:
                    logging.info("Reading Excavation data..")
                    data.getExcavationData()
                    logging.info("Reading Excavation data.. Completed")

                logging.info("Completed getting data from data sources")
                print("Completed getting data from data sources")
                logging.info("Time Taken to fetch all the data: " +str(datetime.datetime.now() - start_time) )
                print("Time Taken to fetch all the data: " +str(datetime.datetime.now() - start_time) )

        
        if 'CP' in config.MODELS_TO_RUN:
            logging.info("Config Models to Run is set to CP")

            logging.info("Reading Common data..")
            data.getCommonData()
            logging.info("Reading Common data..Completed")

            logging.info("Reading CRM data..")
            data.getCrmCases()
            logging.info("Reading CRM data..Completed")

            logging.info("Reading Class Config data")
            data.getClassConfig()
            logging.info("Reading Class Config data..Completed")

            logging.info("Reading Amana data")
            data.getAmanaDataGdf()
            logging.info("Reading Amana data..Completed")

            logging.info("Reading Population data")
            data.getPopulationDataGdf()
            logging.info("Reading Population data..Completed")

            logging.info("Reading Amana Population Overlay data")
            data.getAmanaPopulationOverlay2()
            logging.info("Reading Amana Population Overlay data.. Completed")

            logging.info("Reading Priority Area data")
            gdata.getPriorityAreasData()
            logging.info("Reading Priority Area data.. Completed")

            logging.info("Completed getting data from data sources")
            print("Completed getting data from data sources")
            logging.info("Time Taken to fetch all the data: " +str(datetime.datetime.now() - start_time) )
            print("Time Taken to fetch all the data: " +str(datetime.datetime.now() - start_time) )

            engines['CP'] = CasePrioritization()

            
            #data.updateModelStatus(recordId, None, 'CP data fetched')

        #data.updateModelStatus(recordId, datetime.now(), 'Finished')

        for engine in config.MODELS_TO_RUN:
            if engine == 'CP':
                start_time1  = datetime.datetime.now()
                logging.info("Initializing CP EXCAVATION engine..")
                print("Initializing CP EXCAVATION engine..")
                for inspection in config.INSPECTION_CATEGORIES:
                    #recordId = data.insertModelStatus(engine + '_Inspection_' + inspection, getDateString(), 'In progress')

                    try :
                        inspectionName = globals()[inspection + engine]
                        inspectionInstance = inspectionName(data)
                        
                        #data.updateModelStatus(recordId, datetime.now(), 'building features')
                        logging.info("Preparing data for model run..")
                        inspectionInstance.getFeatures()
                        logging.info("Preparing data for model run..Completed")
                        engines[engine].setInspectionType(inspectionInstance)

                        #data.updateModelStatus(recordId, datetime.now(), 'running model')
                        start_time1  = datetime.datetime.now()
                        logging.info("Running the predictions..")
                        print("Running the predictions..")
                        engines[engine].process()
                        logging.info("Running the predictions..Completed")
                        print("Running the predictions..Completed")
                        logging.info("Process Time Taken: " + str(datetime.datetime.now() - start_time1) )
                        print("Process Time Taken: " + str(datetime.datetime.now() - start_time1) )
                        #data.updateModelStatus(recordId, datetime.now(), 'Finished')
                        del inspectionInstance
                    except Exception as e:
                        logging.info("Error in running CP EXCAVATION ENGINE: " + str(e) )
                        print(e)

                        #data.updateModelStatus(recordId, datetime.now(), 'Error occured')
            if engine == 'RBD':
                start_time1  = datetime.datetime.now()
                logging.info("Initializing RBD EXCAVATION engine..")
                print("Initializing RBD EXCAVATION engine..")
                for inspection in config.INSPECTION_CATEGORIES:
                    # recordId = data.insertModelStatus(engine + '_Inspection_' + inspection, datetime.now(), 'In progress')
                        
                    try:
                        inspectionName = globals()[inspection + engine]
                        inspectionInstance = inspectionName(data)

                        logging.info("Building Feature Data FRAME" )
                        #data.updateModelStatus(recordId, None, 'building feature DF')
                        trainDataset : pd.DataFrame = inspectionInstance.buildFeatureDf()
                        logging.info("Building Feature Data FRAME..Completed" )
                        if len(trainDataset) == 0:
                            print('Dataset for training and testing the model is empty.\nFurther execution of the script is not possible.')
                            logging.info("Dataset for training and testing the model is empty.\nFurther execution of the script is not possible." )
                            exit()
                        
                        
                        trainTestModelFunc = getattr(engines[engine], inspectionInstance.methods['trainTestModel'])
                        print(trainDataset.columns)
                        print(trainDataset.shape)
                        logging.info("TrainDataset Columns:" + str(trainDataset.columns) )
                        logging.info("TrainDataset Shape:" + str(trainDataset.shape) )
                        
                        
                        logging.info("Calling trainTestModelExcavation function" )

                        trainTestModelFunc(trainDataset, inspectionInstance.featuresColumns, inspectionInstance.labelsColumn)

                        #data.updateModelStatus(recordId, None, 'building artifical DF')
                        artificialDataset : pd.DataFrame = inspectionInstance.buildArtificialDf()

                        
                        if artificialDataset is not None:
                            output_table_name = config.FEATURES_OUTPUT_TABLE_NAME
                            # Helper.backup(output_table_name)
                            # Helper.insert_df_Batchwise( artificialDataset, output_table_name, 50000)
                        start_time1  = datetime.datetime.now()
                        logging.info("Running the predictions..")
                        processModelFunc = getattr(engines[engine], inspectionInstance.methods['processModel'])
                        predictedResult = processModelFunc(artificialDataset, inspectionInstance.featuresColumns, inspectionInstance.labelsColumn)
                        print("Running the predictions..Completed")
                        logging.info("Process Time Taken: " + str(datetime.datetime.now() - start_time1) )
                        ##data.updateModelStatus(recordId, None, 'running model')
                        
                        getModelResultsFunc = getattr(engines[engine], inspectionInstance.methods['getModelResults'])
                        tooltip_df, points_for_qgis = getModelResultsFunc(data, predictedResult, inspection,  inspectionInstance.data.licensesDf)
                    
                        #data.updateModelStatus(recordId, None, 'saving to the database')
                        
                        if points_for_qgis is not None:
                            
                            # data.deleteRBDOutputLicenses(inspection, 'INSPECTIONS_RBD_' + inspection + '_OUTPUT_LICENSES')
                            # data.saveRBDOutputLicenses(points_for_qgis, table_name = f"inspections_rbd_{inspection}_output_licenses".lower())

                            output_table_name = config.LICENSES_OUTPUT_TABLE

                            #ADDED
                            populationgrid_df = GDB.getPopulationData()
                            print(populationgrid_df.shape)
                            print(points_for_qgis.columns)
                            print(points_for_qgis.shape)
                            #points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry='GEOMETRY', crs="EPSG:4326")
                            # points_for_qgis['long'] = pd.to_numeric(points_for_qgis['long'], errors='coerce')
                            # points_for_qgis['lat'] = pd.to_numeric(points_for_qgis['lat'], errors='coerce')
                            
                            # # REVERSING LAT AND LONG
                            #points_for_qgis.rename(columns = {'START_POINT_Y':'START_POINT_X', 'START_POINT_X':'START_POINT_Y'}, inplace = True)
                            
                            points_for_qgis = gpd.GeoDataFrame(points_for_qgis, geometry=gpd.points_from_xy(points_for_qgis.START_POINT_Y, points_for_qgis.START_POINT_X), crs="EPSG:4326")
    
                            points_for_qgis = gpd.sjoin(populationgrid_df, points_for_qgis,how="right",predicate="intersects")
                            print(points_for_qgis.columns)
                            print(points_for_qgis.shape)

                            Zone_df = GDB.get_grid_zone()
                            logging.info("""Fetched zone_grids shape: {}
                            """.format(Zone_df.shape))
                            print(Zone_df.shape)
                            points_for_qgis_withzone=points_for_qgis.merge(Zone_df, how="left", left_on="GridNumber", right_on="GRIDUNIQUECODE")
                            logging.info("""OUTPUT tooltip_df_subset_withzone shape: {}
                            """.format(points_for_qgis_withzone.shape))
                            print(points_for_qgis_withzone.shape)
                            points_for_qgis_withzone.to_csv('tooltip_df_subset_withzone.csv')
                            points_for_qgis_withzone = points_for_qgis_withzone.loc[:,points_for_qgis_withzone.columns != 'GEOMETRY']
                            Helper.backup(output_table_name)
                            logging.info("Inserting prediction in DB, having shape : " + str(points_for_qgis.shape) )
                            Helper.insert_df_Batchwise( points_for_qgis_withzone, output_table_name, 50000)
                        if tooltip_df is not None:
                            # data.deleteRBDOutputGrids(inspection, 'INSPECTIONS_RBD_' + inspection + '_OUTPUT_GRIDS')
                            # data.saveRBDOutputGrids(tooltip_df, table_name = f"inspections_rbd_{inspection}_output_grids".lower())
                            output_table_name = config.GRIDS_OUTPUT_TABLE_NAME
                            #Helper.backup(output_table_name)
                            Helper.insert_df_Batchwise( tooltip_df, output_table_name, 50000)
                        
                        #data.updateModelStatus(recordId, datetime.now(), 'Finished')
                        Helper.Engine_End_Metadata_Update()
                        logging.info("Excavation RBD Model completed Successfully")
                        print("Excavation RBD Model completed Successfully")
                        logging.info("Total Time Taken for the model: " + str(datetime.datetime.now() - start_time_final) )
                        print("Total Time Taken for the model: " + str(datetime.datetime.now() - start_time_final) )
                        del inspectionInstance
                    except Exception as e:
                        traceback.print_exc()
                        print(e)
                        logging.info("Error in running Excavation RBD Model: " + str(e) )
                        #data.updateModelStatus(recordId, datetime.now(), 'Error occured')
    #Helper.Engine_End_Metadata_Update()
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)