import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
import sys
import classes.Database as DB
import classes.GISDatabase as GDB
import classes.engines.Helper as Helper
from classes.types.RBD.GeneralCleaning import GeneralCleaning
import logging
import traceback
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)

if __name__ == "__main__":
    logging.basicConfig(filename="generalcleaning.txt", level=logging.INFO)
    logging.basicConfig(filename="generalcleaning.txt", level=logging.ERROR)

    Helper = Helper.Helper()
    if len(config.MODELS_TO_RUN):

        if 'GENERAL_CLEANING' in config.MODELS_TO_RUN:
            try:
                engines = {}
                data = DB.Database()
                gisdata = GDB.GISDatabase()
                Helper.Engine_Start_Metadata_Update()
                logging.info("Started getting data from data sources")
                print("Started getting data from data sources")

                ##### Normal Data
                start_time1  = datetime.datetime.now()              

                logging.info("Reading getPOINTS_OF_INTERESTS1 data..")
                data.getPOINTS_OF_INTERESTS1()
                logging.info("Reading getPOINTS_OF_INTERESTS1 data..Completed")

                logging.info("Reading getPOINTS_OF_INTERESTS2 data..")
                data.getPOINTS_OF_INTERESTS2()
                logging.info("Reading getPOINTS_OF_INTERESTS2 data..Completed")
                logging.info("Reading CRM data..")
                data.getCrmCases()
                logging.info("Reading CRM data..Completed")

                logging.info("Completed getting data from data sources")
                print("Completed getting data from data sources")
                logging.info("Time Taken Normal data: " +str(datetime.datetime.now() - start_time1) )
                print("Time Taken Normal data: " +str(datetime.datetime.now() - start_time1) )

                # GIS data
                start_time1  = datetime.datetime.now()

                logging.info("Reading get_buildingdata..")
                gisdata.get_buildingdata()    
                logging.info("Reading get_buildingdata..Completed")

                logging.info("Reading get_usManholep..")
                gisdata.get_usManholep()    
                logging.info("Reading get_usManholep..Completed")


                logging.info("Reading Population data..")
                gisdata.getPopulationData()    
                logging.info("Reading Population data..Completed")

                logging.info("Reading Priority Areas data..")
                gisdata.getPriorityAreasData()
                logging.info("Reading Priority Areas data..Completed")

                logging.info("Reading getGridZones data..")
                gisdata.getGridZones()
                logging.info("Reading getGridZones data..Completed")

                logging.info("Time Taken GIS: " + str(datetime.datetime.now() - start_time1) )
                print("Time Taken GIS: " + str(datetime.datetime.now() - start_time1) )
                
               

                # Initialize the engine with normale data and gisdata
                start_time1  = datetime.datetime.now()
                logging.info("Initializing General Cleaning engine..")
                print("Initializing General Cleaning engine..")
                engine = GeneralCleaning(data,gisdata)
                logging.info("Initializing General Cleaning engine..Completed")
                print("Initializing General Cleaning engine..Completed")
                
                # Preprocess the data
                logging.info("Preparing data for model run..")
                print("Preparing data for model run")
                engine.PrepareData(Helper)
                logging.info("Preparing data for model run..Completed")
                print("Preparing data for model run..Completed")

                # Run the model
                logging.info("Running the predictions..")
                print("Running the predictions..")
                engine.Process(Helper)
                logging.info("Running the predictions..Completed")
                print("Running the predictions..Completed")

                Helper.Engine_End_Metadata_Update()
                logging.info("General Cleaning Model completed Successfully")
                print("General Cleaning Model completed Successfully")

            except Exception as error:
                print(traceback.print_exc())
                logging.error("Error running the model, Please investigate")
                logging.error(traceback.print_exc())
                Helper.Engine_End_Metadata_Update_Failed()
                print("*****************Error running the model, Please investigate*********************")
                sys.exit(1)
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))
    logging.info("TOTAL TIME: " + str(datetime.datetime.now() - start_time1))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)
          