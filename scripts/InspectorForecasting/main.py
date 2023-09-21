import os
import sys
import time
import math
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
import os.path as path
import pandas as pd
import classes.Database as DB
import classes.GISDatabase as GDB
#import classes.SqlAlchemy as SQL
import classes.engines.Helper as Helper
from classes.types.RBD.InspectorForecasting import InspectorForecasting
#from classes.engines.Helper import  AddMonths,insert_df, Engine_End_Metadata_Update, backup,insert_df_Batchwise
from abc import ABC
import logging
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)

if __name__ == "__main__":

    Helper = Helper.Helper()
    if len(config.MODELS_TO_RUN):

        if 'INSPECTOR_FORECASTING' in config.MODELS_TO_RUN:
            #try:
                engines = {}
                data = DB.Database()
                gisdata = GDB.GISDatabase()
                Helper.Engine_Start_Metadata_Update()
                # GIS data

                #********Need to change to GIS data ***********#
                data.getPopulationData()    
                gisdata.getPriorityAreasData()
                gisdata.getMUNICIPALITY()
                gisdata.getSUBMUNICIPALITY()

                ####********IMPORTANT*******########
                #### Weather data needs to be updated after 6 months -  29/09/2022
                #### File to Process the updated Weather data weather_api_generation.py, *****it required Internet*****
                data.getWEATHER_2021()
                data.getWEATHER_2022()
                data.getCrmCases()
                data.getConstructionLicenses()
                data.getCommercialLicenses()
                data.getRiskBasedEngineOutputData()
                data.getStreetsInspectorDemand()

                # Initialize the engine with normale data and gisdata
                engine = InspectorForecasting(data,gisdata)

                # Preprocess the data
                engine.PrepareData(Helper)
                engine.Process(Helper)
                Helper.Engine_End_Metadata_Update()

            # except Exception as error:
            #     print(error)
            #     logging.error("Error running the model:" + str(error))
            #     sys.exit(1)
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)
          