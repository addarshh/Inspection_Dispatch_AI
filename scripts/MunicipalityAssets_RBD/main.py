import os
import sys
import traceback
from classes.engines import Helper as Help
import warnings
import logging
import config
import datetime
sys.path.insert(0, '/var/www/html')
from docker.python.helper .database import DataBase
eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
from docker.python.helper.gisDatabase import GisDatabase
GisDatabase()
from classes import assets_modelling as clus

if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
model=clus.clustering()
Helper=Help.Helper()
warnings.filterwarnings('ignore')
logging.basicConfig(filename="Municipality_Assets_RBD.txt", level=logging.INFO)
if __name__ == "__main__":
    Helper.Engine_Start_Metadata_Update()
    try:
        model.buildings_model()
        model.street_model()
        model.lighting_model()
        model.construction_model()
        model.parks_model()
        model.score_generation()
        Helper.Engine_End_Metadata_Update()
    except Exception as error:
        #print(traceback.print_exc())
        logging.error("Error running the model, Please investigate")
        logging.error(traceback.print_exc())
        Helper.Engine_Fail_Metadata_Update()
        print("*****************Error running the model, Please investigate*********************")
        sys.exit(1)
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)