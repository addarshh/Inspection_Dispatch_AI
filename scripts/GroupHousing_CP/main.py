import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
import datetime
import traceback
import classes.engines.Helper as Helper
import logging
from classes.GroupHousingCP import clean_grouphousing
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)

if __name__ == "__main__":

    Helper = Helper.Helper()
    if len(config.MODELS_TO_RUN):
        logging.basicConfig(filename="grouphousinglog.txt", level=logging.INFO)
        logging.basicConfig(filename="grouphousinglog.txt", level=logging.ERROR)

        if 'GROUPHOUSING' in config.MODELS_TO_RUN:
            try:
                Helper.Engine_Start_Metadata_Update()
                clean_grouphousing()
                Helper.Engine_End_Metadata_Update()
            except Exception as error:
                logging.error(traceback.print_exc())
                #print(error)
                Helper.Engine_End_Metadata_Update_FAILED()
                print("*****************Error running the model, Please investigate*********************")
                sys.exit(1)
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)
          