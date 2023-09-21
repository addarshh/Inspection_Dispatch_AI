import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
import sys
from classes.Score import Risk_score
from classes import CP_GeneralCleaning
from classes import Database as DB
import warnings
import logging
warnings.filterwarnings("ignore")
from classes.engines import Helper as Help
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
Helper=Help.Helper()

if __name__ == "__main__":
    try:

        Helper.Engine_Start_Metadata_Update()
        logging.basicConfig(filename="General_Cleaning_CP.txt", level=logging.INFO)
        logging.basicConfig(filename="General_Cleaning_CP.txt", level=logging.ERROR)
        logging.info("****************************************************************************************************")
        Cleaned_data, feature_list=CP_GeneralCleaning.featurebuilding_count()
        Final_df=Risk_score(Cleaned_data, feature_list)
        DB.save_final(Final_df, config.OUTPUT_TABLE_NAME)
        Helper.Engine_End_Metadata_Update()

    except Exception as error:
        print(str(error))
        logging.error("Error while running the model, Please investigate",exc_info=1)
        Helper.Engine_End_Metadata_Update_Failed()
        sys.exit(1)
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)