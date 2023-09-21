import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
from classes.Score import Risk_score
from classes import Data_Cleaning
from classes import Database as DB
import warnings
warnings.filterwarnings("ignore")
from classes.engines import Helper as Help
# from classes.types.CP.Health import Health as HealthCP
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
Helper=Help.Helper()

if __name__ == "__main__":
    Helper.Engine_Start_Metadata_Update()
    Cleaned_data, feature_list=Data_Cleaning.Failedcauses_proportion()
    Final_df=Risk_score(Cleaned_data, feature_list)
    DB.save_final(Final_df, config.OUTPUT_TABLE_NAME)
    Helper.Engine_End_Metadata_Update()

if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)