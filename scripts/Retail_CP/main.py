import os
import sys
import config
import datetime
if config.DB['DEBUG']:
    start_time = datetime.datetime.now()
import classes.Database as DB
from classes.engines import Helper as Help
from classes.engines.CasePrioritization import CasePrioritization
from classes.types.CP.Retail import Retail as RetailCP
sys.path.insert(0, '/var/www/html')
from docker.python.helper.database import DataBase

eng_name = os.path.basename(os.path.dirname(__file__))
db = DataBase()
eng_launch_id = db.insertRowAboutEngineLaunch(eng_name)
Helper = Help.Helper()
Helper.Engine_Start_Metadata_Update()

if __name__ == "__main__":

    #DB.Engine_Start_Metadata_Update( config.MODEL_NAME)
    if len(config.MODELS_TO_RUN) and len(config.INSPECTION_CATEGORIES):
        engines = {}
        #data = DB.Database(config.AMANA_CODE)
        # Helper = Help.Helper()
        # print("1")
        # Helper.Engine_Start_Metadata_Update()
        # print("2")
        data = DB.Database()
        
        data.getCommonData()
        
        if 'CP' in config.MODELS_TO_RUN:
            data.getCrmCases()
            data.getClassConfig()
            data.getAmanaDataGdf()
            data.getPopulationDataGdf()
            data.getAmanaPopulationOverlay2()
            data.getPriorityAreasData()
            engines['CP'] = CasePrioritization() 

        for engine in config.MODELS_TO_RUN:
            if engine == 'CP':
                
                for inspection in config.INSPECTION_CATEGORIES:

                    inspectionName = globals()[inspection + engine]
                    inspectionInstance = inspectionName(data)
                    
                    inspectionInstance.getFeatures()
                    engines[engine].setInspectionType(inspectionInstance)
                    engines[engine].process()
                    Helper.Engine_End_Metadata_Update()
if config.DB['DEBUG']:
    print('TOTAL TIME: ' + str(datetime.datetime.now() - start_time) + '. Timestamp: ' + str(datetime.datetime.now()))

db.updateEngineStatusByRecordId(eng_launch_id, db.eng_status_SUCCESS)