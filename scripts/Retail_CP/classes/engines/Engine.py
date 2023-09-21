#Parent engine class
from abc import ABC, abstractmethod
import pandas as pd
import os.path as path

import config
from classes.Inspection import Inspection
from classes.engines import Helper as Help
#as Helper
#from .Helper import Engine_End_Metadata_Update, backup,insert_df_Batchwise
#Helper1 = help.Helper()
class Engine(ABC):

    def __init__(self) -> None:
        self.Helper=Help.Helper()
        #Helper.__init__()
        self.inspection = None
        self.df = None
        self.model_score_path = config.MODEL_SCORING_OUTPUT_FOLDER
        self.feature_creation_path = config.FEATURE_CREATION_OUTPUT_FOLDER
        

    @abstractmethod
    def getScores(self):
        pass

    @abstractmethod
    def scoreModel(self):
        pass

    def setInspectionType(self, inspection: Inspection):
        self.inspection = inspection
        self.df = None

    def _readInput(self, engine: str, fileName: str = None):

        if self.inspection is not None:
            file_exists = path.exists(path.join(path.dirname(__file__),'..', self.feature_creation_path, engine, self.inspection.label + ".xlsx"))
            print(path.join(path.dirname(__file__), '..', self.feature_creation_path, engine, self.inspection.label + ".xlsx"))
            if file_exists is True:
                self.df = pd.read_excel(path.join(path.dirname(__file__), '..', self.feature_creation_path, engine, self.inspection.label + ".xlsx"))
                df = self.df
                #self.Helper.backup(config.OUTPUT_TABLE_NAME)
                #insert_df( df, config.OUTPUT_TABLE_NAME)
                
                #self.Helper.insert_df_Batchwise( df, config.OUTPUT_TABLE_NAME, 50000)
            else:
                print ("File with features doesn't exist, run feature creation cript")
                exit()
        else:
            if fileName is not None:
                file_exists = path.exists(path.join(path.dirname(__file__), fileName))

                if file_exists is True:
                    self.df = pd.read_excel(path.join(path.dirname(__file__), fileName))
                else:
                    print ("File with features doesn't exist, run feature creation cript")
                    exit()
            else:
                print ("File name is not provided")
                exit()

    def _outputToFile(self, engine: str):
        #self.df.to_excel(path.join(path.dirname(__file__), self.model_score_path, engine, ('file' if self.inspection is None else self.inspection.label) + ".xlsx"), index = False)
        df = self.df
        df=pd.merge(df, config.meta_data, on='CaseId',how='left')
        self.Helper.backup(config.OUTPUT_TABLE_NAME)
        df.drop(columns=['geometry'], axis=1, inplace=True)
        # df = df[['CaseId', 'LATITUDE', 'LONGITUDE', 'Priority_Value', 'DN', \
        # #    'days_elapsed', 'satisfaction_level', 'no_of_priority_areas', \
        # #    'no_of_retail_licenses_vicinity', 'avg_compliance_percent', \
        # #    'days_elapsed_last_inspection', 'Priority_Value_score', 'DN_score', \
        # #    'days_elapsed_score', 'satisfaction_level_score', \
        # #    'no_of_priority_areas_score', 'no_of_retail_licenses_vicinity_score', \
        # #    'avg_compliance_percent_score', 'days_elapsed_last_inspection_score', \
        # 'Final_score_Visibility', 'Final_score_Impact', 'Total_score', \
        # 'Total_score_classes', 'OBJECTID', 'REGION', 'AMANACODE', 'AMANAARNAM', \
        # 'AMANAENAME'#, 'SHAPE_AREA', 'SHAPE_LEN', 'index_right' \
        # ]]
        #insert_df( df, config.OUTPUT_TABLE_NAME) 
        self.Helper.insert_df_Batchwise(df, config.OUTPUT_TABLE_NAME, 50000)
        #Engine_End_Metadata_Update()