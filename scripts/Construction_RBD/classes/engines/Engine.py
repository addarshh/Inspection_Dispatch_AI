#Parent engine class
import logging
from abc import ABC, abstractmethod
import pandas as pd
import os.path as path

import config
from classes.Inspection import Inspection


class Engine(ABC):

    def __init__(self) -> None:
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
                logging.info("shape of "+engine+self.inspection.label+ str(self.df.shape))
            else:
                print ("File with features doesn't exist, run feature creation script")
                exit()
        else:
            if fileName is not None:
                file_exists = path.exists(path.join(path.dirname(__file__), fileName))

                if file_exists is True:
                    self.df = pd.read_excel(path.join(path.dirname(__file__), fileName))
                    logging.info("shape of " + engine + self.inspection.label + str(self.df.shape))
                else:
                    print ("File with features doesn't exist, run feature creation cript")
                    exit()
            else:
                print ("File name is not provided")
                exit()

    def _outputToFile(self, engine: str):
        self.df.to_excel(path.join(path.dirname(__file__), self.model_score_path, engine, ('file' if self.inspection is None else self.inspection.label) + ".xlsx"), index = False)
        logging.info("shape of feature data of " + engine + self.inspection.label + str(self.df.shape))