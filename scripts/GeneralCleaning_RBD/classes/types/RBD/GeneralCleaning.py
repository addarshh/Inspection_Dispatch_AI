import pandas as pd
from classes.types.RBD.GeneralCleaningRBD import GeneralCleaningRBD
from ...GISDatabase import GISDatabase
from ...Database import Database

class GeneralCleaning(GeneralCleaningRBD):
    def __init__(self, data : Database, gisdata: GISDatabase):
        super().__init__(data,gisdata)
     