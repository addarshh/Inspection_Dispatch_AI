import pandas as pd
from classes.types.RBD.InspectionRBD import InspectionRBD
from ...GISDatabase import GISDatabase
from ...Database import Database

class InspectorForecasting(InspectionRBD):
    def __init__(self, data : Database, gisdata: GISDatabase):
        super().__init__(data,gisdata)
     