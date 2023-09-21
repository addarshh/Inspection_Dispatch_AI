from abc import ABC, abstractmethod
import copy
from os import path
from typing import Dict
import pandas as pd
import config

from classes.Database import Database
from classes.GISDatabase import GISDatabase

class Inspection(ABC):
    def __init__(self, data : Database, gisdata : GISDatabase):
        self.data : Database = copy.copy(data)
        self.gisdata : GISDatabase = copy.copy(gisdata)