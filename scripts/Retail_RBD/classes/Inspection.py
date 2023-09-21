from abc import ABC, abstractmethod
import copy
from os import path
from typing import Dict
import pandas as pd
import config

from .Database import Database

class Inspection(ABC):
    def __init__(self, data : Database):
        self.data : Database = copy.copy(data)