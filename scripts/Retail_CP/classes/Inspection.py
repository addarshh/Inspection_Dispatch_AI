from abc import ABC, abstractmethod
import copy
from os import path
from typing import Dict
import pandas as pd
import config

from .FileDatabase import fileDatabase

class Inspection(ABC):
    def __init__(self, data : fileDatabase):
        self.data : fileDatabase = copy.copy(data)