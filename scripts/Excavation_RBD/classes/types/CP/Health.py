import pandas as pd

from ...FileDatabase import fileDatabase
from ...InspectionCP import InspectionCP


class Health(InspectionCP):
    def __init__(self, data : fileDatabase):
        self.type = 'الرقابة الصحية'
        self.activity_type_id = 1
        self.label = 'health'
        super().__init__(data, self.type, self.activity_type_id)


    def getFeatures(self):
        self._getDNFeature()
        self._getDaysElapsedFeature()
        self._getSatisfactionLevelFeature()
        self._getNumberOfPriorityAreasFeature()
        self._getNumberOfLicensesInVicinityFeature()
        self._getDaysSinceLastInspectionFeature()
        self._getAverageComplianceScore()
        self.prepareFeatureDataset()
        self._outputToFile(self.label)
        print('all fearute suceess')
