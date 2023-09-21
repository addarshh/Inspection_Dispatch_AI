import pandas as pd

from ...FileDatabase import fileDatabase
from ...InspectionCP import InspectionCP


class Retail(InspectionCP):
    def __init__(self, data : fileDatabase):
        self.type = 'الرقابة على الأسواق و الصحية'
        self.activity_type_id = 2
        self.label = 'retail'
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
