from datetime import datetime
from difflib import SequenceMatcher


def getDateString(format = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now().strftime(format)

def similar(a, b):
        return SequenceMatcher(None, a, b).ratio()

def getStageNameDictionary(stageNames):
    dict_transform={}
    for count1, i in enumerate(stageNames):
        for count2, j in enumerate(stageNames):
            if similar(i,j)>0.8:
                dict_transform[i]=j
                stageNames[count1]=stageNames[count2]
    return dict_transform