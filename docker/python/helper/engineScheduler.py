import os
import datetime as dt
import time
import calendar
import random
import subprocess
from .database import DataBase

class EngineScheduler:

    def __init__(self):
        self.now = None
        self.launched_engines = None
        self.db = DataBase()
        self.engine_list = None
        self.newLaunch()

    def newLaunch (self):
        while 1 == 1:
            self.engine_list = self.getEngineList()
            self.launched_engines = self.db.getLaunchedEngines()
            self.now = dt.datetime.now()
            if self.checkCaseClusteringPermission() == True:
                subprocess.Popen(['python', os.path.join('scripts', 'CaseClustering', 'main.py')],
                                 stdout=open(os.path.join('docker', 'python', 'scheduler', 'log', 'CaseClustering' + '.txt'), 'a'),
                                 close_fds=True, stderr=subprocess.STDOUT, encoding='utf-8')
            if self.checkLaunchPermission():
                finished_engines = self.db.getTodayLaunchedEngines()
                b_set_finished_engines = frozenset(finished_engines)
                engines_to_launch = [item for item in self.engine_list if item not in b_set_finished_engines]
                if len(engines_to_launch) > 0:
                    engine_to_launch = random.choice(engines_to_launch)
                    subprocess.Popen(['python', os.path.join('scripts', engine_to_launch, 'main.py')],
                                     stdout=open(os.path.join('docker','python', 'scheduler', 'log', engine_to_launch + '.txt'), 'w'),
                                     close_fds=True, stderr=subprocess.STDOUT, encoding='utf-8')
            time.sleep(int(os.getenv('CHECK_INTERVAL')))

    def checkLaunchPermission (self):
        if len(self.launched_engines) >= 1:
            counter = 0
            for eng in self.launched_engines:
                max_time_execution = self.db.getEngineMaxExecutionTime(eng['engine_title'])
                if self.now - eng['dt_start'] >= max_time_execution:
                    self.db.updateEngineStatusByRecordId(eng['id'])
                else:
                    counter = counter + 1
            return counter < int(os.getenv('SIMULTANEOUSLY_RUNNING_ENGINES'))
        elif len(self.launched_engines) == 0 and len(self.engine_list) == len(self.db.getTodayFinishedEngines()): return False
        else: return True

    def checkCaseClusteringPermission (self):
        launches = self.db.selectTodayLaunchdCaseClustering()
        return False if len (launches) >= 1 else True

    def getEngineList (self):
        path = os.path.join('/var','www', 'html', 'scripts')
        engine_list = []
        for engine in os.listdir(path):
            if os.path.isdir(os.path.join(path, engine)) \
                    and engine != 'GISdata' \
                    and engine != 'CaseClustering':
                if engine == 'GIS_generator':
                    if self.isLastMonthDay(): engine_list.append(engine)
                else: engine_list.append(engine)
        return engine_list

    def isLastMonthDay(self):
        today = dt.date.today()
        resource = calendar.monthrange(today.year, today.month)
        day = resource[1] - int(os.getenv('GIS_DATE_OFFSET'))
        last_month_day = dt.datetime(today.year, today.month, day)
        return today.strftime('%Y-%m-%d') == last_month_day.strftime('%Y-%m-%d')