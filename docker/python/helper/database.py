import os
import datetime as dt
import cx_Oracle
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine

class DataBase:

    def __init__(self):
        self.db_string = "oracle+cx_oracle://{}:{}@".format(
            os.getenv('DB_DL_USER'),
            os.getenv('DB_DL_PSWD')) + cx_Oracle.makedsn(
                os.getenv('DB_DL_HOST'),
                os.getenv('DB_DL_PORT'),
                service_name=os.getenv('DB_DL_BASE'))
        self.db = create_engine(self.db_string)
        self.default_execution_time_limit = dt.timedelta(seconds=float(os.getenv('MAX_SCRIPT_EXECUTION_TIME_IN_SECONDS')))
        self.select_start_date = None
        self.eng_status_IN_PROGRESS = 'in_progress'
        self.eng_status_SUCCESS = 'success'
        self.eng_status_ERROR = 'error'

    def getLaunchedEngines(self):
        query = "SELECT * FROM {}.ENGINE_LAUNCH_STATISTICS WHERE SERVER_NAME = '{}' AND ENGINE_LAUNCH_STATUS = '{}'"\
            .format(os.getenv('DB_DL_USER'), os.getenv('APP_SERVER_NAME'), self.eng_status_IN_PROGRESS)
        eng_list = []
        for row in self.db.execute(query):
            eng_list.append(row)
        return eng_list

    def getEngineMaxExecutionTime(self, engine_name):
        self.select_start_date = dt.datetime.strptime(str(dt.date.today() - relativedelta(months=2)), '%Y-%m-%d')
        query = "SELECT MAX(DT_FINISH - DT_START) FROM {}.ENGINE_LAUNCH_STATISTICS " \
                "WHERE ENGINE_TITLE = '{}' AND ENGINE_LAUNCH_STATUS = '{}' AND DT_FINISH IS NOT NULL AND DT_START >= '{}'".format(
            os.getenv('DB_DL_USER'), engine_name, self.eng_status_SUCCESS, self.select_start_date.strftime('%d-%b-%y %I:%M:%S %p'))
        eng_max_time = None
        for row in self.db.execute(query):
            eng_max_time = self.default_execution_time_limit if row[0] is None else row[0] * 1.2 * int(os.getenv('SIMULTANEOUSLY_RUNNING_ENGINES'))
            break
        return eng_max_time

    def updateEngineStatusByRecordId(self, engine_id, status = 'default'):
        if status == 'default': status = self.eng_status_ERROR
        query = "UPDATE {}.ENGINE_LAUNCH_STATISTICS SET ENGINE_LAUNCH_STATUS = '{}'".format(os.getenv('DB_DL_USER'), status)
        if status == self.eng_status_SUCCESS:
            query = query + ", DT_FINISH = '{}' WHERE id = '{}'".format(dt.datetime.now().strftime('%d-%b-%y %I:%M:%S %p'), engine_id)
        else:
            query = query + " WHERE id = '{}'".format(engine_id)
        with self.db.connect() as connection:
            trans = connection.begin()
            connection.execute(query)
            trans.commit()
        return True

    def getTodayLaunchedEngines(self):
        response = []
        query = "SELECT * FROM {}.ENGINE_LAUNCH_STATISTICS WHERE DT_START >= '{}' AND (ENGINE_LAUNCH_STATUS = '{}' OR ENGINE_LAUNCH_STATUS = '{}')"\
            .format(os.getenv('DB_DL_USER'), dt.date.today().strftime('%d-%b-%y %I:%M:%S %p'), self.eng_status_SUCCESS, self.eng_status_IN_PROGRESS)
        for row in self.db.execute(query):
            response.append(row['engine_title'])
        return response

    def selectTodayLaunchdCaseClustering (self):
        query = "SELECT * FROM {}.ENGINE_LAUNCH_STATISTICS WHERE ENGINE_TITLE = '{}' AND DT_START >= '{}'"\
            .format(os.getenv('DB_DL_USER'), 'CaseClustering', dt.date.today().strftime('%d-%b-%y %I:%M:%S %p'))
        return self.db.execute(query).fetchall()

    def getTodayFinishedEngines(self):
        response = []
        query = "SELECT * FROM {}.ENGINE_LAUNCH_STATISTICS WHERE ENGINE_LAUNCH_STATUS = '{}' AND DT_FINISH >= '{}'"\
            .format(os.getenv('DB_DL_USER'), self.eng_status_SUCCESS, dt.date.today().strftime('%d-%b-%y %I:%M:%S %p'))
        for row in self.db.execute(query):
            response.append(row['engine_title'])
        return response

    def insertRowAboutEngineLaunch(self, engine_title):
        moment = dt.datetime.now()
        query = "INSERT INTO {}.ENGINE_LAUNCH_STATISTICS (ENGINE_TITLE, DT_START, ENGINE_LAUNCH_STATUS, SERVER_NAME) VALUES ('{}', '{}', '{}', '{}')"\
            .format(os.getenv('DB_DL_USER'), engine_title, moment.strftime('%d-%b-%y %I:%M:%S %p'), self.eng_status_IN_PROGRESS, os.getenv('APP_SERVER_NAME'))
        with self.db.connect() as connection:
            trans = connection.begin()
            connection.execute(query)
            trans.commit()
        result = self.db.execute("SELECT ID FROM {}.ENGINE_LAUNCH_STATISTICS WHERE ENGINE_TITLE = '{}' AND DT_START = '{}'"
                                 .format(os.getenv('DB_DL_USER'), engine_title, moment.strftime('%d-%b-%y %I:%M:%S %p')))
        for row in result:
            return row[0]

    def exec(self, sql):
        return self.db.execute(sql)