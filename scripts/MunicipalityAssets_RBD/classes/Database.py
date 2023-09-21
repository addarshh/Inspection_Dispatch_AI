import pandas as pd
import config
import os.path as path
from classes.engines import Helper as Help
from abc import ABC
import sqlalchemy as sql
import cx_Oracle
import sys
import geopandas as gpd
Helper=Help.Helper()
class Database(ABC):
    def __init__(self, amanaCode=None):
        self.connectionString = 'oracle+cx_oracle://' + config.DB['user'] + ':' + config.DB['password'] + '@' + cx_Oracle.makedsn(config.DB['host'], config.DB['port'], service_name = config.DB['service'])
        self.connection = None
        self._connect()
        self.crm_data: pd.DataFrame = None
        self.datareq_excavations: pd.DataFrame = None
        self.drilling_inspections: pd.DataFrame = None
        self.amanaCode = amanaCode

    def _connect(self):
        #lib_dir = path.join(path.dirname(__file__), config.DB['instantclient'])
        lib_dir = config.DB['instaclientpath']
        try:
            cx_Oracle.init_oracle_client(lib_dir = lib_dir)

        except Exception as error:
            print("Error handling cx_Oracle.init_oracle_client")
            print(error)
            #sys.exit(1)
            pass

        try:
            engine = sql.create_engine(self.connectionString)
            self.connection = engine.connect()
        except Exception as error:
            print("Error with creating connection")
            print(error)
            #sys.exit(1)
            pass

    def get_crm_data(self):
        sqlQuery = 'SELECT c.PYID "PYID", c.INTERACTIONTYPE "INTERACTIONTYPE", c.PXCREATEDATETIME "PXCREATEDATETIME", c.PYRESOLVEDTIMESTAMP "CLOSURE_DATE" , c.PYSTATUSWORK "SHORT_STATUS" , c.LATITUDE "LATITUDE", c.LONGITUDE "LONGITUDE", c.MAINCLASSIFICATION "MAIN_Classificaion", c.SPLCLASSIFICATION "SP_Classificaion", c.CATEGORY "Category", c.PRIORITY "PRIORITY", c.EXTERNALCONTRACTOR "IS_Contractor", c.RESOLUTIONSATISFYBYPETITIONER "Satisfaction", c.VISUAL_POLLUTION_CATEGORY "VISUAL POLLUTION CATEGORY", SUBMUNICIPALITYID "SUBMUNIC_3" FROM  ' + config.DB['input_schema'] + '.CASES c WHERE c.PYSTATUSWORK <> \'Close\''
        self.crm_data = self._executeQuery(sqlQuery)
        if self.crm_data.shape[0]==0:
            print("CRM data is not available in database")
            exit()
        return self.crm_data

    def get_grid_zone(self):
        # sqlQuery = """SELECT * FROM (SELECT g.*, ROW_NUMBER() OVER (PARTITION BY g.GRIDUNIQUECODE
        #                 ORDER BY to_number(substr(g.GRID_COVERAGE_PERC,1,LENGTH(g.GRID_COVERAGE_PERC) - 1)) DESC) AS rnk
        #                 FROM "C##MOMRAH".GGGRIDINSPECTIONZONEST g WHERE g.INSPECTIONTYPE=4
        #                 ) WHERE RNK =1"""

        # zone_data = self._executeQuery(sqlQuery)
        zone_data=pd.read_csv(config.grid_zone_path)
        zone_data = zone_data[zone_data['inspectiontype'] == 4]
        return zone_data


    def get_licencedata_constructions(self):
        sqlQuery = 'SELECT BUILDING_ID, LICENCES_ID, REQUEST_ID, AMANA, MUNICIPALITY, SUB_MUNICIPALITY, LICENSE_START_DATE , LICENSE_EXPIRY_DATE, LATITUDE, LONGITUDE, CONSULTANT_NAME, CONSULTANT_LICENSE_ID, BUILDING_TYPE, BUILDING_TYPE_ID, BUILDING_MAIN_USE, BUILDING_MAIN_USE_ID, BUILDING_SUB_USE, BUILDING_SUB_USE_ID, PARCEL_AREA, CONTRACTOR, CONTRACTOR_LICENSE_ID, BUILDING_HEIGHT, OWNER_ID FROM  ' + config.DB['input_schema'] + '.LICENSES_DATA_CONSTRUCTIONS'
        self.constr_licenses = self._executeQuery(sqlQuery)
        # cursor_targetDB=self.connection.connection.cursor()
        # self.constr_licenses = gpd.GeoDataFrame.from_records(self.constr_licenses,
        #                                                 columns=[x[0] for x in cursor_targetDB.description])

        if self.constr_licenses.shape[0]==0:
            print(" Licenses data constructions is not available in database")
            exit()
        return self.constr_licenses


    def get_licencedata_retail(self):
        sqlQuery = 'SELECT LICENCES_ID, LATITUDE , LONGITUDE, COMMERCIAL_REG_NUMBER, NATIONAL_NUMBER, FACILITY_TYPE, LIST_OF_ACTIVITIES, BUSINESS_ACTIVITY, BUSINESS_ACTIVITY_WEIGHT, LICENSE_START_DATE, LICENSE_EXPIRY_DATE, AREA_OF_THE_RETAIL_OUTLET, OPERATING_HOURS, TENANCY_OWN_OR_RENTED, COMMERCIAL_BUILDING_ID , AMANA, MUNICIPALITY, SUB_MUNICIPALITY, DISTRICT_ID, DISTRICT_NAME, LAST_LICENSE_RENEWAL_DATE FROM  ' + config.DB['input_schema'] + '.LICENSES_DATA'
        self.dfRetail = self._executeQuery(sqlQuery)
        if self.dfRetail.shape[0]==0:
            print(" Licenses_data retail is not available in database")
            exit()
        return self.dfRetail

    def get_licencedata_excavations(self):
        sqlQuery = 'SELECT LICENCES_ID, LATITUDE , LONGITUDE, LICENSE_START_DATE,LICENSE_EXPIRY_DATE, AMANA, MUNICIPALITY, SUB_MUNICIPALITY,CONTRACTOR_CR,DISTRICT_ID, DISTRICT_NAME FROM  ' + config.DB['input_schema'] + '.LICENSES_DATA_EXCAVATIONS'
        self.excav_licenses = self._executeQuery(sqlQuery)
        if self.excav_licenses.shape[0]==0:
            print(" excavations licenses is not available in database")
            exit()
        return self.excav_licenses

    def get_inspectionsdata_construction(self):
        sqlQuery = 'SELECT  STATUS_OF_WORK "PYSTATUSWORK",LICENSE_NUMBER "LIC_ID",INSEPECTION_ID "INSPECTION_ID",INSPECTION_DATE "INSPECTION_DATE",PHASE_NUMBER "PHASE_NUMBER",PHASE_NAME "PHASE_NAME",STAGENAME "STAGENAME",STAGENO "STAGENO",NUMBEROFFAILEDCLAUSES "NUMBEROFFAILEDCLAUSES",COMPLIANCE "COMPLIANCE",COMPLYINGITEMS "COMPLYINGITEMS",WORKS_STOPPED "WORKS_STOPPED",NO_LICENSE "HASNOLICENSE" FROM  ' + config.DB['input_schema'] + '.INSPECTION_DATA_CONSTRCTIONS'
        self.drilling_inspections = self._executeQuery(sqlQuery)
        self.drilling_inspections.columns = self.drilling_inspections.columns.str.lower()
        if self.drilling_inspections.shape[0]==0:
            print(" inspections data constructions is not available in database")
            exit()
        return self.drilling_inspections

    def get_visualdistortionsinspections(self):
        sqlQuery="""SELECT * FROM %s.INSPECTION_DATA_VISUAL_DISTORTIONS""" % config.DB['input_schema']
        distortions_data=self._executeQuery(sqlQuery)
        distortions_data.columns=distortions_data.columns.str.upper()
        distortions_data.rename(columns={"LICENSE_NUMBER":"LICENSE NUMBER","INSPECTION_DATE":"Inspection Date","INSEPECTION_ID":"INSEPECTION ID","CLAUSE_NAME":"CLAUSE NAME",
        "CLAUSE_NO":"CLAUSE NO", "SUBCLAUS_ENAME":"SUBCLAUS ENAME","SUBCLAUSE_NO":"SUBCLAUSE NO", "CLAUSE_WEIGHT":"CLAUSE WEIGHT","SUBCLAUSE_WEIGHT":"SUBCLAUSE WEIGHT",
        "IS_CLAUSE_VALIDATED_APPROVER":"IS CLAUSE VALIDATED APPROVER","IS_CLAUSE_VALIDATED":"IS CLAUSE VALIDATED","VIOLATION_TYPE":"VIOLATION TYPE","LINK_ID":"LINK ID",
        "SADAD_NO":"SADAD NO","FINE_PAYMENT_STATUS":"Fine payment status","SADAD_PAYMENT_DATE":"SADAD PAYMENT DATE"}, inplace=True)

        return distortions_data

    def _executeQuery(self, sqlQuery : str) -> pd.DataFrame:
        result = self.connection.execute(sqlQuery)
        columnsNames = result.keys()
        result = result.fetchall()
        return pd.DataFrame(result, columns = columnsNames)


def save_final(Final_df, output_table_name):
    Helper.backup(output_table_name)
    # insert_df( df, output_table_name)
    Helper.insert_df_Batchwise(Final_df, output_table_name, 50000)

class shapefiles(ABC):
    def __init__(self):
        pass
        # connectionString = 'oracle+cx_oracle://' + config.DB['gisuser'] + ':' + config.DB['gispassword'] + '@' + cx_Oracle.makedsn(config.DB['gishost'], config.DB['gisport'], service_name = config.DB['gisservice'])
        # engine = sql.create_engine(connectionString)
        # connection = engine.connect()
        # self.cursor_gis = connection.connection.cursor()
        # print("GIS Connection established")

    def get_buildingdata(self):
        # self.cursor_gis.execute(
        # """SELECT OBJECTID, sde.st_astext(b.SHAPE) AS geometry FROM GISOWNER.BUBUILDINGFOOTPRINTS b where rownum<5""")
        # buildingdata = self.cursor_gis.fetchall()
        buildingdata=pd.read_csv(config.buildings_footprint_path, dtype={"geometry":"str"})
        buildingdata=buildingdata[['objectid','geometry']]
        buildingdata.columns=buildingdata.columns.str.upper()
        #buildingdata = gpd.GeoDataFrame.from_records(buildingdata, columns=[x[0] for x in self.cursor_gis.description])
        buildingdata=gpd.GeoDataFrame.from_records(buildingdata, columns=buildingdata.columns)
        return buildingdata

    def get_pavementsdata(self):
        # self.cursor_gis.execute("""SELECT OBJECTID, sde.st_astext(t.SHAPE) as geometry FROM GISOWNER.TNPAVEMENTSS t""")
        # pavementsdata = self.cursor_gis.fetchall()
        pavementsdata=pd.read_csv(config.pavements_path, dtype={"geometry":"str"})
        pavementsdata=pavementsdata[['objectid','geometry']]
        pavementsdata.columns=pavementsdata.columns.str.upper()
        #pavementsdata = gpd.GeoDataFrame.from_records(pavementsdata,columns=[x[0] for x in self.cursor_gis.description])
        pavementsdata=gpd.GeoDataFrame.from_records(pavementsdata,columns=pavementsdata.columns)
        #pavementsdata = gpd.read_file(self.RAW_DATA_PATH + r'\New_shape_files\tnPavementsS.shp')
        return pavementsdata

    def get_publicfacilities(self):

        # self.cursor_gis.execute('''
        # SELECT OBJECTID, sde.st_astext(l.SHAPE) as geometry FROM GISOWNER.LMPUBLICFACILTITIESP l
        # ''')
        # publicfacilities = self.cursor_gis.fetchall()
        publicfacilities=pd.read_csv(config.publicfacilities_path, dtype={"geometry":"str"})
        publicfacilities=publicfacilities[['objectid','geometry']]
        publicfacilities.columns=publicfacilities.columns.str.upper()
        # publicfacilities = gpd.GeoDataFrame.from_records(publicfacilities,
        #                                                  columns=[x[0] for x in self.cursor_gis.description])
        publicfacilities = gpd.GeoDataFrame.from_records(publicfacilities,columns=publicfacilities.columns)
        return publicfacilities

    def get_parksandrec(self):
        # self.cursor_gis.execute('''
        # SELECT OBJECTID, sde.st_astext(l2.SHAPE) as geometry FROM GISOWNER.LMPARKSANDRECREATIONP l2
        # ''')
        # parksandrec = self.cursor_gis.fetchall()
        parksandrec=pd.read_csv(config.parks_path, dtype={"geometry":"str"})
        parksandrec=parksandrec[['objectid','geometry']]
        parksandrec.columns=parksandrec.columns.str.upper()
        # parksandrec = gpd.GeoDataFrame.from_records(parksandrec,
        #                                             columns=[x[0] for x in self.cursor_gis.description])
        parksandrec = gpd.GeoDataFrame.from_records(parksandrec,columns=parksandrec.columns)
        return parksandrec
