import pandas as pd
import cx_Oracle
import config
import os
import os.path as path
import geopandas #as gpd
import logging

## NEW CODE BLOCK HERE
def getLicensesKeysData():
    if not config.DB['instaclientpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclientpath']
    
    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
            
    cursor = conn.cursor()
    sqlQuery = """SELECT D_ACTIVITIES_ID, D_ACTIVITIES_NAME, IS_ENABLE, ACTIVITIE_TYPE_ID, ACTIVITIE_TYPE_NAME, ACTIVITYNO AS "MOMTATHEL ACTIVITY NUMBER" FROM 
    """ + config.DB['input_schema'] + '.MOMTHATEL_DATA'
    cursor.execute(sqlQuery)
    rows = cursor.fetchall()
    licensesKeysDf = pd.DataFrame(rows)
    col_names = [row[0] for row in cursor.description]
    licensesKeysDf.columns = col_names
    # self.licensesKeysDf = self._executeQueryToDf(sqlQuery)
    licensesKeysDf.columns = licensesKeysDf.columns.str.upper()
    return licensesKeysDf
    

def getLicensesData():
    #######SET MODEL NAME##############
    #MODEL_NAME = 'HEALTH RBD ENGINE'
    if not config.DB['instaclientpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclientpath']
    
    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
            
    # if config.DB['connectiontype'] == 'service_name': #STAGE
    #     conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], 
    #     cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
            
    # if config.DB['connectiontype'] == 'SID': #DEV
    #     conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], 
    #     cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    

    cursor = conn.cursor()
    #sql_query = #'SELECT * FROM ' + config.DB['input_schema'] + '.LICENSES_DATA'
    sql_query = """
    SELECT LICENCES_ID "License ID (MOMRAH)", LATITUDE "Latitude", LONGITUDE "Longitude",
    COMMERCIAL_REG_NUMBER "Commercial Reg. Number", NATIONAL_NUMBER "700 - number", 
    FACILITY_TYPE "Facility type", LIST_OF_ACTIVITIES "List of activities", 
    BUSINESS_ACTIVITY "Business activity", BUSINESS_ACTIVITY_WEIGHT "Business activity weight", 
    LICENSE_START_DATE "License Start Date", LICENSE_EXPIRY_DATE "License Expiry Date",
    AREA_OF_THE_RETAIL_OUTLET "Area of the retail outlet", 
    OPERATING_HOURS "Operating hours", TENANCY_OWN_OR_RENTED "Tenancy (Own/Rented)", 
    COMMERCIAL_BUILDING_ID "Commercial Building ID", AMANA "AMANA", 
    MUNICIPALITY "MUNICIPALITY", SUB_MUNICIPALITY "SUB_MUNICIPALITY", 
    DISTRICT_ID "DISTRICT_ID", DISTRICT_NAME "DISTRICT_NAME", 
    LAST_LICENSE_RENEWAL_DATE "Last License renewal date",
    STATUS_ID "STATUS_ID"
    FROM
    """ + config.DB['input_schema'] + '.LICENSES_DATA'
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    Retail_Licenses_medina_df = pd.DataFrame(rows)
    col_names = [row[0] for row in cursor.description]
    Retail_Licenses_medina_df.columns = col_names
    # col_names_new = ['License ID (MOMRAH)',
    # 'Latitude',
    # 'Longitude',
    # 'Commercial Reg. Number',
    # '700 - number',
    # 'Facility type',
    # 'List of activities',
    # 'Business activity',
    # 'Business activity weight',
    # 'License Start Date',
    # 'License Expiry Date',
    # 'Area of the retail outlet',
    # 'Operating hours',
    # 'Tenancy (Own/Rented)',
    # 'Commercial Building ID',
    # 'AMANA',
    # 'MUNICIPALITY',
    # 'SUB_MUNICIPALITY',
    # 'DISTRICT_ID',
    # 'DISTRICT_NAME',
    # 'Last License renewal date']

    # Retail_Licenses_medina_df.columns = col_names_new

    cursor.close()
    print("Retail_Licenses_medina_df")
    print(Retail_Licenses_medina_df.columns)
    return Retail_Licenses_medina_df

def getInspectionsData():
    #######SET MODEL NAME##############
    #MODEL_NAME = 'HEALTH RBD ENGINE'
    if not config.DB['instaclientpath'] in os.environ['PATH']:
        os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclientpath']

    conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'], service_name = config.DB['service'] ))
    cursor = conn.cursor()
    sql_query = """SELECT PZINSKEY "PZINSKEY",LICENSE_NUMBER "LICENSE NUMBER", 
    INSEPECTION_ID "INSEPECTION ID", INSPECTYPE_TYPE_ID "INSPECTYPE ID", 
    INSPECTION_NAME "INSPECTION NAME", ESTABLISHMENT_NAME "Establishment Name", 
    BUSINESS_ACTIVITY_DESCRIPTION "Business Activity Description", 
    STATUS_OF_WORK "Status of Work", TYPE_OF_VISIT "TYPE OF VISIT",
    BUSINESS_ACTIVITY_NUMBER "Business Activity Number", 
    BUSINESS_ACTIVITY_WEIGHT "Business Activity Weight", 
    INSPECTION_DATE "Inspection Date", DEGREE_OF_COMPLIANCE "Degree of Compliance",
    NUMBER_OF_CLAUSES "Number of clauses", NUMBER_OF_COMPLIANT_CLAUSES "Number of compliant clauses", NUMBER_OF_NONCOMPLIANT_CLAUSES "Number of non-compliant clauses", 
    NUMBER_OF_NONCOMPLIANT_CLAUSES_AND_HIGH_RISK "Number of non-compliant clauses and High risk", 
    NUMBER_OF_NONCOMPLIANT_CLAUSES_AND_MEDIUM_RISK "Number of non-compliant clauses and medium risk", 
    ISSUED_FINE_AMOUNT "Issued fine amount", SADAD_NO "SADAD NO", 
    FINE_PAYMENT_STATUS "Fine payment status", SADAD_PAYMENT_DATE "SADAD PAYMENT DATE", 
    INSPECTOR_ACTION "Inspector_Action", APPROVER_CONFISCATION "APPROVER CONFISCATION", 
    APPROVER_FOLLOWUP "APPROVER FOLLOWUP", APPROVER_DESTROY "APPROVER DESTROY", 
    APPROVER_SAMPLE "APPROVER SAMPLE", APPROVER_CLOSE "APPROVER CLOSE", 
    NO_LICENSE "NO LICENSE", SUBMUNICIPALITYID "SUBMUNICIPALITYID", 
    MUNICIPALITYID "MUNICIPALITYID"
    FROM {}.INSPECTION_DATA""".format( config.DB['input_schema'])
    print(sql_query)
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    Inspections_df = pd.DataFrame(rows)
    col_names = [row[0] for row in cursor.description]
    Inspections_df.columns = col_names

    # col_names_inspection_new = ['PZINSKEY',
    # 'LICENSE NUMBER',
    # 'INSEPECTION ID',
    # 'INSPECTYPE ID',
    # 'INSPECTION NAME',
    # 'Establishment Name',
    # 'Business Activity Description',
    # 'Status of Work',
    # 'TYPE OF VISIT',
    # 'Business Activity Number',
    # 'Business Activity Weight',
    # 'Inspection Date',
    # 'Degree of Compliance',
    # 'Number of clauses',
    # 'Number of compliant clauses',
    # 'Number of non-compliant clauses',
    # 'Number of non-compliant clauses and High risk',
    # 'Number of non-compliant clauses and medium risk',
    # 'Issued fine amount',
    # 'SADAD NO',
    # 'Fine payment status',
    # 'SADAD PAYMENT DATE',
    # 'Inspector_Action',
    # 'APPROVER CONFISCATION',
    # 'APPROVER FOLLOWUP',
    # 'APPROVER DESTROY',
    # 'APPROVER SAMPLE',
    # 'APPROVER CLOSE',
    # 'NO LICENSE',
    # 'SUBMUNICIPALITYID',
    # 'MUNICIPALITYID']

    # Inspections_df.columns = col_names_inspection_new

    cursor.close()
    print(Inspections_df.shape)
    print("INSPECTIONS")
    print(Inspections_df.columns)
    return Inspections_df

def getPopulationGrids():
    # gisconn = cx_Oracle.connect(config.DB['gisuser'], config.DB['gispassword'], cx_Oracle.makedsn(config.DB['gishost'], config.DB['gisport'],service_name = config.DB['gisservice']))
    # giscursor = gisconn.cursor()

    # giscursor.execute('SELECT r.OBJECTID OBJECTID, \
    #             r.GRIDUNIQUECODE, \
    #             r.AMANA AMANA,\
    #             r.AMANACODE AMANACODE, \
    #             r.MUNICIPALITY MUNICIPALITY, \
    #             r.MunicipalityCode Municipa_1, \
    #             r.REGION REGION, \
    #             r.DN DN, \
    #             r.GRID_ID GRID_ID, \
    #             SDE.ST_AREA(r.SHAPE) SHAPE_AREA ,  \
    #             SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN,  \
    #             SDE.ST_ASTEXT(r.SHAPE) geometry   \
    #             FROM GISOWNER.GGMUNICIPALITYGRID r WHERE DN > 0')

    # rows = giscursor.fetchall()

    # col_names = [row[0] for row in giscursor.description]

    # population_grids = pd.DataFrame(rows)
    # population_grids.columns = col_names
    # return population_grids

    population_file = path.join(config.GISPATH,'GGINSPECTIONGRIDS.csv')
    #print(population_file)
    shpGrid = pd.read_csv(population_file, dtype ={'municipalitycode':'str', 'amanacode': 'str'})
    shpGrid.columns = map(str.upper, shpGrid.columns)
    shpGrid = shpGrid.rename(columns={'GEOMETRY': 'geometry'})
    shpGrid = geopandas.GeoDataFrame(shpGrid , geometry=geopandas.GeoSeries.from_wkt(shpGrid.geometry, crs = 'epsg:4326'))
    #shpGrid.drop(columns=['GRID_ID.1'], inplace=True)
    shpGrid = shpGrid.rename(columns={ 'GRIDNAME':'GridName','MUNICIPALITY': 'MUNICIPALI','MUNICIPALITYCODE':'MUNICIPA_1'})
    shpGrid = shpGrid.rename(columns={'GRIDNUMBER': 'GridNumber'})

    logging.info("Function:getPopulationData df:shpGrid Shape: {}".format(shpGrid.shape))
    return shpGrid

def getPriorityAreasData():
    # sqlQuery ="""SELECT AREANAME, SDE.ST_ASTEXT(SHAPE) geometry FROM GISOWNER.VDPRIORITYAREAS """
    # self.priorityAreas = self._executeQuery(sqlQuery)
    priorityAreas=pd.read_csv(config.priority_areas, dtype={"geometry":"str"})
    priorityAreas=priorityAreas[["areaname","geometry"]]
    # self.priorityAreas.geometry = self.priorityAreas.geometry.astype('str')
    priorityAreas = geopandas.GeoDataFrame(priorityAreas , geometry=geopandas.GeoSeries.from_wkt(priorityAreas.geometry, crs = 'epsg:4269'))
    priorityAreas.columns = map(str.upper, priorityAreas.columns)
    priorityAreas = priorityAreas.rename(columns={'GEOMETRY': 'geometry'})
    priorityAreas = priorityAreas.rename(columns={'AREANAME': 'Name'})
    return priorityAreas

def get_grid_zone():
    # if not config.DB['instaclientpath'] in os.environ['PATH']:
    #     os.environ['PATH'] = os.environ['PATH'] + ';' + config.DB['instaclientpath']

    # #conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],config.DB['service']  ))
    # conn = cx_Oracle.connect(config.DB['user'], config.DB['password'], cx_Oracle.makedsn(config.DB['host'],config.DB['port'],service_name=config.DB['service'] ))
    # cursor = conn.cursor()
    # #sql_query = """SELECT * FROM "C##MOMRAH".INSPECTION_DATA"""
    
    # sql_query = """SELECT * FROM (SELECT g.*, ROW_NUMBER() OVER (PARTITION BY g.GRIDUNIQUECODE 
    #                     ORDER BY to_number(substr(g.GRID_COVERAGE_PERC,1,LENGTH(g.GRID_COVERAGE_PERC) - 1)) DESC) AS rnk
    #                     FROM "C##MOMRAH".GGGRIDINSPECTIONZONEST g WHERE g.INSPECTIONTYPE=1
    #                     ) WHERE RNK =1"""
    # #zone_data = self._executeQueryToDf(sqlQuery)
    # cursor.execute(sql_query)
    # rows = cursor.fetchall()
    # zone_data = pd.DataFrame(rows)
    # #zone_data = self._executeQuery(sqlQuery)
    # return zone_data

    grid_file =  path.join(config.GISPATH,'GGGRIDINSPECTIONZONEST.csv')
    GridZones = pd.read_csv(grid_file)
    GridZones.columns = map(str.upper, GridZones.columns)
    #self.GridZones.drop(columns=['RNK'], inplace=True)
    GridZones = GridZones.loc[GridZones['INSPECTIONTYPE'] == 1]
    logging.info("Function:priorityAreas df:GridZones Shape: {}".format(GridZones.shape))


    return GridZones

def getAMANA():
    # sqlQuery = \
    #     """SELECT b.OBJECTID, \
    #     b.REGION, \
    #     b.AMANACODE, \
    #     b.AMANAARNAME AMANAARNAM, \
    #     b.AMANAENAME, \
    #     SDE.ST_AREA(b.SHAPE) SHAPE_AREA	, \
    #     SDE.ST_LENGTH(b.SHAPE) SHAPE_LEN, \
    #     SDE.ST_ASTEXT(b.SHAPE) geometry \
    #     FROM GISOWNER.BBAMANABOUNDARYS b  \
    #     """
    # self.AMANA = self._executeQuery(sqlQuery)
    AMANA=pd.read_csv(config.amana_shp_path, dtype={"amanacode":"str"})
    #self.AMANA.geometry = self.AMANA.geometry.astype('str')
    AMANA = geopandas.GeoDataFrame(AMANA , geometry=geopandas.GeoSeries.from_wkt(AMANA.geometry, crs = 'epsg:4326'))
    AMANA.columns  = map(str.upper, AMANA.columns)
    AMANA = AMANA.rename(columns={'GEOMETRY':'geometry'})
    print('self.AMANA.shape')
    print(AMANA.shape)
    return AMANA


