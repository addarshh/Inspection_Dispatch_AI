VDPRIORITYAREAS = f"SELECT OBJECTID, \
                    GLOBALID, \
                    AREANAME, \
                    AREA_ID, \
                    AREATYPE, \
                    PRIORITY, \
                    REGION_ID, \
                    AMANA_ID, \
                    MUNICIPALITY_ID, \
                    CITY_ID, \
                    DISTRICT_ID, \
                    DATASOURCE, \
                    SDE.ST_AREA(SHAPE) SHAPE_AREA, \
                    SDE.ST_LENGTH(SHAPE) SHAPE_LEN, \
                    SDE.ST_ASTEXT(SHAPE) geometry \
                    FROM GISOWNER.VDPRIORITYAREAS \
                    where SHAPE is not NULL \
                    "

MUNICIPALITY = f"SELECT  \
                    r.AMANACODE AMANACODE, \
                    r.MUNICIPALITYUNIQUECODE MUNICIPALI, \
                    r.MUNICIPALITYARNAME MUNICIPA_1, \
                    r.MUNICIPALITYENAME MUNICIPA_2,  \
                    r.CATEGORY          CATEGORY, \
                    SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
                    SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN,  \
                    SDE.ST_ASTEXT(r.SHAPE) geometry  \
                    FROM GISOWNER.BBMUNICIBALITY940S r  \
                    where r.CATEGORY = '1' \
                    and SHAPE is not NULL \
                    "

SUBMUNICIPALITY = f"SELECT  \
                    r.AMANACODE AMANACODE, \
                    substr(r.MUNICIPALITYUNIQUECODE, 0,6)  MUNICIPALI, \
                    r.MUNICIPALITYARNAME MUNICIPA_1,  \
                    r.MUNICIPALITYENAME SUBMUNIC_1,  \
                    r.MUNICIPALITYUNIQUECODE SUBMUNIC_3,  \
                    SDE.ST_AREA(r.SHAPE) SHAPE_AREA	,  \
                    SDE.ST_LENGTH(r.SHAPE) SHAPE_LEN ,  \
                    SDE.ST_ASTEXT(r.SHAPE) geometry  \
                    FROM GISOWNER.BBMUNICIBALITY940S r  \
                    where r.CATEGORY = '2' \
                    and SHAPE is not NULL \
                    "
tnStreetLightingP = f"SELECT OBJECTID	OBJECTID,	\
                    BEGINLIFESPAN	    BEGINLIFES,	\
                    ENDLIFESPAN		    ENDLIFESPA,	\
                    VALIDFROM		    VALIDFROM,	\
                    VALIDTO		        VALIDTO,	\
                    STREETNAME_AR		STREETNAME,	\
                    STREETNAME_EN		STREETNA_1,	\
                    STATUS_EN		    STATUS_EN,	\
                    STATUS_AR		    STATUS_AR,	\
                    DATEOFDESIGN		DATEOFDESI,	\
                    DATEOFCONSTRUCTION	DATEOFCONS,	\
                    WIDTH		        WIDTH,	\
                    LENGTH		        LENGTH,	\
                    HEIGHT		        HEIGHT,	\
                    INSERVICEDATE		INSERVICED,	\
                    OUTSERVICEDATE		OUTSERVICE,	\
                    REGION_ID		    REGION_ID,	\
                    AMANA_ID		    AMANA_ID,	\
                    SECTOR_ID		    SECTOR_ID,	\
                    GOVERNORATE_ID		GOVERNORAT,	\
                    CITY_ID		        CITY_ID,	\
                    MUNICIPALITY_ID		MUNICIPALI,	\
                    SUBMUNICIPALITY_ID	SUBMUNICIP,	\
                    DISTRICT_ID		    DISTRICT_I,	\
                    DATASOURCE		    DATASOURCE,	\
                    DESCRIPTION		    DESCRIPTIO,	\
                    STREETLIGHTINGNAME_AR		STREETLIGH,	\
                    STREETLIGHTINGNAME_EN		STREETLI_1,	\
                    STREETLIGHTING_ID		STREETLI_2,	\
                    STREETLIGHTINGTYPE		STREETLI_3,	\
                    ARMSTATUS		    ARMSTATUS,	\
                    LAMPTYPE		    LAMPTYPE,	\
                    LAMPCOUNT		    LAMPCOUNT,	\
                    LAMPSTATUS		    LAMPSTATUS,	\
                    MATERIAL		    MATERIAL,	\
                    MANUFACTURER		MANUFACTUR, 	\
                    STREETLIGHTING_UNIFIED_ID		STREETLI_4,	 \
                    GLOBALID		    GLOBALID,	 \
                    DATAOWNER		    DATAOWNER,	 \
                    CREATED_USER		CREATED_US,	  \
                    CREATED_DATE		CREATED_DA,	  \
                    LAST_EDITED_USER	LAST_EDITE,	  \
                    LAST_EDITED_DATE	LAST_EDI_1,	 \
                    AMANATRACKING		AMANATRACK,	\
                    AMANABALADI		    AMANABALAD,	\
                    MUNICIPALITYBALADI	MUNICIPA_1, \
                    DISTRICTBALADI		DISTRICTBA,  \
                    STREETNO		    STREETNO, \
                    SDE.ST_ASTEXT(SHAPE) geometry \
                    FROM GISOWNER.tnStreetLightingP \
                    where SHAPE is not NULL \
                    "
BBAMANABOUNDARYS = f"SELECT b.OBJECTID, \
                    b.REGION, \
                    b.AMANACODE, \
                    b.AMANAARNAME AMANAARNAM, \
                    b.AMANAENAME, \
                    SDE.ST_AREA(b.SHAPE) SHAPE_AREA	, \
                    SDE.ST_LENGTH(b.SHAPE) SHAPE_LEN, \
                    SDE.ST_ASTEXT(b.SHAPE) geometry \
                    FROM GISOWNER.BBAMANABOUNDARYS b  \
                    where SHAPE is not NULL \
                    "
usManholeP = f"SELECT  \
                    u.OBJECTID OBJECTID, \
                    u.AMANA_ID AMANACODE, \
                    u.GROUNDELEVATION, \
                    u.LABEL, \
                    u.INVERTLEVEL1, \
                    SDE.ST_AREA(u.SHAPE) SHAPE_AREA	, \
                    SDE.ST_LENGTH(u.SHAPE) SHAPE_LEN,  \
                    SDE.ST_ASTEXT(u.SHAPE) geometry  \
                    FROM GISOWNER.USMANHOLEP u  \
                    where SHAPE is not NULL \
                    "

buBuildingFootPrintS = f"SELECT  \
                        u.OBJECTID OBJECTID, \
                        u.BUILDINGNUMBER B_NO,\
                        u.BUILDINGTYPE \"TYPE\",\
                        u.STREET_ID STREET_NO,\
                        u.BUILDINGOCCUPATION BEL_CODE, \
                        u.DATASOURCE \"SOURCE\", \
                        u.LICENSENUMBER LCNNO, \
                        u.CREATED_DATE INSERT_DAT, \
                        u.LAST_EDITED_DATE UPDATE_DATE,\
                        SDE.ST_AREA(u.SHAPE) Shape_Area,\
                        SDE.ST_LENGTH(u.SHAPE) Shape_Leng,\
                        SDE.ST_ASTEXT(u.SHAPE) geometry  \
                        FROM GISOWNER.buBuildingFootPrintS u \
                        where \
                        u.REGION_ID is not null \
                        AND u.MUNICIPALITY_ID is not null\
                        AND SHAPE is not NULL \
                        "
tnRoadCenterLineL = f"SELECT  \
                        u.OBJECTID OBJECTID, \
                        u.AMANA_ID AMANA_ID, \
                        u.DATEOFCONSTRUCTION , \
                        u.DATEOFDESIGN , \
                        u.INSERVICEDATE , \
                        u.OUTSERVICEDATE , \
                        u.\"LENGTH\" RoadLength, \
                        u.WIDTH RoadWidth,\
                        u.REGION_ID , \
                        u.MUNICIPALITY_ID MUNICIPALI,\
                        u.SUBMUNICIPALITY_ID , \
                        u.DESCRIPTION  , \
                        u.ROADCENTERLINETYPE , \
                        u.ROADCENTERLINENAME_AR , \
                        u.ROADCENTERLINENAME_EN , \
                        u.ROADCENTERLINE_ID , \
                        u.ROADCENTERLINESTATUS , \
                        u.ROADNO , \
                        SDE.ST_AREA(u.SHAPE) SHAPE_AREA	, \
                        SDE.ST_LENGTH(u.SHAPE) SHAPE_LEN,  \
                        SDE.ST_ASTEXT(u.SHAPE) geometry  \
                        FROM GISOWNER.tnRoadCenterLineL u \
                        where \
                        u.REGION_ID is not null \
                        AND u.MUNICIPALITY_ID is not null \
                        AND SHAPE is not NULL \
                        "
tnPavementsS = f"SELECT  \
                        u.OBJECTID OBJECTID,  \
                        u.AMANA_ID AMANACODE, \
                        u.PAVEMENT_ID, \
                        u.DATASOURCE, \
                        u.REGION_ID , \
                        u.CITY_ID , \
                        u.MUNICIPALITY_ID , \
                        u.STREET_ID, \
                        u.STREETNO , \
                        u.PAVEMENTNO , \
                        u.PAVEMENTNAME_AR , \
                        u.PAVEMENTNAME_EN , \
                        u.WIDTH , \
                        u.STATUS_AR, \
                        u.STATUS_EN , \
                        u.LIGHTED , \
                        u.CONDITION_AR , \
                        u.CONDITION_EN , \
                        SDE.ST_AREA(u.SHAPE) SHAPE_AREA	, \
                        SDE.ST_LENGTH(u.SHAPE) SHAPE_LEN,   \
                        SDE.ST_ASTEXT(u.SHAPE) geometry  \
                        FROM GISOWNER.tnPavementsS u   \
                        where \
                        u.REGION_ID is not null \
                        AND u.MUNICIPALITY_ID is not null \
                        AND SHAPE is not NULL \
                        "

lcBaladyBuildinglicenseP = f"SELECT \
                            u.AMMANA_NAME	AMMANA_NAME, \
                            u.BALADIA_NAME	BALADIANAME, \
                            u.LICENSENO	LICENSE_ID, \
                            u.CREATED_DATE	CREATE_DATE, \
                            u.OWNERIDENTIFIERTYPENAME	FULL_NAME, \
                            u.OWNERMOBILENUMBER	MOBILENO, \
                            u.MAIN_OCCUPANCY_CATEGORIES	MAIN_BUILDING, \
                            u.SUB_CATEGORIES_OF_OCCUPANCY	SUB_BUILDING, \
                            u.DISTRICTNAME	DISTRICTNAME, \
                            u.COORDINATE_X	X, \
                            u.COORDINATE_Y	Y, \
                            u.LICENSEISSUEDATEH, \
                            u.LICENSEEXPIRYDATE2,\
                            SDE.ST_AREA(u.SHAPE) SHAPE_AREA	,\
                            SDE.ST_LENGTH(u.SHAPE) SHAPE_LEN,\
                            SDE.ST_ASTEXT(u.SHAPE) geometry\
                            FROM GISOWNER.lcBaladyBuildinglicenseP u\
                            where SHAPE is not NULL \
                            "

LCBALADYDIGGINGLICENSEL = f"SELECT  \
                            OBJECTID,\
                            BLICENSENO,\
                            CLOSENEED,\
                            DIGDATE,\
                            ENDPOINT,\
                            OUTFITDATE,\
                            PATHDEPTH,\
                            PATHLENGTH,\
                            PATHSIGN,\
                            PATHSUM,\
                            PATHWIDTH,\
                            PERMISSION,\
                            PLANENO,\
                            ROADSCRAP,\
                            SAKNO,\
                            SERVTYPE,\
                            SITESSUM,\
                            STARTPOINT,\
                            STARTUPTIM,\
                            STREETNAME,\
                            STREETW,\
                            TENTNO,\
                            WORKPERIOD,\
                            WORKTYPE,\
                            PREPARINGT,\
                            WSITENAME,\
                            WORKSITEID,\
                            DRILMODEID,\
                            NETTYPEID,\
                            NETNAME,\
                            WORKNATURE,\
                            DSTATUSID,\
                            DSTATUSN,\
                            SCLOSETYPE,\
                            SCLOSEID,\
                            PATHID,\
                            ISAPPROVED,\
                            DRILLMODE,\
                            TOOLSDATE,\
                            TOOLSENTRY,\
                            ID,\
                            ROADNATURE,\
                            ROADTYPE,\
                            CREATED_USER,\
                            CREATED_DATE,\
                            LAST_EDITED_USER,\
                            LAST_EDITED_DATE,\
                            SOURCEID,\
                            SOURCENAME,\
                            PATHINDEX,\
                            FRIENDLYPATHID,\
                            FRIENDLYWORKSITEID,\
                            LICENSE_ID,\
                            REQUEST_ID,\
                            WORK_SITE_ID,\
                            AMMANA,\
                            BALADIA,\
                            DISTRICT_NAME,\
                            STREET_NAME,\
                            \"LENGTH\",\
                            WIDTH,\
                            \"DEPTH\",\
                            ROAD_TYPE,\
                            STREET_CLOSE_TYPE,\
                            IS_TRAFFIC_PLAN_REQUIRED,\
                            START_POINT_X,\
                            START_POINT_Y,\
                            END_POINT_X,\
                            END_POINT_Y,\
                            PATH_CODE,\
                            MUNICIPALITYARNAME,\
                            MUNICIPALITYUNIQUECODE,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA	,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.LCBALADYDIGGINGLICENSEL\
                            where SHAPE is not NULL \
                            "

LUPARKS = f"SELECT \
                            OBJECTID,\
                            CREATED_USER,\
                            CREATED_DATE,\
                            LAST_EDITED_USER,\
                            LAST_EDITED_DATE,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            REGION_ID,\
                            SECTOR_ID,\
                            AMANA_ID,\
                            GOVERNORATE_ID,\
                            MUNICIPALITY_ID,\
                            CITY_ID,\
                            DISTRICT_ID,\
                            SUBDIVISIONPLAN_ID,\
                            BLOCK_ID,\
                            PARCEL_ID,\
                            PARKSANDRECREATION_ID,\
                            PARKSANDRECREATIONTYPE,\
                            PARKSANDRECREATIONNAME_AR,\
                            PARKSANDRECREATIONNAME_EN,\
                            OWNER,\
                            SCHEDULEOFMAINTENANCEANDFOLLOW,\
                            IRRIGATIONSCHEDULE,\
                            QUANTITIESTABLES,\
                            STADIUMCAPACITY,\
                            PARKSTATUS,\
                            PROJECTNAME,\
                            PARKTYPE,\
                            PARKING,\
                            WC,\
                            COURT,\
                            IRRIGATIONSYSTEM,\
                            IRRIGATIONSTATUS,\
                            FENCE,\
                            PLANTATION,\
                            MAINTENANCE,\
                            DATEOFMAINTENANCE,\
                            INVESTED,\
                            BUILT,\
                            OFFICIALAREA,\
                            PUBLICFACILITES,\
                            STREET_ID,\
                            SERVICESTYPEDESCRIPTION,\
                            PARKSANDRECREATION_UNIFIED_ID,\
                            GLOSSARYID,\
                            PARKCATEGORY,\
                            ROADCENTERLINENAME_AR,\
                            ROADCENTERLINENAME_EN,\
                            REALESTATEID,\
                            SUBDIVISIONPLANNAME,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.LUPARKS \
                            where SHAPE is not NULL \
                            "

tnBridgesL = f"SELECT \
                            OBJECTID,\
                            BRIDGENAME_AR,\
                            BRIDGENAME_EN,\
                            RIGHTOFWAY,\
                            CLEARANCE,\
                            MAXSPEED,\
                            NOOFEXIT,\
                            NOOFLANES,\
                            LIFECYCLESTATUS,\
                            REGION_ID,\
                            AMANA_ID,\
                            SECTOR_ID,\
                            GOVERNORATE_ID,\
                            CITY_ID,\
                            MUNICIPALITY_ID,\
                            SUBMUNICIPALITY_ID,\
                            DISTRICT_ID,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            BRIDGES_ID,\
                            BRIDGEHEIGHT,\
                            DATEOFLASTINSPECTION,\
                            DATEOFINSPECTION,\
                            DATEOFDESIGN,\
                            DATEOFCONSTRUCTION,\
                            INSERVICEDATE,\
                            OUTSERVICEDATE,\
                            MAINTENANCERESPONSIBILITY,\
                            DRAWINGNAME,\
                            DRAWINGNO,\
                            STREET_ID,\
                            BRIDGEWIDTH,\
                            BRIDGELEVEL,\
                            LIGHTED,\
                            BRIDGETYPE_AR,\
                            \"LENGTH\",\
                            MATERIAL,\
                            BRIDGES_UNIFIED_ID,\
                            GLOBALID,\
                            DATAOWNER,\
                            CREATED_USER,\
                            CREATED_DATE,\
                            LAST_EDITED_USER,\
                            LAST_EDITED_DATE,\
                            AMANATRACKING,\
                            AMANABALADI,\
                            MUNICIPALITYBALADI,\
                            DISTRICTBALADI,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.tnBridgesL \
                            where SHAPE is not NULL \
                            "
tnParkingAreaS = f"SELECT \
                            OBJECTID,\
                            STREETNAME_AR,\
                            STREETNAME_EN,\
                            REGION_ID,\
                            AMANA_ID,\
                            SECTOR_ID,\
                            GOVERNORATE_ID,\
                            CITY_ID,\
                            MUNICIPALITY_ID,\
                            SUBMUNICIPALITY_ID,\
                            DISTRICT_ID,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            ACTUALMAINLANDUSE,\
                            ACTUALSUBLANDUSE,\
                            PARKINGCATEGORY,\
                            ACTUALDETAILLANDUSE,\
                            OWNERSHIPOFPARKING,\
                            PARKINGCAPACITY,\
                            PARKINGCOMPUTEDCAPACITY,\
                            PARKINGFEE,\
                            PARKINGFEEVALUE,\
                            AGEOFCONSTRUCTION,\
                            AREAUTM,\
                            OPERATIONALDAYS,\
                            OPERATIONALHOURS,\
                            SUBTYPEOFPARKING,\
                            BUILDINGREGULATION,\
                            CAPACITYBUILDINGREGULATION,\
                            REALCAPACITY,\
                            CAPACITYDATASOURCE,\
                            PARKINGAREANAME_AR,\
                            PARKINGAREANAME_EN,\
                            PARKINGAREA_ID,\
                            STREET_ID,\
                            DATEOFLASTINSPECTION,\
                            DATEOFINSPECTION,\
                            DATEOFDESIGN,\
                            DATEOFCONSTRUCTION,\
                            INSERVICEDATE,\
                            OUTSERVICEDATE,\
                            DRAWINGNAME,\
                            DRAWINGNO,\
                            PARKINGAREASURFACETYPE_AR,\
                            PARKINGAREASURFACETYPE_EN,\
                            PARKINGAREACONDITION_AR,\
                            LIGHTED,\
                            PARKINGAREASTATUS_AR,\
                            PARKINGAREA_UNIFIED_ID,\
                            GLOBALID,\
                            DATAOWNER,\
                            CREATED_USER,\
                            CREATED_DATE,\
                            LAST_EDITED_USER,\
                            LAST_EDITED_DATE,\
                            AMANATRACKING,\
                            AMANABALADI,\
                            MUNICIPALITYBALADI,\
                            DISTRICTBALADI,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.tnParkingAreaS\
                            where SHAPE is not NULL \
                            "
tnTrafficLightP = f"SELECT \
                            OBJECTID,\
                            BEGINLIFESPAN,\
                            ENDLIFESPAN,\
                            VALIDFROM,\
                            VALIDTO,\
                            STREETNAME_AR,\
                            STREETNAME_EN,\
                            STATUS_EN,\
                            STATUS_AR,\
                            DATEOFDESIGN,\
                            DATEOFCONSTRUCTION,\
                            WIDTH,\
                            \"LENGTH\",\
                            HEIGHT,\
                            INSERVICEDATE,\
                            OUTSERVICEDATE,\
                            REGION_ID,\
                            AMANA_ID,\
                            SECTOR_ID,\
                            GOVERNORATE_ID,\
                            CITY_ID,\
                            MUNICIPALITY_ID,\
                            SUBMUNICIPALITY_ID,\
                            DISTRICT_ID,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            TRAFFICLIGHTNAME_AR,\
                            TRAFFICLIGHTNAME_EN,\
                            TRAFFICLIGHT_ID,\
                            TRAFFICLIGHTTYPE,\
                            MANUFACTUREMANUAL,\
                            INTERSECTIONNAME,\
                            STREET_ID,\
                            STREETNUMBER,\
                            TRAFFICLIGHT_UNIFIED_ID,\
                            GLOBALID,\
                            DATAOWNER,\
                            CREATED_USER,\
                            CREATED_DATE,\
                            LAST_EDITED_USER,\
                            LAST_EDITED_DATE,\
                            AMANATRACKING,\
                            AMANABALADI,\
                            MUNICIPALITYBALADI,\
                            DISTRICTBALADI,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.tnTrafficLightP\
                            where SHAPE is not NULL \
                            "
usWastewaterPipeL = f"SELECT \
                            OBJECTID,\
                            STATUS_AR,\
                            DRAWINGNAME,\
                            DIAMETER,\
                            ELEVATION,\
                            WASTEWATERPIPE_ID,\
                            WASTEWATERPIPETYPE,\
                            REGION_ID,\
                            SECTOR_ID,\
                            AMANA_ID,\
                            GOVERNORATE_ID,\
                            MUNICIPALITY_ID,\
                            SUBMUNICIPALITY_ID,\
                            CITY_ID,\
                            DISTRICT_ID,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            STATUS_EN,\
                            WASTEWATERPIPE_UNIFIED_ID,\
                            GLOBALID,\
                            DATAOWNER,\
                            CREATED_USER,\
                            CREATED_DATE,\
                            LAST_EDITED_USER,\
                            LAST_EDITED_DATE,\
                            AMANATRACKING,\
                            AMANABALADI,\
                            MUNICIPALITYBALADI,\
                            DISTRICTBALADI,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.usWastewaterPipeL \
                            where SHAPE is not NULL \
                            "
LMPUBLICFACILTITIESP =      f"SELECT OBJECTID, \
                            sde.st_astext(SHAPE) as geometry \
                            FROM GISOWNER.LMPUBLICFACILTITIESP \
                            where SHAPE is not NULL \
                            "
GGGRIDINSPECTIONZONEST =    f"SELECT GRIDUNIQUECODE,GUID,INSPECTIONTYPE \
                             FROM ( \
                             SELECT g.GRIDUNIQUECODE, g.GUID ,g.INSPECTIONTYPE, \
                             ROW_NUMBER() OVER (PARTITION BY g.GRIDUNIQUECODE,g.INSPECTIONTYPE \
                             ORDER BY to_number(substr(g.GRID_COVERAGE_PERC,1,LENGTH(g.GRID_COVERAGE_PERC) - 1)) DESC) AS rnk \
                             FROM GISOWNER.GGGRIDINSPECTIONZONEST g \
                             ) WHERE RNK =1 \
                             "

USSOLIDWASTEDUMPP =    f"SELECT OBJECTID,\
                            CAPACITY,\
                            STATUS,\
                            NOOFWORKER,\
                            OWNER,\
                            OPERATIONDATE,\
                            ENDDATE,\
                            SOLIDWASTEDUMPNAME_AR,\
                            SOLIDWASTEDUMPNAME_EN,\
                            SOLIDWASTEDUMP_ID,\
                            SOLIDWASTEDUMPTYPE,\
                            REGION_ID,\
                            SECTOR_ID,\
                            AMANA_ID,\
                            GOVERNORATE_ID,\
                            MUNICIPALITY_ID,\
                            SUBMUNICIPALITY_ID,\
                            CITY_ID,\
                            DISTRICT_ID,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            REGISTEREDAREA,\
                            ORGANICWASTEQUANTITY,\
                            NONORGANICWASTEQUANTITY,\
                            BUILDINGWASTEQUANTITY,\
                            AGRICULTUREWASTEQUANTITY,\
                            OTHERWASTEQUANTITY,\
                            COMPLYTOENVIRONMENTSPECIFICATI,\
                            SOLIDWASTEDUMPCODE,\
                            LICENSEISEXPIRED,\
                            NOVIOLATION,\
                            CONTRACTNUMBER,\
                            SOLIDWASTEDUMP_UNIFIED_ID,\
                            GLOBALID,\
                            DATAOWNER,\
                            CREATED_USER,\
                            CREATED_DATE,\
                            LAST_EDITED_USER,\
                            LAST_EDITED_DATE,\
                            AMANATRACKING,\
                            AMANABALADI,\
                            MUNICIPALITYBALADI,\
                            DISTRICTBALADI,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.USSOLIDWASTEDUMPP \
                            where SHAPE is not NULL \
                             "
INBALADYINVESTMENTLOCATIONP =f"SELECT OBJECTID,\
                            REGION_ID,\
                            SECTOR_ID,\
                            AMANA_ID,\
                            GOVERNORATE_ID,\
                            MUNICIPALITY_ID,\
                            CITY_ID,\
                            DISTRICT_ID,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            CREATED_DATE,\
                            CREATED_USER,\
                            LAST_EDITED_DATE,\
                            LAST_EDITED_USER,\
                            BSINVESTMENTASSES_UNIFIED_ID,\
                            GLOBALID,\
                            DATAOWNER,\
                            AMANATRACKING,\
                            AMANABALADI,\
                            MUNICIPALITYBALADI,\
                            DISTRICTBALADI,\
                            INVESTMENTASSEST_ID,\
                            INVESTMENTASSESTNAME_AR,\
                            INVESTMENTASSESTNAME_EN,\
                            INVESTMENTASSESTTYPE,\
                            DATEOFSITEREGISTRATION,\
                            ASSETSAREA,\
                            OWNERTYPE,\
                            RENTINGSTATUS,\
                            STATUS,\
                            INFRINGEMENTS,\
                            CONTRACTNUMBER,\
                            CONTRACTVALUE,\
                            INVESTMENTYEAR,\
                            DATEOFCONTRACTSTART,\
                            DATEOFCONTRACTEND,\
                            ACTIVITYASCONTRACT,\
                            MAINACTIVITY_ID,\
                            SUBACTIVITY_ID,\
                            NEIGHBORACTVTY,\
                            INVESTORTYPE,\
                            INVESTORIDTYPE,\
                            INVESTORIDNO,\
                            INVESTORNAME,\
                            NOOFBILLBOARDSIDES,\
                            BILLBOARDHEIGHT,\
                            BILLBOARDWIDTH,\
                            TOWERNO,\
                            TOWERTYPE,\
                            STREETNAME,\
                            NOTE,\
                            ASSETSSOURCE,\
                            ENTRYMODE,\
                            INFRINGEMENTFLAG,\
                            CALLGISSTATUS,\
                            INVESTMENTCONTRACTTYPE,\
                            MAINACTIVITY,\
                            SUBACTIVITY,\
                            DISTINCTIVE_MARK,\
                            DATEOFASSETS,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.INBALADYINVESTMENTLOCATIONP \
                            where SHAPE is not NULL \
                             "
INBALADYINVESTMENTASSETSS = f"SELECT OBJECTID,\
                            REGION_ID,\
                            SECTOR_ID,\
                            AMANA_ID,\
                            GOVERNORATE_ID,\
                            MUNICIPALITY_ID,\
                            CITY_ID,\
                            DISTRICT_ID,\
                            DATASOURCE,\
                            DESCRIPTION,\
                            CREATED_DATE,\
                            CREATED_USER,\
                            LAST_EDITED_DATE,\
                            LAST_EDITED_USER,\
                            BSINVESTMENTASSES_UNIFIED_ID,\
                            GLOBALID,\
                            DATAOWNER,\
                            AMANATRACKING,\
                            AMANABALADI,\
                            MUNICIPALITYBALADI,\
                            DISTRICTBALADI,\
                            INVESTMENTASSEST_ID,\
                            INVESTMENTASSESTNAME_AR,\
                            INVESTMENTASSESTNAME_EN,\
                            INVESTMENTASSESTTYPE,\
                            DATEOFSITEREGISTRATION,\
                            ASSETSAREA,\
                            OWNERTYPE,\
                            RENTINGSTATUS,\
                            STATUS,\
                            INFRINGEMENTS,\
                            CONTRACTNUMBER,\
                            CONTRACTVALUE,\
                            INVESTMENTYEAR,\
                            DATEOFCONTRACTSTART,\
                            DATEOFCONTRACTEND,\
                            ACTIVITYASCONTRACT,\
                            MAINACTIVITY_ID,\
                            SUBACTIVITY_ID,\
                            NEIGHBORACTVTY,\
                            INVESTORTYPE,\
                            INVESTORIDTYPE,\
                            INVESTORIDNO,\
                            INVESTORNAME,\
                            NOOFBILLBOARDSIDES,\
                            BILLBOARDHEIGHT,\
                            BILLBOARDWIDTH,\
                            TOWERNO,\
                            TOWERTYPE,\
                            STREETNAME,\
                            NOTE,\
                            ASSETSSOURCE,\
                            ENTRYMODE,\
                            INFRINGEMENTFLAG,\
                            CALLGISSTATUS,\
                            INVESTMENTCONTRACTTYPE,\
                            MAINACTIVITY,\
                            SUBACTIVITY,\
                            DISTINCTIVE_MARK,\
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.INBALADYINVESTMENTASSETSS \
                            where SHAPE is not NULL \
                             "

AACITYBOUNDARYS              = f"SELECT OBJECTID, \
                            DATASOURCE, \
                            DESCRIPTION, \
                            CITY_ID, \
                            CITYCODE, \
                            CITYTYPE, \
                            CITYNAME_AR, \
                            CITYNAME_EN, \
                            REGION_ID, \
                            SECTOR_ID, \
                            AMANA_ID, \
                            GOVERNORATE_ID, \
                            MUNICIPALITY_ID, \
                            CITYGRADE, \
                            CITYAREA, \
                            OFFICIALAREA, \
                            CITYBOUNDARY_UNIFIED_ID, \
                            GLOBALID, \
                            CITYCLASS, \
                            DATAOWNER, \
                            CREATED_USER, \
                            CREATED_DATE, \
                            LAST_EDITED_USER, \
                            LAST_EDITED_DATE, \
                            AMANATRACKING, \
                            AMANABALADI, \
                            MUNICIPALITYBALADI, \
                            SDE.ST_AREA(SHAPE) SHAPE_AREA,\
                            SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
                            SDE.ST_ASTEXT(SHAPE) geometry\
                            FROM GISOWNER.AACITYBOUNDARYS \
                            where SHAPE is not NULL \
                            "
GGINSPECTIONGRIDS = f"SELECT \
                    g.GRIDNUMBER GRIDUNIQUECODE, \
                    g.AMANABALADIARNAME AMANA, \
                    g.AMANABALADI AMANACODE, \
                    g.GRID_ID, \
                    g.GRIDNAME, \
                    g.MUNICIPALITYBALADIARNAME MUNICIPALITY, \
                    g.MUNICIPALITYBALADI MUNICIPALITYCODE, \
                    g.REGIONBALADIARNAME REGION, \
                    g.REGIONBALADI REGIONCODE, \
                    g.DN, \
                    SDE.ST_AREA(g.SHAPE) SHAPE_AREA	, \
                    SDE.ST_LENGTH(g.SHAPE) SHAPE_LEN, \
                    SDE.ST_ASTEXT(g.SHAPE) geometry \
                    FROM GISOWNER.GGINSPECTIONGRIDS g  \
                    where SHAPE is not NULL"
#********************************************************#
#******* New Tables requested by Kearney ****************#
#********************************************************#

# WATER_POINT             = f"SELECT OBJECTID,\
#                             CUST_NAME,\
#                             CUST_ID_TYPE,\
#                             CUST_ID_NUMBER,\
#                             ACCOUNT_NO,\
#                             REGION,\
#                             SMART_METER,\
#                             METER_ID,\
#                             METER_STATUS,\
#                             METER_DIAMETER,\
#                             CUST_CLASS,\
#                             X_GOOGLE_COORDINATES,\
#                             Y_GOOGLE_COORDINATES,\
#                             DISTRICT,\
#                             X_COORDINATES,\
#                             Y_COORDINATES,\
#                             SDE.ST_AREA(SHAPE) SHAPE_AREA,\
#                             SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
#                             SDE.ST_ASTEXT(SHAPE) geometry\
#                             FROM Water.WATER_POINT \
#                             where SHAPE is not NULL \
#                             "
# ELECTPOINT              = f"SELECT OBJECTID,\
#                             PROVINCE_NAME,\
#                             DEPARTMENT_NAME,\
#                             OFFICE_NAME,\
#                             INSTALLATION_NUMBER,\
#                             PREMISE_NUMBER,\
#                             METER_NUMBER,\
#                             SUBSCRIPTION_NO,\
#                             OWNER_NAME,\
#                             OWNER_CONTRACT_ACCOUNT,\
#                             OWNER_ID,\
#                             BENEFICIARY_NAME,\
#                             BENEFICIARY_CONTRACT_ACCOUNT,\
#                             BENEFICIARY__ID,\
#                             SERVICE_CLASS,\
#                             COORDINATES_SOURCE,\
#                             X,\
#                             Y,\
#                             SDE.ST_AREA(SHAPE) SHAPE_AREA,\
#                             SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
#                             SDE.ST_ASTEXT(SHAPE) geometry\
#                             FROM SE.ELECTPOINT \
#                             where SHAPE is not NULL \
#                             "
# CPSUBDIVISIONPARCELS =    f"SELECT OBJECTID, \
#                             SUBTYPE, \
#                             DETAILSLANDUSE, \
#                             MAINLANDUSE, \
#                             DATASOURCE, \
#                             DESCRIPTION, \
#                             CREATED_DATE, \
#                             CREATED_USER, \
#                             LAST_EDITED_DATE, \
#                             LAST_EDITED_USER, \
#                             REGION_ID, \
#                             SECTOR_ID, \
#                             AMANA_ID, \
#                             GOVERNORATE_ID, \
#                             MUNICIPALITY_ID, \
#                             CITY_ID, \
#                             DISTRICT_ID, \
#                             SUBDISTRICTNAME_ID, \
#                             SUBDIVISIONPLAN_ID, \
#                             BLOCK_ID, \
#                             PARCEL_ID, \
#                             PARCELIDOLD, \
#                             PARCELSTATUS, \
#                             MEASUREDAREA, \
#                             SUBDIVISIONPLANPARTNUMBER, \
#                             STREET_ID, \
#                             DEEDNUMBER, \
#                             SUBDIVISIONPLANNAME_AR, \
#                             SUBDIVISIONPLANNAME_EN, \
#                             OWNERSHIPTYPE, \
#                             SUBDIVISIONPARCELNUMBER, \
#                             ASBUILTREMARKS, \
#                             ISLICENSED, \
#                             ISBUILT, \
#                             DATEOFLICENSEHIJRI, \
#                             LICENSENUMBER, \
#                             NOOFFLOORS, \
#                             CONSTRUCTIONPERCENTAGE, \
#                             REARDEFECTION, \
#                             SIDEDEFECTION, \
#                             FRONTDEFECTION, \
#                             MAINLANDUSEDESCRIPTION, \
#                             SUBTYPEDESCRIPTION, \
#                             DETAILSLANDUSEDESCRIPTION, \
#                             SUBDIVISIONPARCEL_UNIFIED_ID, \
#                             ISCOMMERCIAL, \
#                             BUILDINGCONDITION, \
#                             SUBMUNICIPALITY_ID, \
#                             LANDUSEDESCRIPTION, \
#                             GLOBALID, \
#                             POSTALCODE, \
#                             AREATYPE, \
#                             STREETNAME, \
#                             MUNICIPALITYUNIQUECODE, \
#                             HEIGHTCONDITION, \
#                             DATAOWNER, \
#                             AMANATRACKING, \
#                             AMANABALADI, \
#                             MUNICIPALITYBALADI, \
#                             DISTRICTBALADI, \
#                             DATEOFLICENSE, \
#                             BUILDINGCONDITION_ID, \
#                             PARCELDVELOPED, \
#                             PARCELINVESTED, \
#                             PARCELPROPERTY, \
#                             UNITSNUMBER, \
#                             PARCELNAME, \
#                             BLOCKED, \
#                             UNBLOCKREASON, \
#                             BLOCKREASON, \
#                             WATERSERVICE, \
#                             ELECTRICITYSERVICE, \
#                             TELECOMESERVICE, \
#                             WASTEWATERSERVICE, \
#                             OWNERNAME, \
#                             BLOCKHDATE, \
#                             UNBLOCKHDATE, \
#                             SDE.ST_AREA(SHAPE) SHAPE_AREA,\
#                             SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
#                             SDE.ST_ASTEXT(SHAPE) geometry\
#                             FROM GISOWNER.CPSUBDIVISIONPARCELS \
#                             where SHAPE is not NULL \
#                             "
# CPSUBDIVISIONPLANS =    f"SELECT OBJECTID,\
#                             SUBTYPE,\
#                             DETAILSLANDUSE,\
#                             MAINLANDUSE,\
#                             DATASOURCE,\
#                             DESCRIPTION,\
#                             CREATED_DATE,\
#                             CREATED_USER,\
#                             LAST_EDITED_DATE,\
#                             LAST_EDITED_USER,\
#                             REGION_ID,\
#                             SECTOR_ID,\
#                             AMANA_ID,\
#                             GOVERNORATE_ID,\
#                             MUNICIPALITY_ID,\
#                             CITY_ID,\
#                             DISTRICT_ID,\
#                             SUBDIVISIONPLAN_ID,\
#                             SUBDIVISIONPLANLCODE,\
#                             SUBDIVISIONMOMRACODE,\
#                             SUBDIVISIONNAME_AR,\
#                             SUBDIVISIONNAME_EN,\
#                             VERSION,\
#                             SCALE,\
#                             NOOFPARCEL,\
#                             PLANSTATUS,\
#                             MEASUREDAREA,\
#                             BUILDINGREQUIREMENTS,\
#                             INFRINGMENTS,\
#                             ORDER_ID,\
#                             ORDERDATA,\
#                             APPROVEDBY,\
#                             PLANTYPE,\
#                             APPROVALSTATUS,\
#                             DEEDNUMBER,\
#                             OWNERTYPE,\
#                             SUBDIVISIONPLAN_UNIFIED_ID,\
#                             SUBMUNICIPALITY_ID,\
#                             PLANTYPEDESCRIPTION,\
#                             DATEOFPLANDESCRIPTION,\
#                             GLOBALID,\
#                             DATEOFAPPROVED_HIJRI,\
#                             MUNICIPALITYUNIQUECODE,\
#                             HEIGHTCONDITION,\
#                             DATEOFPLAN,\
#                             DATAOWNER,\
#                             NOOFBLOCK,\
#                             AMANATRACKING,\
#                             AMANABALADI,\
#                             MUNICIPALITYBALADI,\
#                             DISTRICTBALADI,\
#                             SDEFECTION,\
#                             APPROVALNUMBER,\
#                             PLANAPPROVDOCNO,\
#                             PLANCLASS,\
#                             PLANSOILTESTOFFICE,\
#                             PLANNOFLOOR,\
#                             PLANMAXHEIGHT,\
#                             PLANSTREETOFFSET,\
#                             PLANSIDEOFFSET,\
#                             PLANBACKOFFSET,\
#                             PLANMAXOFFSET,\
#                             PLANMINOFFSET,\
#                             PLANRESDUNITNO,\
#                             PLANCOMMUNITNO,\
#                             PLANINDSTUNITNO,\
#                             PLANMOSQUENO,\
#                             PLANSCHOOLNO,\
#                             PLANGOVFACILITYNO,\
#                             PLANGARDENNO,\
#                             PLANMEDICALNO,\
#                             UNIFIEDNUMBER,\
#                             BLOCKREASON,\
#                             UNBLOCKREASON,\
#                             FLGBLOCKED,\
#                             HASLIMITED,\
#                             BLOCKHDATE,\
#                             UNBLOCKHDATE,\
#                             OWNERNAME,\
#                             SUBMUNICIPALITYBALADI,\
#                             DATEOFAPPROVED,\
#                             DATEOFDEED,\
#                             SDE.ST_AREA(SHAPE) SHAPE_AREA,\
#                             SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
#                             SDE.ST_ASTEXT(SHAPE) geometry\
#                             FROM GISOWNER.CPSUBDIVISIONPLANS \
#                             where SHAPE is not NULL \
#                              "
# LUURBANAREABOUNDARYS =    f"SELECT OBJECTID, \
#                                 REGION_ID, \
#                                 SECTOR_ID, \
#                                 AMANA_ID,\
#                                 GOVERNORATE_ID,\
#                                 MUNICIPALITY_ID,\
#                                 CITY_ID,\
#                                 URBANAREABOUNDARY_ID,\
#                                 URBANAREABOUNDARYCODE,\
#                                 URBANAREABOUNDARYTYPE,\
#                                 URBANAREABOUNDARYNAME_AR,\
#                                 URBANAREABOUNDARYNAME_EN,\
#                                 DATEOFMODIFICATION,\
#                                 COMMENTS,\
#                                 URBANAREABOUNDARYYEAR,\
#                                 DATASOURCE,\
#                                 DESCRIPTION,\
#                                 EMIRATE_ID,\
#                                 REGISTEREDAREA,\
#                                 APPROVEDBY,\
#                                 DATEOFAPPROVAL,\
#                                 DATEOFAPPROVALHIJRI,\
#                                 APPROVALNUMBER,\
#                                 URBANAREABOUNDARY_UNIFIED_ID,\
#                                 AMANATRACKING,\
#                                 AMANABALADI AMANACODE,\
#                                 MUNICIPALITYBALADI MUNICIPALITYCODE,\
#                                 DISTRICTBALADI,\
#                                 DISTRICT_ID,\
#                                 SDE.ST_AREA(SHAPE) SHAPE_AREA,\
#                                 SDE.ST_LENGTH(SHAPE) SHAPE_LENG,\
#                                 SDE.ST_ASTEXT(SHAPE) geometry\
#                                 FROM GISOWNER.LUURBANAREABOUNDARYS\
#                                 "
                                        