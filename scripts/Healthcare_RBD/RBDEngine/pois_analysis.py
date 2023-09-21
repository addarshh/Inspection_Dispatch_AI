import os
import geopandas as gpd
import logging

def generate_pois(licences_df, Health_df, population_grids):
    import pandas as pd
    All_Licenses_medina_df = licences_df
    Health_df = Health_df
    Health_df['Business activity'] = Health_df['D_ACTIVITIES_NAME']
    All_Licenses_medina_df = pd.merge(left=All_Licenses_medina_df, right = Health_df, on='Business activity', how='left')
    All_Licenses_medina_df = All_Licenses_medina_df.drop_duplicates(subset=['License ID (MOMRAH)'])
    Health_Licenses_medina_df = All_Licenses_medina_df.loc[All_Licenses_medina_df['ACTIVITIE_TYPE_ID'] == 1]
    population_grids = population_grids
    POI_df1_raw=pd.read_excel(os.path.join(os.path.dirname(__file__), '..', 'Datav2', '20220807092142e65d.xlsx'))
    logging.info("""CHECK:POIS df:POI_df1_raw {}""".format(POI_df1_raw.shape))
    POI_df2_raw=pd.read_excel(os.path.join(os.path.dirname(__file__), '..', 'Datav2', '20220808121409a756.xlsx'))
    logging.info("""CHECK:POIS df:POI_df2_raw {}""".format(POI_df2_raw.shape))
    population_grids['GridNumber'] = population_grids['GRIDUNIQUECODE'].astype(str)
    licenses_df=Health_Licenses_medina_df.copy()
    POI_df1=POI_df1_raw.copy()
    POI_df2=POI_df2_raw.copy()
    licenses_df['Latitude']=pd.to_numeric(licenses_df['Latitude'], errors='coerce')
    licenses_df['Longitude']=pd.to_numeric(licenses_df['Longitude'], errors='coerce')
    licenses_df = gpd.GeoDataFrame(licenses_df, geometry=gpd.points_from_xy(licenses_df.Latitude, licenses_df.Longitude), crs="EPSG:4326")
    POI_df = pd.concat([POI_df1, POI_df2])
    logging.info("""CHECK:POIS df:POI_df {}""".format(POI_df.shape))
    POI_df = gpd.GeoDataFrame(POI_df, geometry=gpd.points_from_xy(POI_df.longitude, POI_df.latitude), crs="EPSG:4326")
    licenses_with_coordinates=licenses_df[~licenses_df['Latitude'].isna()]
    licenses_with_coordinates=licenses_with_coordinates[licenses_with_coordinates['License Expiry Date']>'2022-07-30']
    print("population_grids POIS", population_grids.columns)
    #population_grids = gpd.GeoDataFrame(population_grids, geometry=gpd.GeoSeries.from_wkt(population_grids.geometry.astype('str')), crs="EPSG:4326")
    POI_grids=gpd.sjoin(POI_df, population_grids, op='within', how='right')
    licenses_with_coordinates = licenses_with_coordinates[licenses_with_coordinates.is_valid]
    license_grids=gpd.sjoin(licenses_with_coordinates, population_grids, how='right')
    license_grids['License ID (MOMRAH)'].nunique()
    n_licenses_per_grid=license_grids.groupby('GridNumber')['License ID (MOMRAH)'].nunique().reset_index()
    n_pois_per_grid=POI_grids.groupby('GridNumber')['name'].nunique().reset_index()
    pois_licenses_comparison=n_licenses_per_grid.merge(n_pois_per_grid, how='outer', on='GridNumber')
    pois_licenses_comparison=pois_licenses_comparison.rename(columns={'License ID (MOMRAH)':'n_licenses','name':'n_POIs'})
    pois_licenses_comparison['pois_licenses_difference']=pois_licenses_comparison['n_POIs']-pois_licenses_comparison['n_licenses']
    pois_licenses_comparison=pois_licenses_comparison.merge(population_grids[['GridNumber','geometry']], on='GridNumber', how='inner')
    pois_licenses_comparison=gpd.GeoDataFrame(pois_licenses_comparison, geometry='geometry',crs='epsg:4326')
    pois_licenses_comparison=pois_licenses_comparison.merge(population_grids[['GridNumber','GRID_ID']], on='GridNumber')
    #pois_licenses_comparison.columns
    pois_licenses_comparison=pois_licenses_comparison[['GridNumber','GRID_ID','pois_licenses_difference']]
    #pois_licenses_comparison.to_csv('Datav2/pois_licenses_comparison.csv')
    pois_licenses_comparison.to_csv(os.path.join(os.path.dirname(__file__), '..', 'Datav2', 'pois_licenses_comparison.csv'))
    pois_licenses_comparison = pd.DataFrame(pois_licenses_comparison)
    print(pois_licenses_comparison.shape)
    logging.info("""CHECK:POIS calculation result df:pois_licenses_comparison {}""".format(pois_licenses_comparison.shape))
    return  pois_licenses_comparison