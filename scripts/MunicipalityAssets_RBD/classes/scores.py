import config

def generate_build(row):
    if row["ma_clusters_buildings"] == 0:
        if row['cases_clusters_buildings'] == 0:
            return 0
        else:
            return 0.50
    elif row["cases_clusters_buildings"] == 0:
        return 0
    else:
        return 1

def generate_roads(row):
    if row["ma_clusters_street"] == 0:
        if row['cases_clusters_street'] == 0:
            if row['contractor_clusters_street'] == 0:
                return 0
            else:
                return 0.5
        elif row['contractor_clusters_street'] == 0:
            return 0.5
        else:
            return 1

    elif row['cases_clusters_street'] == 0:
        if row['contractor_clusters_street'] == 0:
            return 0
        else:
            return 1
    else:
        return 1


def generate_light(row):
    if row["ma_clusters_light"] == 0:
        if row['cases_clusters_light'] == 0:
            return 0
        else:
            return 0.5
    elif row["cases_clusters_light"] == 0:
        return 0
    else:
        return 1

def generate_park(row):
    if row["ma_clusters_parks"] == 0:
        if row['cases_clusters_parks'] == 0:
            return 0
        else:
            return 0.5
    elif row["cases_clusters_parks"] == 0:
        return 0
    else:
        return 1

def generate_const(row):
    if row["ma_clusters_construction"] == 0:
        if row['cases_clusters_construction'] == 0:
            if row['contract_clusters_construction'] == 0:
                return 0
            else:
                return 0.5
        elif row['contract_clusters_construction'] == 0:
            return 0.5
        else:
            return 1
    elif row['cases_clusters_construction'] == 0:
        if row['contract_clusters_construction'] == 0:
            return 0
        else:
            return 1
    else:
        return 1



