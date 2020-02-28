""""
Processes Survey Solutions raw data to create four CSV files
1. from_Copperbelt_[date].csv, from_the_rest_of_provinces_[date].csv
2. POI.csv
3. HH.csv
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
import os
import numpy as np
from ListingDataProcessor import utils as ut


def main():
    # ==========================================
    # SET UP DIRECTORIES
    # ==========================================
    data_dir = Path.cwd().parents[1].joinpath("data")
    out_prov_csv = data_dir.joinpath('from_ss_python_processed')
    all_provs_csv = out_prov_csv.joinpath("from_Copperbelt_2020-02-18.csv")

    # ==========================================
    # SET PROCESSING PARAMS
    # ==========================================

    cols_to_keep_hh = ['interview__key', 'interview__id', 'Cluster', 'PROV', 'DIST', 'CONS',
                       'WARD', 'REGION', 'SEA', 'LOCALITY', 'GPSLocation__Latitude',
                       'GPSLocation__Longitude', 'date',
                       'GeoLocation_Latitude', 'GeoLocation_Longitude', 'Males_in_structure', 'Females_in_structure',
                       'Males_in_Household',
                       'Females_in_Household', 'Total_Households', 'Household_Population',
                       'First_Head_Name', 'Add_Head_Name', "Multipurpose_Residential_Building",
                       "Structure_Type_Categorisation", 'Structure_Institution_Occupied', "Add_Structure_Occupied"]
    cols_to_keep_POI = ['interview__key', 'interview__id', 'Cluster', 'PROV', 'DIST', 'CONS',
                        'WARD', 'REGION', 'SEA', 'LOCALITY', 'GPSLocation__Latitude',
                        'GPSLocation__Longitude', 'date',
                        'GeoLocation_Latitude', 'GeoLocation_Longitude', 'Multipurpose_Residential_Building',
                        'Multipurpose_Religious_Building',
                        'Multipurpose_Institutional_Building',
                        'Multipurpose_Commercial_Building', 'Residential_Building',
                        'Specify_Other_Residential', 'Religious_Building',
                        'Specify_Other_Religious_Structures', 'Institutional_Building',
                        'Specify_Other_Institutional_Building', 'Educational_Building',
                        'Commercial_Building', 'Specify_Other_Commercial_Buildings',
                        'Health_Facility_Hospital_Health_Center', 'Ownership_of_Institution',
                        'Status_of_the_Institution', 'Structure_Name']

    # ==========================================
    # RUN THE FUNCTIONS
    # ==========================================
    ut.extract_structures_with_households(survey_solutions_python_processed_csv=all_provs_csv,
                                          cols_to_keep_hh=cols_to_keep_hh, cols_to_keep_pois=cols_to_keep_POI,
                                          out_csv_dir=out_prov_csv)


if __name__ == '__main__':
    main()
