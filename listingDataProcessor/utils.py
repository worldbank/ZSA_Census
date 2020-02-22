"""
Miscellaneous data processing utility functions.
"""
import os
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import datetime


def create_df(file):
    """
    Returns a pandas data frame

    :param file: CSV file with raw hh listing data
    :return:
    """
    encodings = ["ISO-8859-1", "latin1"]

    for encoding in encodings:
        try:
            df = pd.read_csv(file, encoding=encoding)
            return df
        except Exception as e:
            continue


def fix_coordinates(row, col_to_fix=None):
    """
    Fixes coordinates to ensure all
    records have coordinates.
    """
    geo_loc_lon = row["GeoLocation_Longitude"]
    geo_loc_lat = row["GeoLocation_Latitude"]

    gps_lon = row["GPSLocation__Longitude"]
    gps_lat = row["GPSLocation__Latitude"]

    try:
        if "Latitude" in col_to_fix:
            if str(gps_lat) == str(np.nan):
                return geo_loc_lat
            else:
                return row[col_to_fix]
        elif "Longitude" in col_to_fix:
            if str(gps_lon) == str(np.nan):
                return geo_loc_lon
            else:
                return row[col_to_fix]
    except Exception as e:
        return np.nan


def label_households(row):
    """
    Select households by using the following columns.
    The columns are shamelessly hardcoded
    """
    # =====================================
    # First check the following columns
    # =====================================
    if row["Multipurpose_Residential_Building"] == 1.0:
        return 1
    elif row["Structure_Type_Categorisation"] == 1.0:
        return 1
    elif row['Structure_Institution_Occupied'] == 1.0:
        return 1
    elif row["Add_Structure_Occupied"] == 1.0:
        return 1
    elif row['Total_Households'] > 0 or row['Household_Population'] > 0:
        return 1
    else:
        return 0


def label_pois(row):
    """
    Label a structure as POI based on the following.
    Note that we leave out Nulll values
    """
    if row['Structure_Type_Categorisation'] > 1:
        return 1
    elif row["Institutional_Building"] >= 1:
        return 1
    elif row["Religious_Building"] >=1:
        return 1
    elif row["Educational_Building"] >= 1:
        return 1
    elif row['Commercial_Building'] >= 1:
        return 1
    elif row['Health_Facility_Hospital_Health_Center']>= 1:
        return 1
    elif row['Multipurpose_Religious_Building'] == 1.0:
        return 1
    elif row['Multipurpose_Institutional_Building'] == 1.0:
        return 1
    elif row['Multipurpose_Commercial_Building'] == 1.0:
        return 1
    else:
        return 0


def extract_structures_with_households(survey_solutions_python_processed_csv,
                                       cols_to_keep_hh, cols_to_keep_pois, out_csv_dir):
    """
    Processes and spits out a dataframe with households.
    """
    # ===========================
    # Read the data
    # ===========================
    df_csv = pd.read_csv(survey_solutions_python_processed_csv)

    # =============================
    # Fix coordinates
    # Drop any records without coords
    # =============================
    df_csv["GPSLocation__Longitude"] = df_csv.apply(fix_coordinates, args=("GPSLocation__Longitude",), axis=1)
    df_csv["GPSLocation__Latitude"] = df_csv.apply(fix_coordinates, args=("GPSLocation__Latitude",), axis=1)

    num_rows_before = df_csv.shape[0]
    df_csv.dropna(subset=["GPSLocation__Longitude", "GPSLocation__Latitude"], inplace=True)
    num_rows_after = df_csv.shape[0]

    print("Number of records dropped due to missing cordinates: {}".format(num_rows_before - num_rows_after))

    # ================================
    # Add column to determine whether its
    # a HH or POI and subset HH only
    # ================================
    df_csv["Is_Household"] = df_csv.apply(label_households, axis=1)
    df_hh = df_csv[df_csv.Is_Household == 1]
    df_csv["Is_POI"] = df_csv.apply(label_pois, axis=1)
    df_pois = df_csv[df_csv.Is_POI == 1]

    # ================================
    # Keep only required columns
    # ================================
    df_hh = df_hh[cols_to_keep_hh]
    df_pois = df_pois[cols_to_keep_pois]

    # ================================
    # Save to disk
    # ================================
    hh_file = out_csv_dir.joinpath("HH.csv")
    poi_file = out_csv_dir.joinpath("POI.csv")
    df_hh.to_csv(hh_file, index=False)
    df_pois.to_csv(poi_file, index=False)


def join_two_tab_files(left_df, right_df, merge_cols):
    """
    Helper function for joining two files
    """

    # first add merge_id column
    if len(merge_cols) > 1:
        col1, col2 = merge_cols[0], merge_cols[1]
        left_df["merge_id"] = left_df.apply(lambda x: str(x[col1]) + str(x[col2]), axis=1)
        right_df["merge_id"] = right_df.apply(lambda x: str(x[col1]) + str(x[col2]), axis=1)

    # merge and rename some columns to prepare for further merging
    df = left_df.merge(right=right_df, on='merge_id', how="left", indicator=True)
    df.rename(columns={'interview__key_x': 'interview__key', "_merge": "_merge_1"}, inplace=True)

    return df


def summarize_by_interview_key(df, agg_func_first=None, agg_func_mean=None, agg_func_sum=None, agg_func=None,
                               agg_col='interview__key'):
    """

    """
    if agg_func:
        dfg = df.groupby([agg_col]).agg(agg_func).reset_index()
        return dfg
    # =====================================================================
    # PREPARE AGG FUNCTIONS
    # =====================================================================
    # create a dict object
    agg_func = {i: "first" for i in agg_func_first}
    agg_func_mean_dict = {i: "mean" for i in agg_func_mean}
    agg_func_sum_dict = {i: "sum" for i in agg_func_sum}

    agg_func.update(agg_func_mean_dict)
    agg_func.update(agg_func_sum_dict)

    # =====================================================================
    # GROUP AND SUMMARISE
    # =====================================================================
    dfg = df.groupby([agg_col]).agg(agg_func).reset_index()

    return dfg


def convert_geolocation_to_numeric(row, col_name, geo_col):
    try:
        if col_name == 'Longitude':
            val = float(row[geo_col].split(",")[0])
            return val
        elif col_name == 'Latitude':
            val = float(row[geo_col].split(",")[1])
            return val
    except Exception as e:
        return np.nan


def prepare_tab_files_for_joining(dir_with_tab_files, tab_file_names):
    """
    Takes all the tab files from survey solutions and preps them for merging
    and processing
    """
    # ======================================================
    # GRAB ALL THE REQUIRED TAB FILES and CREATE DATAFRAMES
    # ======================================================
    final, hh_roster, add_hh_roster, hh_units, add_struct = None, None, None, None, None
    final_tab, hroster, addroster, hhunits, addstruct = tab_file_names['final'], tab_file_names['hh_roster'], \
                                                        tab_file_names['add_roster'], tab_file_names['hh_units'], \
                                                        tab_file_names['add_struct']
    for f in dir_with_tab_files.iterdir():
        file_name = f.parts[-1]
        if file_name == final_tab:
            final = pd.read_csv(f, sep="\t")
            # final = final[final.PROV == 2]
        elif file_name == hroster:
            hh_roster = pd.read_csv(f, sep="\t")
        elif file_name == addroster:
            add_hh_roster = pd.read_csv(f, sep="\t")
        elif file_name == hhunits:
            hh_units = pd.read_csv(f, sep="\t")
        elif file_name == addstruct:
            add_struct = pd.read_csv(f, sep="\t")
    print("Size, final: {}, Size, hhroster: {},Size, hhunits: {}, ".format(final.shape[0],
                                                                           hh_roster.shape[0],
                                                                           hh_units.shape[0]))
    # ======================================================
    # CHANGE COLUMN NAMES, DROP COLUMNS AND ADD NEW COLUMNS
    # ======================================================
    # final
    final['Structure_Category'] = 'REG'  # indicates whether this is addional structure or not
    final['GeoLocation_Latitude'] = final.apply(convert_geolocation_to_numeric, args=("Latitude", 'GEOLOCATION'),
                                                axis=1)
    final['GeoLocation_Longitude'] = final.apply(convert_geolocation_to_numeric, args=("Longitude", 'GEOLOCATION'),
                                                 axis=1)
    drop_cols_final = ['rand_sys', 'Comment', 'Comments', 'sssys_irnd', 'has__errors', 'assignment__id']
    change_col_names_final = {"IMales": 'Males_in_structure', "IFemales": 'Females_in_structure',
                              'NOHU': 'Number_of_Housing_Units',
                              'MULTI__1': 'Multipurpose_Residential_Building',
                              'MULTI__2': 'Multipurpose_Religious_Building',
                              'MULTI__3': 'Multipurpose_Institutional_Building',
                              'MULTI__4': 'Multipurpose_Commercial_Building',
                              "CATEGORY": "Structure_Type_Categorisation",
                              'SCATEGORY': 'Specify_Other_Type_Categorisation',
                              "RESIDENTIAL": 'Residential_Building',
                              "SRESIDENTIAL": 'Specify_Other_Residential',
                              "RELIGIOUS": 'Religious_Building',
                              "SRELIGIOUS": 'Specify_Other_Religious_Structures',
                              "INSTITUTIONAL": 'Institutional_Building',
                              "SINSTITUTIONAL": 'Specify_Other_Institutional_Building',
                              "EDUCATIONAL": 'Educational_Building',
                              "COMMERCIAL": 'Commercial_Building',
                              "SCOMMERCIAL": 'Specify_Other_Commercial_Buildings',
                              "HEALTH": 'Health_Facility_Hospital_Health_Center',
                              "OWNERSHIP": 'Ownership_of_Institution',
                              "STATUS": 'Status_of_the_Institution',
                              "IName": 'Structure_Name',
                              "OCCUPANCY": 'Structure_Institution_Occupied'
                              }
    final.drop(labels=drop_cols_final, axis=1, inplace=True)
    final.rename(columns=change_col_names_final, inplace=True)
    final["date"] = final.apply(lambda x: x['GPSLocation__Timestamp'].split("T")[0], axis=1)

    # housing units
    hh_units.rename(columns={"NOHH": 'Total_Households', 'HUOS': 'Housing_Unit_Occupied'}, inplace=True)

    # hh
    hh_roster['Household_Category'] = "REG"
    hh_roster.drop(labels=['HPhone', 'Own', 'NUMBEROFFARMS', 'NoFarmsA5ha'], axis=1, inplace=True)
    hh_roster.rename(columns={"HMales": 'Males_in_Household', "HFemales": 'Females_in_Household',
                              "HName": 'First_Head_Name'}, inplace=True)

    # additional household
    add_hh_roster['Household_Category'] = "ADD"
    add_hh_roster.rename(columns={"addHMales": 'Males_in_Household', "addHFemales": 'Females_in_Household',
                                  'addHName': "Add_Head_Name"},
                         inplace=True)

    # additional structure
    add_struct['Structure_Category'] = 'ADD'
    add_struct.rename(columns={'ADDITION_HH': "Total_Households", 'AddtionOccupany': 'Add_Structure_Occupied'},
                      inplace=True)
    add_struct['GeoLocation_Latitude'] = add_struct.apply(convert_geolocation_to_numeric,
                                                          args=("Latitude", 'ADDTIONGEOLOCATION'), axis=1)
    add_struct['GeoLocation_Longitude'] = add_struct.apply(convert_geolocation_to_numeric,
                                                           args=("Longitude", 'ADDTIONGEOLOCATION'), axis=1)

    # ======================================================
    # SUMMARIZE AT INTERVIEW_KEY LEVEL
    # ======================================================
    agg_func_hhunits = {'Total_Households': 'sum', 'Housing_Unit_Occupied': "first"}
    agg_func_roster = {'Males_in_Household': 'sum', 'Females_in_Household': 'sum',
                       "interview__id": "first", 'First_Head_Name': 'first', 'Household_Category': 'first'}
    agg_func_add_hh_roster = {'Add_Head_Name': 'first', 'Males_in_Household': 'sum',
                              'Females_in_Household': 'sum', 'Household_Category': 'first'}
    agg_func_add_struct = {'Total_Households': 'sum', 'Add_Structure_Occupied': 'first', 'interview__id': 'first',
                           'GeoLocation_Latitude': 'mean', 'GeoLocation_Longitude': 'mean',
                           'Structure_Category': 'first'}

    hh_units_ik = summarize_by_interview_key(df=hh_units, agg_func=agg_func_hhunits)
    hh_roster_ik = summarize_by_interview_key(df=hh_roster, agg_func=agg_func_roster)
    add_hh_roster_ik = summarize_by_interview_key(df=add_hh_roster, agg_func=agg_func_add_hh_roster)
    add_struct_ik = summarize_by_interview_key(df=add_struct, agg_func=agg_func_add_struct)

    return final, hh_roster_ik, hh_units_ik, add_hh_roster_ik, add_struct_ik


def process_tab_files_hh_roster_hh_units_final(dir_with_tab_files, tab_file_names):
    """
    Combines the following three tab files:
    - Final.tab
    - HHROSTER.tab
    - HOUSINGUNITS.tab
    to create a final output which is summarized at Interview__key level.
    :param dir_with_tab_files: Dir where the tab delimited Survey Solutions files are
    :return: a dataframe which will later be appended to another dataframe coming from add_roster and add_struct
    """

    # ======================================================
    # PREP THE DATA TAB FILES FOR MERGING
    # ======================================================
    final, hh_roster, hh_units, add_hh_roster, add_struct = prepare_tab_files_for_joining(dir_with_tab_files,
                                                                                          tab_file_names=tab_file_names)

    # ======================================================
    # MERGE HHROSTER WITH HHUNITS
    # ======================================================
    hhroster_hhunits = join_two_tab_files(left_df=hh_roster, right_df=hh_units,
                                          merge_cols=['interview__key', "HOUSINGUNITS__id"])

    # ======================================================
    # MERGE PREVIOUS RESULT (hhroster_hhunits) TO FINAL.TAB
    # ======================================================
    hhroster_hhunits_final = hhroster_hhunits.merge(right=final, on='interview__key', how="left", indicator=True)

    # rename and drop some columns
    hhroster_hhunits_final.drop(labels=['interview__id_x', 'interview__id_y', "HOUSINGUNITS__id_x",
                                        'HOUSINGUNITS__id_x', 'merge_id', '_merge_1', "_merge", 'interview__key_y'],
                                axis=1, inplace=True)

    hhroster_hhunits_final.rename(columns={'HOUSINGUNITS__id_y': "HOUSINGUNITS__id"}, inplace=True)

    print("hhroster_hhunits_final: {}".format(hhroster_hhunits_final.shape[0]))
    # ======================================================
    # SUMMARIZE BY INTERVIEW KEY
    # ======================================================
    agg_func_first = ['interview__id', 'PROV', 'DIST', 'CONS', 'WARD',
                      'REGION', 'SEA', 'LOCALITY', 'Religious_Building', 'Specify_Other_Religious_Structures',
                      'Institutional_Building', 'Specify_Other_Institutional_Building',
                      'Educational_Building', 'Commercial_Building', 'Specify_Other_Commercial_Buildings',
                      'Health_Facility_Hospital_Health_Center', 'Ownership_of_Institution',
                      'Status_of_the_Institution', 'CBN', 'GPSLocation__Timestamp', "Structure_Type_Categorisation",
                      'Specify_Other_Type_Categorisation', 'Educational_Building', 'Commercial_Building',
                      'Specify_Other_Commercial_Buildings', 'Health_Facility_Hospital_Health_Center',
                      'Ownership_of_Institution', 'addtionalstructure', 'First_Head_Name',
                      'Multipurpose_Commercial_Building',
                      'Multipurpose_Institutional_Building', "date",
                      'Multipurpose_Religious_Building',
                      'Multipurpose_Residential_Building', 'GEOLOCATION',
                      'Residential_Building',
                      'Specify_Other_Commercial_Buildings',
                      'Specify_Other_Religious_Structures',
                      'Specify_Other_Residential',
                      'Specify_Other_Type_Categorisation',
                      'Structure_Institution_Occupied',
                      'Structure_Name',
                      'Structure_Type_Categorisation', 'interview__status'
                      ]

    agg_func_mean = ['GPSLocation__Latitude', 'GPSLocation__Longitude', 'GeoLocation_Latitude',
                     'GPSLocation__Accuracy', 'GPSLocation__Altitude', 'GeoLocation_Longitude']

    agg_func_sum = ['Males_in_Household', 'Females_in_Household', 'Males_in_structure',
                    'Females_in_structure', 'Number_of_Housing_Units', 'Total_Households']
    # summarize by interview key
    hhroster_hhunits_final_ik = summarize_by_interview_key(df=hhroster_hhunits_final, agg_func_first=agg_func_first,
                                                           agg_func_mean=agg_func_mean, agg_func_sum=agg_func_sum,
                                                           agg_col='interview__key')
    # rename above dataframe for convinience, ik stands for Interview__Key
    hhr_hhu_final_ik = hhroster_hhunits_final_ik

    # add Hoousehold_Population Column
    hhr_hhu_final_ik['Household_Population'] = hhr_hhu_final_ik['Males_in_Household'] + \
                                               hhr_hhu_final_ik['Females_in_Household']
    hhr_hhu_final_ik['Institutional_Structure_Population'] = hhr_hhu_final_ik['Males_in_structure'] + \
                                                             hhr_hhu_final_ik['Females_in_structure']

    return hhr_hhu_final_ik


def process_tab_files(dir_with_tab_files, tab_file_names, output_csv_dir,
                                                from_which_download):
    """
    Combines the following three tab files:
    - Final.tab
    - HHROSTER.tab
    - HOUSINGUNITS.tab
    to create a final output which is summarized at Interview__key level.
    :param dir_with_tab_files: Dir where the tab delimited Survey Solutions files are
    :return: a dataframe which will later be appended to another dataframe coming from add_roster and add_struct
    """

    # ======================================================
    # PREP THE DATA TAB FILES FOR MERGING
    # ======================================================
    final, hh_roster, hh_units, add_hh_roster, add_struct = prepare_tab_files_for_joining(dir_with_tab_files,
                                                                                          tab_file_names=tab_file_names)
    # ======================================================
    #  ADD HOUSEHOLD POPULATION VARIABLES AT IK LEVEL
    # ======================================================
    hh_roster['Household_Population'] = hh_roster['Males_in_Household'] + \
                                        hh_roster['Females_in_Household']
    final['Institutional_Structure_Population'] = final['Males_in_structure'] + \
                                                  final['Females_in_structure']
    add_hh_roster['Household_Population'] = add_hh_roster['Males_in_Household'] + \
                                            add_hh_roster['Females_in_Household']

    # ======================================================
    # MERGE HHROSTER, HHUNITS WITH FINAL
    # ======================================================
    final_hh_roster = final.merge(right=hh_roster, on='interview__key', how="left", indicator=True)

    # ensure all records from hh_roster merged into final
    hh_roster_size = hh_roster.shape[0]
    hh_roster_matched_size = final_hh_roster[final_hh_roster._merge == 'both'].shape[0]
    assert hh_roster_size == hh_roster_matched_size
    final_hh_roster.drop(labels=['_merge'], axis=1, inplace=True)

    # now merge hh_units to above output
    final_hh_roster_hh_units = final_hh_roster.merge(right=hh_units, on='interview__key', how="left", indicator=True)

    # ensure all records from hh_units merged into final
    hh_units_size = hh_units.shape[0]
    hh_units_matched_size = final_hh_roster_hh_units[final_hh_roster_hh_units._merge == 'both'].shape[0]
    assert hh_units_size == hh_units_matched_size
    # clean up
    final_hh_roster_hh_units.drop(labels=['_merge', 'interview__id_y'], axis=1, inplace=True)
    final_hh_roster_hh_units.rename(columns={'interview__id_x': 'interview__id'}, inplace=True)

    # ========================================================================
    # MERGE ADDITIONAL STRUCTURES WITH ADD_HHROSTER SO THAT WE HAVE LOCATION
    # ========================================================================
    add_struct_add_roster = add_struct.merge(right=add_hh_roster, on='interview__key', how='left', indicator=True)

    add_hh_roster_size = add_hh_roster.shape[0]
    add_hh_roster_matched_size = add_struct_add_roster[add_struct_add_roster._merge == 'both'].shape[0]
    assert add_hh_roster_size == add_hh_roster_matched_size

    # clean up
    add_struct_add_roster.drop(labels=['_merge'], axis=1, inplace=True)

    # =========================================================
    # APPEND add_struct_add_roster TO final_hh_roster_hh_units
    # =========================================================
    df = final_hh_roster_hh_units.append(add_struct_add_roster)
    rearranged_cols = ['interview__key', 'interview__id', 'Cluster',
                       'PROV',
                       'DIST',
                       'CONS',
                       'WARD',
                       'REGION',
                       'SEA',
                       'LOCALITY',
                       'Address',
                       'GPSLocation__Latitude',
                       'GPSLocation__Longitude',
                       'GPSLocation__Accuracy',
                       'GPSLocation__Altitude',
                       'GPSLocation__Timestamp',
                       'date',
                       'GeoLocation_Latitude',
                       'GeoLocation_Longitude',
                       'GEOLOCATION',
                       'Structure_Type_Categorisation',
                       'Specify_Other_Type_Categorisation',
                       'CBN',
                       'Multipurpose_Residential_Building',
                       'Multipurpose_Religious_Building',
                       'Multipurpose_Institutional_Building',
                       'Multipurpose_Commercial_Building',
                       'Residential_Building',
                       'Specify_Other_Residential',
                       'Religious_Building',
                       'Specify_Other_Religious_Structures',
                       'Institutional_Building',
                       'Specify_Other_Institutional_Building',
                       'Educational_Building',
                       'Commercial_Building',
                       'Specify_Other_Commercial_Buildings',
                       'Health_Facility_Hospital_Health_Center',
                       'Ownership_of_Institution',
                       'Status_of_the_Institution',
                       'Structure_Name',
                       'Structure_Institution_Occupied',
                       'Add_Structure_Occupied',
                       'Males_in_structure',
                       'Females_in_structure',
                       'Number_of_Housing_Units',
                       'addtionalstructure',
                       'ADDITION_NUM',
                       'interview__status',
                       'Structure_Category',
                       'Institutional_Structure_Population',
                       'Household_Category',
                       'Males_in_Household',
                       'Females_in_Household',
                       'First_Head_Name',
                       'Add_Head_Name',
                       'Household_Population',
                       'Total_Households',
                       'Housing_Unit_Occupied']
    df = df[rearranged_cols]

    # =========================================================
    # SAVE IN OUTPUT DIR
    # =========================================================
    date = datetime.strftime(datetime.now(), "%Y-%m-%d")
    fname = "from_{}_{}.csv".format(from_which_download, date)
    fpath = output_csv_dir.joinpath(fname)
    df.to_csv(fpath, index=False)

    return df


def process_tab_delimited_add_hh_roster_add_struct_final(dir_with_tab_files, tab_file_names):
    """
    Combines the following files comining from Survey Solutions API:
    - Final.tab
    - ADDHHROSTER.tab
    - ADDTIONSTRUCTURES.tab
    to create a final output which is summarized at Interview__key level.
    :param dir_with_tab_files: Dir where the tab delimited Survey Solutions files are
    :return: a dataframe which will later be appended to another dataframe coming from hh_roster and hh_units
    """

    # ======================================================
    # PREP THE DATA TAB FILES FOR MERGING
    # ======================================================
    final, hh_roster, hh_units, add_hh_roster, add_struct = prepare_tab_files_for_joining(dir_with_tab_files,
                                                                                          tab_file_names=tab_file_names)

    # ======================================================
    # MERGE ADDHHROSTER WITH ADDSTRUCTURE
    # ======================================================
    addhhroster_addhhstruct = join_two_tab_files(left_df=add_hh_roster, right_df=add_struct,
                                                 merge_cols=['interview__key', 'ADDTIONSTRUCTURES__id'])

    addhhroster_addhhstruct.rename(columns={'interview__key_x': 'interview__key', "_merge": "_merge_1"}, inplace=True)

    # ===============================================================
    # MERGE PREVIOUS RESULT (addhhroster_addhhstruct) TO FINAL.TAB
    # ==============================================================
    addhhroster_addhhstruct_final = addhhroster_addhhstruct.merge(right=final, on='interview__key', how="left",
                                                                  indicator=True)

    # rename and drop some columns
    addhhroster_addhhstruct_final.drop(
        labels=['interview__id_x', 'interview__id_y', 'merge_id', '_merge_1', "_merge", 'ADDTIONSTRUCTURES__id_x',
                'interview__key_y'],
        axis=1, inplace=True)

    addhhroster_addhhstruct_final.rename(columns={'ADDTIONSTRUCTURES__id_y': 'ADDTIONSTRUCTURES__id'}, inplace=True)

    # ======================================================
    # SUMMARIZE BY INTERVIEW KEY
    # ======================================================
    # agg functions hhroster_hhunits_final.
    agg_func_first = ['interview__id', 'PROV', 'DIST', 'CONS', 'WARD',
                      'REGION', 'SEA', 'LOCALITY', 'Religious_Building', 'Specify_Other_Religious_Structures',
                      'Institutional_Building', 'Specify_Other_Institutional_Building',
                      'Educational_Building', 'Commercial_Building', 'Specify_Other_Commercial_Buildings',
                      'Health_Facility_Hospital_Health_Center', 'Ownership_of_Institution',
                      'Status_of_the_Institution', 'CBN', 'GPSLocation__Timestamp', "Structure_Type_Categorisation",
                      'Specify_Other_Type_Categorisation', 'Educational_Building', 'Commercial_Building',
                      'Specify_Other_Commercial_Buildings', 'Health_Facility_Hospital_Health_Center',
                      'Ownership_of_Institution', 'addtionalstructure', 'Add_Head_Name',
                      'Multipurpose_Commercial_Building', "date",
                      'Multipurpose_Institutional_Building',
                      'Multipurpose_Religious_Building',
                      'Multipurpose_Residential_Building', 'GEOLOCATION',
                      'Residential_Building',
                      'Specify_Other_Commercial_Buildings',
                      'Specify_Other_Religious_Structures',
                      'Specify_Other_Residential',
                      'Specify_Other_Type_Categorisation',
                      'Structure_Institution_Occupied',
                      'Structure_Name',
                      'Structure_Type_Categorisation', 'interview__status'
                      ]

    agg_func_mean = ['GPSLocation__Latitude', 'GPSLocation__Longitude', 'GeoLocation_Latitude',
                     'GPSLocation__Accuracy', 'GPSLocation__Altitude', 'GeoLocation_Longitude']

    agg_func_sum = ['Males_in_Add_Household', 'Females_in_Add_Household', 'Males_in_structure',
                    'Females_in_structure', 'Total_Add_Households']
    # summarize by interview key
    addhhroster_addhhstruct_final_ik = summarize_by_interview_key(df=addhhroster_addhhstruct_final,
                                                                  agg_func_first=agg_func_first,
                                                                  agg_func_mean=agg_func_mean,
                                                                  agg_func_sum=agg_func_sum,
                                                                  agg_col='interview__key')
    # rename above dataframe for convinience, ik stands for Interview__Key
    addhhr_addstruct_final_ik = addhhroster_addhhstruct_final_ik

    # add Hoousehold_Population Column
    addhhr_addstruct_final_ik['Household_Population'] = addhhr_addstruct_final_ik['Males_in_Add_Household'] + \
                                                        addhhr_addstruct_final_ik['Females_in_Add_Household']
    addhhr_addstruct_final_ik['Institutional_Structure_Population'] = addhhr_addstruct_final_ik['Males_in_structure'] + \
                                                                      addhhr_addstruct_final_ik['Females_in_structure']

    return addhhr_addstruct_final_ik


def process_tab_files_tmp(input_dir, tab_file_names_dict, ref_spss_csv=None, output_csv=None):
    """
    Processes all relevant tab delimited files to create an output CSV
    """
    # =======================================================
    # CREATE A MERGED FILE SUMMARIZED AT INTERVIEW_KEY_LEVEL
    # =======================================================
    hhroster_hhunits_final_ik = process_tab_files_hh_roster_hh_units_final(dir_with_tab_files=input_dir,
                                                                           tab_file_names=tab_file_names_dict)
    add_hh_roster_add_struct_final_ik = process_tab_delimited_add_hh_roster_add_struct_final(
        dir_with_tab_files=input_dir, tab_file_names=tab_file_names_dict)

    # =======================================================
    # FOR ADD HH_ROSTER AND ADD_STRUCT OUTPUT, CHANGE COLNAMES
    # TO PREPARE FOR APPENDING
    # =======================================================
    add_hh_roster_add_struct_final_ik.rename(columns={"Total_Add_Households": 'Total_Households',
                                                      'Males_in_Add_Household': 'Males_in_Household',
                                                      'Females_in_Add_Household': 'Females_in_Household'},
                                             inplace=True)
    # add this column to separate regular households from additional households as
    # some interview__key have additional households with different locations while most of the
    # additional households have their own interview_key(ik).
    add_hh_roster_add_struct_final_ik['Household_Category'] = "ADD"
    hhroster_hhunits_final_ik['Household_Category'] = "REG"

    # =======================================================
    # APPEND
    # =======================================================
    df = hhroster_hhunits_final_ik.append(add_hh_roster_add_struct_final_ik)

    # =======================================================
    # REARRANGE COLUMNS
    # =======================================================
    ref_cols = ['interview__key', 'interview__id', 'PROV', 'DIST', 'CONS', 'WARD',
                'REGION', 'SEA', 'LOCALITY', 'GPSLocation__Latitude',
                'GPSLocation__Longitude', 'GPSLocation__Accuracy',
                'GPSLocation__Altitude', 'GPSLocation__Timestamp', 'GEOLOCATION',
                'Structure_Type_Categorisation', 'Specify_Other_Type_Categorisation',
                'CBN', 'Multipurpose_Residential_Building',
                'Multipurpose_Religious_Building',
                'Multipurpose_Institutional_Building',
                'Multipurpose_Commercial_Building', 'Residential_Building',
                'Specify_Other_Residential', 'Religious_Building',
                'Specify_Other_Religious_Structures', 'Institutional_Building',
                'Specify_Other_Institutional_Building', 'Educational_Building',
                'Commercial_Building', 'Specify_Other_Commercial_Buildings',
                'Health_Facility_Hospital_Health_Center', 'Ownership_of_Institution',
                'Status_of_the_Institution', 'Structure_Name', 'Household_Category',
                'Structure_Institution_Occupied', 'Males_in_structure',
                'Females_in_structure', 'Number_of_Housing_Units',
                'interview__status', 'Total_Households', 'First_Head_Name',
                'Males_in_Household', 'Females_in_Household', 'Household_Population',
                'GeoLocation_Latitude', 'GeoLocation_Longitude', 'date']
    df = df[ref_cols]
    # ===========================================================
    # COMPARE WITH OUTPUT FROM SPSS BASED OUTPUT FOR SANITY CHECK
    # ============================================================

    return df


def create_df_without_pandas(file):
    """
    In case creating df with pandas fails due to encoding issues
    we try this manual approach
    :param file: CSV file with raw HH listing data
    :return:
    """
    data = []
    for line in open(file, 'rb'):
        try:
            # Decode to a fail-safe string for PY3
            line = line.decode('latin1')
            data.append(line.split(","))
        except Exception as e:
            pass

        try:
            # Decode to a fail-safe string for PY3
            line = line.decode('utf-8')
            data.append(line.split(","))
        except Exception as e:
            pass
    df = pd.DataFrame(data=data[1:], columns=data[0])
    return df


def check_if_coordinates_colums_need_fixing(df, cols):
    """
    Check if one of the coordinates columns (passed in pairs for lat, lon)
    requires fixing if they have null string values (e.g., #NULL#)
    """
    col_to_fix = []
    col_okay = []
    for l in cols:
        if df[l].dtype == np.float64:
            col_okay.append(l)
            continue
        col_to_fix.append(l)

    fix_tuple = None
    if not col_to_fix:
        pass
    else:
        fix_tuple = {"col_to_fix": col_to_fix[0], "replace_col": col_okay[0]}

    return fix_tuple


def convert_to_float(row, col_to_fix, replace_col):
    """
    Simply converts a string coordinate to float but also fix
    those fixes those NULL coordinate strings by replacing with other available coordinate
    """

    try:
        return float(row[col_to_fix])
    except Exception as e:
        return row[replace_col]


def fix_coordinates(df, cols):
    """
    For coordinate column which require fixing, we it here
    """
    to_fix_or_not_cols = check_if_coordinates_colums_need_fixing(df=df,
                                                                 cols=cols)
    if to_fix_or_not_cols:
        fix_col = to_fix_or_not_cols["col_to_fix"]
        replace_col = to_fix_or_not_cols["replace_col"]
        df[fix_col] = df.apply(convert_to_float, args=(fix_col, replace_col), axis=1)
        df[fix_col] = pd.to_numeric(df[fix_col])

    return df


def sanitize_and_separate_df_pois(df, struct_type_col, null_feat_cat_replacement,
                                  output_file_dwelling, output_file_pois, cols_to_keep_df,
                                  new_col_names_df, cols_to_keep_poi, new_col_names_poi,
                                  residential_struct_category):
    """
    Does some cleaning and then split residential points (dwelling frame (DF)
    from POIs
    :param df:
    :param struct_type_col: Column which has structure type categorization
    :param null_feat_cat_replacement: For features with no feature type category, value to replace
    :param output_file_dwelling: Output CSV filename for DFs
    :param output_file_pois: Output CSV filename for POIs
    :param cols_to_keep_df: For DF, which columns to keep as final
    :param new_col_names_df: new column names for DF
    :param cols_to_keep_poi: For POIs, which columns to keep as final
    :param new_col_names_poi: new column names for POIs
    :param residential_struct_category: category to use to separate residential (HHs/dwelling) from other structure
    :return: saves processed DF and POIs ward level CSV file
    """
    # ============================
    #  do some clean up
    # ============================
    # replace #NULL!  with missing
    df[struct_type_col].replace({"#NULL!": null_feat_cat_replacement}, inplace=True)
    # fix coordinates-replace missing coordinates
    df2 = fix_coordinates(df=df, cols=['GPSLocation__Longitude', 'GeoLocation_Longitude'])
    df3 = fix_coordinates(df=df2, cols=['GPSLocation__Latitude', 'GeoLocation_Latitude'])

    # ============================
    #  separate DFs and POIs
    # ============================
    df_dwellings = df[df[struct_type_col] == residential_struct_category]
    df_pois = df[df[struct_type_col] != residential_struct_category]

    # ============================
    #  process DFs
    # =============================
    df_dwellings = df_dwellings[cols_to_keep_df]
    df_dwellings.rename(columns=new_col_names_df, inplace=True)  # rename cols

    # check for duplicate coordinates and create HH ID
    num_rows_before = df_dwellings.shape[0]
    print(num_rows_before)
    df_dwellings.drop_duplicates(subset=["Latitude", "Longitude"], inplace=True)
    num_rows_after = df_dwellings.shape[0]
    print(num_rows_after)

    # ============================
    #  process POIs
    # ============================
    df_pois = df_pois[cols_to_keep_poi]
    df_pois.rename(columns=new_col_names_poi, inplace=True)  # rename cols
    # check for duplicate coordinates and create HH ID
    num_rows_before = df_pois.shape[0]
    print(num_rows_before)
    df_pois.drop_duplicates(subset=["Latitude", "Longitude"], inplace=True)
    num_rows_after = df_pois.shape[0]
    print(num_rows_after)

    # ============================
    #  do quick checks
    # ============================
    struct_type_col = new_col_names_df[struct_type_col]
    struct_vals_df = list(df_dwellings[struct_type_col].unique())
    struct_vals_poi = list(df_pois[struct_type_col].unique())

    # check that the DF dataframe only contains residential structures
    assert len(struct_vals_df) == 1
    assert struct_vals_df[0] == residential_struct_category

    if residential_struct_category in struct_vals_poi:
        print("Error with separation of DF and POIs")

    # ============================
    #  save files
    # ============================
    df_dwellings.to_csv(output_file_dwelling, index=False)
    df_pois.to_csv(output_file_pois, index=False)

    return df_dwellings, df_pois


def split_csv_into_wards(csv_file, ward_id_col, output_folder, suffix):
    """
    If CSV processed ward CSV ouputed from function "sanitize_and_separate_df_pois"
    split the CSV into ward level DF annd POI CSV files
    :param csv_file: either ward_df or ward_poi processed CSV file
    :param ward_id_col:
    :param output_folder:
    :param suffix:
    :return:
    """
    df = pd.read_csv(csv_file)
    wards = list(df[ward_id_col].unique())
    if len(wards) == 1:
        return

    for w in wards:
        dfw = df[df[ward_id_col] == w]
        outputdir = output_folder / w.upper()
        outputdir.mkdir(exist_ok=True)
        output_csv = outputdir.joinpath("{}_{}.csv".format(w.upper(), suffix))
        dfw.to_csv(output_csv, index=False)


def shpfile_from_csv(csv_file, crs, output_shp, project, lon, lat):
    """
    Given a CSV file, simply creates a shapefile
    :param csv_file: the CSV file to use
    :param crs: Coordinate Reference System (CRS) to use
    :param output_shp: Full path of output shapefile created
    :param lon, lat: longitude and latitude column in CSV
    :param project: whether to project or not
    :return: saves SHP typically ward level shapefile
    """

    df = pd.read_csv(csv_file)
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[lon], df[lat]))

    # manage projections
    gdf.crs = {'init': 'epsg:4326'}  # first set it to WGS84
    if project:
        gdf = gdf.to_crs(crs)  # project to UTM Zone 36

    # save the shp file
    gdf.to_file(output_shp)


def create_shp_for_each_ward(dir_with_ward_subdirs, crs, lon_col, lat_col):
    """
    Given a district directory with processed CSV files (DFs and POIs),
    for each ward, this function loops through Go through each ward and create shp files
    :param dir_with_ward_subdirs: Dir (e.g., district level) containing ward directories with processed CSV
    :param crs: crs for shapefiles
    :return: Saves ward shapefile to each ward folder within dir_with_ward_subdirs
    """
    for w in dir_with_ward_subdirs.iterdir():
        if w.is_dir():
            for f in w.iterdir():
                filename = f.parts[-1]
                if "poi" in filename or "df" in filename:
                    ward_name = w.parts[-1]
                    csv_df = os.path.abspath(w.joinpath("{}_df.csv".format(ward_name)))
                    shp_file_df = os.path.abspath(w.joinpath("{}_df.shp".format(ward_name)))

                    csv_poi = os.path.abspath(w.joinpath("{}_poi.csv".format(ward_name)))
                    shp_file_poi = os.path.abspath(w.joinpath("{}_poi.shp".format(ward_name)))

                    shpfile_from_csv(csv_file=csv_df, crs=crs, output_shp=shp_file_df, project=False, lon=lon_col,
                                     lat=lat_col)
                    shpfile_from_csv(csv_file=csv_poi, crs=crs, output_shp=shp_file_poi, project=False, lon=lon_col,
                                     lat=lat_col)


def append_building_attributes_to_ea(ea_shp, points_shp, agg_id, aggreg_func,
                                     add_struct_cnt_col, struct_cnt_col_name, crs):
    """
    Add columns with population count (from HH listing), building count from HH listing (DF and POI) and
     from satellite imagery building footprints to each EA for coverage
    check
    :param ea_shp: ward level EA shapefile to append attributes to
    :param points_shp: either DF, POIs or building footprints
    :param agg_id: EA col to use when aggregating attributes at EA level
    :param aggreg_func: Aggregate function (e.g., sum, count)
    :param add_struct_cnt_col: Helper column to help with aggregating
    :param struct_cnt_col_name: New column name to be added for structure count
    :param crs: CRS being used in this processing (WGS84)
    :return: Returns a dataframe with agg_id and corresponding attribute (e.g., StructCntHHs, HHPop)
    """
    # =====================================================
    # Check and fix projection to ensure they are the same
    # =====================================================
    shapes = {"points": points_shp, "ea": ea_shp}
    for k, s in shapes.items():
        this_crs = s.crs
        if this_crs != crs:
            s = s.to_crs(crs)
            shapes[k] = s

    # update shp files after fixing CRS
    ea_shp = shapes["ea"]
    points_shp = shapes["points"]

    # =====================================================
    # Add count column for lack of concise solution
    # =====================================================
    if add_struct_cnt_col:
        points_shp[struct_cnt_col_name] = 1

    ea_pts = gpd.sjoin(ea_shp, points_shp, how="inner", op="contains")
    df_grp = ea_pts.groupby([agg_id]).agg(aggreg_func)
    df = df_grp.reset_index()

    return df


def append_all_building_attributes_to_ea(ea_shp_file, building_footprints,
                                         hhlisting_dwellings, hhlisting_pois,
                                         ea_aggregation_id, output_ea_shpfile, crs_info):
    """
    A helper function which runs the
    "append_building_attributes_to_ea" function for ward level DF, POI and building footprints shp file
    :param ea_shp_file: ward level EA
    :param building_footprints: ward level building footprints point shp file
    :param hhlisting_dwellings: ward level DF shp file
    :param hhlisting_pois: ward level POIs shp file
    :param ea_aggregation_id: EA column for aggregating attributes
    :param output_ea_shpfile: Full path for updated shapefile after merging all attributes
    :return: Saves EA shp file to output_ea_shpfile
    """
    # =============================
    # Read shapefiles
    # =============================
    hh = gpd.read_file(hhlisting_dwellings)
    poi = gpd.read_file(hhlisting_pois)
    bldings = gpd.read_file(building_footprints)
    ea = gpd.read_file(ea_shp_file)

    # =========================================
    # Append Dwelling Frames Structure Count
    # =========================================
    df_func = {'HHPop': 'sum', 'StructCntHHs': 'sum'}  # aggregate total populationn and struct count
    ea_hh = append_building_attributes_to_ea(ea_shp=ea, points_shp=hh, agg_id=ea_aggregation_id,
                                             aggreg_func=df_func,
                                             add_struct_cnt_col=True, crs=crs_info,
                                             struct_cnt_col_name='StructCntHHs')

    # =========================================
    # Append POIs Structure Count
    # =========================================
    poi_func = {'StructCntPOIs': 'sum'}  # aggregate total populationn and struct count
    ea_pois = append_building_attributes_to_ea(ea_shp=ea, points_shp=poi, agg_id=ea_aggregation_id,
                                               aggreg_func=poi_func, crs=crs_info,
                                               add_struct_cnt_col=True,
                                               struct_cnt_col_name='StructCntPOIs')

    # =========================================
    # Append building footprints Structure Count
    # =========================================
    # delete n_HH is its already in the EA.shp
    ea_cols = list(ea.columns)
    if 'n_HH' in ea_cols:
        ea.drop(labels=['n_HH'], axis=1, inplace=True)
    bld_func = {'n_HH': 'sum'}  # aggregate total populationn and struct count

    ea_bld = append_building_attributes_to_ea(ea_shp=ea, points_shp=bldings,
                                              agg_id=ea_aggregation_id, crs=crs_info,
                                              aggreg_func=bld_func, struct_cnt_col_name=None,
                                              add_struct_cnt_col=False)
    ea_bld.rename(columns={'n_HH': 'StructCntBlds'}, inplace=True)

    # =========================================
    # Merge the three
    # =========================================
    ea_merged = ea_hh.merge(right=ea_pois, how="inner", on=ea_aggregation_id).merge(right=ea_bld, how='inner',
                                                                                    on=ea_aggregation_id)
    # this column gives total number of structures  based on HHListing
    # for both POIs and residential
    ea_merged['TotalStruct'] = ea_merged.StructCntHHs + ea_merged.StructCntPOIs

    # =========================================
    # Merge to EA shapefile and save to disk
    # =========================================
    ea_out = ea.merge(right=ea_merged, on=ea_aggregation_id, how='left')
    ea_out.to_file(output_ea_shpfile)

    # =========================================
    # Do some checks
    # =========================================
    # TODO
