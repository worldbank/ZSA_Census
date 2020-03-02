""""
Processes raw Survey Solutions data to create two main CSV files
1. POI.csv
2. HH.csv
The process creates intermediate files.
"""

from ListingDataProcessor import utils as ut

# ==========================================
# SET UP WORKING DIRECTORIES
# ==========================================
INPUT_DIR_WITH_ZIP_FILES = None  # should name like "FromSurveySolutionsRaw"
OUTPUT_DIR = None  # should have name like "FromSurveySolutionsPythonProcessed"


def grab_folders_to_process(inputdir):
    """
    Since the folder downloads all raw Survey Solutions data, this script
    returns the latest file based on data
    """

    # ==========================================
    # GET LATEST ZIP FILES
    # ==========================================
    all, cplblt = ut.get_latest_zip_files_metadata(input_dir=inputdir)
    latest_file_all = ut.retrieve_complete_file_downloads(files=all, look_back_days=5)
    latest_file_cpblt = ut.retrieve_complete_file_downloads(files=cplblt, look_back_days=5)

    # ==========================================
    # EXTRACT THE ZIP FILES
    # IN THE SAME DIRECTORY
    # ==========================================
    ut.extract_file(latest_file_all)
    ut.extract_file(latest_file_cpblt)

    # ==========================================
    # RETURN FOLDERS TO PROCESS
    # ==========================================
    return {"all_provs": latest_file_all.parts[-1][:-4], "cpblt": latest_file_cpblt.parts[-1][:-4]}


def create_top_level_csv_file(tab_files_dir_cpblt, tab_files_dir_all_prov, output_csv_dir):
    """
    Call the the proces_tabs functions which creates a CSV file with both POIs and HHs
    """
    # ===================================
    # RUN FOR COPPERBELT
    # ===================================
    cpblt_tab_file_names = {'final': 'Copperbelt_Data.tab', "hh_units": 'HOUSINGUNITS.tab',
                            'hh_roster': 'HHROSTER.tab', 'add_roster': 'ADDHHROSTER.tab',
                            'add_struct': 'ADDTIONSTRUCTURES.tab'}
    csv_filename_cpblt = ut.process_tab_files(tab_file_names=cpblt_tab_file_names,
                                              output_csv_dir=output_csv_dir,
                                              dir_with_tab_files=tab_files_dir_cpblt,
                                              from_which_download="Copperbelt")

    # ===================================
    # RUN FOR REST OF PROVINCES
    # ===================================
    tab_file_names = {'final': 'Final.tab', "hh_units": 'HOUSINGUNITS.tab',
                      'hh_roster': 'HHROSTER.tab', 'add_roster': 'ADDHHROSTER.tab',
                      'add_struct': 'ADDTIONSTRUCTURES.tab'}
    csv_filename_all = ut.process_tab_files(tab_file_names=tab_file_names,
                                            output_csv_dir=output_csv_dir,
                                            dir_with_tab_files=tab_files_dir_all_prov,
                                            from_which_download="RestOfProvinces")

    # ===================================
    # RETURN LATEST CSV FILENAMES
    # ===================================
    return {"csv_cpblt": csv_filename_cpblt, "csv_all": csv_filename_all}


def main():
    # ========================================
    # PROCESS TAB FILES TO CREATE FIRST CSV
    # ========================================
    # get folders to process
    latest_folders = grab_folders_to_process(inputdir=INPUT_DIR_WITH_ZIP_FILES)
    dir_to_process_all_provs = latest_folders["all_provs"]
    dir_to_process_cpblt = latest_folders["cpblt"]

    # process tabs
    res = create_top_level_csv_file(tab_files_dir_cpblt=dir_to_process_cpblt,
                                    tab_files_dir_all_prov=dir_to_process_all_provs, output_csv_dir=OUTPUT_DIR)

    # ========================================
    # PROCESS TO SPLIT BETWEEN HHS AND POIS
    # ========================================
    # set processing params
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

    # run the function for Copperbelt
    csv_file_cpblt = OUTPUT_DIR.joinpath(res["csv_cpblt"])
    ut.extract_structures_with_households(survey_solutions_python_processed_csv=csv_file_cpblt,
                                          cols_to_keep_hh=cols_to_keep_hh, cols_to_keep_pois=cols_to_keep_POI,
                                          out_csv_dir=OUTPUT_DIR)

    # run the function for Copperbelt
    csv_file_all = OUTPUT_DIR.joinpath(res["csv_all"])
    ut.extract_structures_with_households(survey_solutions_python_processed_csv=csv_file_all,
                                          cols_to_keep_hh=cols_to_keep_hh, cols_to_keep_pois=cols_to_keep_POI,
                                          out_csv_dir=OUTPUT_DIR)


if __name__ == '__main__':
    main()
