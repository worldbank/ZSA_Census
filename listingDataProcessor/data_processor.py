"""
This module calls functions from utils to run data processing functions
"""
from pathlib import Path
import os
from ListingDataProcessor import utils_ver1 as ut


class DataProcessor:
    """
    Sets up key variables and runs processing jobs
    """

    def __init__(self, raw_csv_dir, csv_filename, ea_demarcation_dir, province, district, params):
        """

        :param raw_csv_dir: Directory containing raw CSVs coming from Survey Solutions (SS) API
        :param ea_demarcation_dir: directory with all EA demarcation
        :param province: Province being processed
        :param district: District being processed
        :param csv_filename: CSV file to process although this may not be necessary in some cases
        """
        self.raw_csv_dir = raw_csv_dir
        self.raw_csv_filename = csv_filename
        self.ea_demarcation_dir = ea_demarcation_dir
        self.province = province
        self.district = district
        self.dir_with_ward_subdirs = None
        self.params = params

    def create_ouput_files_raw_csv_processing(self):
        """
        Returns DF and POI processed CSV output full paths
        :return:
        """
        prov = self.province
        if self.district:
            df_name = "TMP_{}_District_processed_df.csv".format(self.district)
            poi_name = "TMP_{}_District_processed_poi.csv".format(self.district)
            output_dwellings = self.ea_demarcation_dir.joinpath(prov, self.district, df_name)
            output_poi = self.ea_demarcation_dir.joinpath(prov, self.district, poi_name)

            return {"output_dwellings": output_dwellings, "output_pois": output_poi}

    def prepare_io_files_for_ea_level_structures_summary(self, ward_name):
        prov = self.province
        if self.district:
            dist = self.district
            dist_dir = self.ea_demarcation_dir.joinpath(prov, dist)
            df = os.path.abspath(dist_dir.joinpath(ward_name, "{}_df.shp".format(ward_name)))
            poi = os.path.abspath(dist_dir.joinpath(ward_name, "{}_poi.shp".format(ward_name)))
            bld = os.path.abspath(dist_dir.joinpath(ward_name, "SIMULATED_HH_FAKE.shp"))
            return {"df_shp": df, "poi_shp": poi, "buildings": bld}

    def process_data(self):
        """
        Run a processing
        :return:
        """
        # ===========================================
        # CREATE PANDAS DATAFRAME FROM RAW CSV FILE
        # ===========================================
        df_raw = ut.create_df(self.raw_csv_filename)

        # ===========================================
        # GENERATE PROCESSED CSV
        # ===========================================
        output_dwellings = self.create_ouput_files_raw_csv_processing()['output_dwellings']
        output_pois = self.create_ouput_files_raw_csv_processing()['output_pois']

        ut.sanitize_and_separate_df_pois(df=df_raw, output_file_dwelling=output_dwellings,
                                         output_file_pois=output_pois,
                                         struct_type_col=self.params['struct_type_col'],
                                         null_feat_cat_replacement="missing",
                                         cols_to_keep_df=self.params['cols_df'],
                                         cols_to_keep_poi=self.params['cols_poi'],
                                         new_col_names_df=self.params[
                                             'new_names_df'],
                                         new_col_names_poi=self.params[
                                             'new_names_poi'],
                                         residential_struct_category=self.params[
                                             'res_struct_val'])
        # ===========================================
        # SPLIT CSV BY WARD
        # ===========================================
        if self.district:
            prov = self.province
            dist = self.district
            self.dir_with_ward_subdirs = self.ea_demarcation_dir.joinpath(prov, dist)

        ward_id_col = self.params['ward_id_col']
        ut.split_csv_into_wards(csv_file=output_dwellings, ward_id_col=ward_id_col,
                                output_folder=self.dir_with_ward_subdirs,
                                suffix="HH")
        ut.split_csv_into_wards(csv_file=output_pois, ward_id_col=ward_id_col,
                                output_folder=self.dir_with_ward_subdirs,
                                suffix="POI")

        # ===========================================
        # CREATE SHP FILES
        # ===========================================
        ut.create_shp_for_each_ward(self.dir_with_ward_subdirs, crs=self.params["crs"],
                                    lon_col=self.params["lon"], lat_col=self.params["lat"])

        # =========================================================
        # APPEND HH LISTING BASED POPULATION COUNT, STRUCTURE COUNT
        # AND BUILDING COUNTS FROM SATELLITE IMAGERY
        # ==========================================================
        self.append_building_attributes_to_ward_level_ea_shp()

    def append_building_attributes_to_ward_level_ea_shp(self):
        """
        Helper function to loop through all wards and append building attributes to EA shapefile
        :return:
        """
        for w in self.dir_with_ward_subdirs.iterdir():
            if w.is_dir():
                try:
                    df = os.path.abspath(w.joinpath("HH.shp"))
                    poi = os.path.abspath(w.joinpath("POI.shp"))
                    buildings_filename = self.params['ward_level_buildings_filename']
                    bld = os.path.abspath(w.joinpath("{}.shp".format(buildings_filename)))
                    ea_filename = self.params['ward_level_ea_filename']
                    ea_shp = os.path.abspath(w.joinpath("{}.shp".format(ea_filename)))

                    ut.append_all_building_attributes_to_ea(ea_shp_file=ea_shp,
                                                            building_footprints=bld,
                                                            hhlisting_dwellings=df,
                                                            hhlisting_pois=poi,
                                                            ea_aggregation_id=self.params[
                                                                'ea_aggregation_id'],
                                                            output_ea_shpfile=ea_shp, crs_info=self.params['crs'])
                except Exception as e:
                    print(e)
                    print("Failed to append attributes to ward shapefile {}".format(w.parts[-1]))
                    continue


def prepare_processing_parameters():
    """
    Edit this function manually to fix several parameters/constants
    POIs
    """
    columns_to_keep_df = ['PROV', 'DIST', 'CONS', 'WARD',
                          'REGION', 'SEA', 'LOCALITY', 'GPSLocation__Latitude',
                          'GPSLocation__Longitude',
                          'GPSLocation__Altitude', 'GPSLocation__Timestamp',
                          'Structure_type_categorisation', 'Structure_Name', 'Males_in_structure',
                          'Females_in_structure', 'Number_of_Housing_Units', 'Total_Households',
                          'First_Head_Name', 'Males_in_Household', 'Females_in_Household',
                          'Household_Population']

    columns_to_keep_poi = ['PROV', 'DIST', 'CONS', 'WARD',
                           'REGION', 'SEA', 'LOCALITY', 'GPSLocation__Latitude',
                           'GPSLocation__Longitude',
                           'GPSLocation__Altitude', 'GPSLocation__Timestamp',
                           'Structure_type_categorisation', 'Structure_Name',
                           'Multipurpose_Residential_Building',
                           'Multipurpose_Religious_Building',
                           'Multipurpose_Institutional_Building',
                           'Multipurpose_Commercial_Building',
                           'Religious_Building',
                           'Institutional_Building',
                           'Educational_Building',
                           'Commercial_Building',
                           'Health_Facility_Hospital_Health_Center',
                           'Ownership_of_Institution',
                           'Status_of_the_Institution']

    new_col_names_df = {'GPSLocation__Latitude': 'Latitude', 'GPSLocation__Longitude': 'Longitude',
                        'GPSLocation__Altitude': 'Altitude', 'GPSLocation__Timestamp': 'TimeStamp',
                        'Structure_type_categorisation': 'StructType', 'Structure_Name': 'StructName',
                        'Structure_Institution_occupied': 'StructInstOccup', 'Males_in_structure': 'StructMales',
                        'Females_in_structure': 'StructFemales', 'Number_of_Housing_Units': 'NumHHUnits',
                        'Total_Households': 'TotalHHs', 'First_Head_Name': 'HeadName',
                        'Males_in_Household': 'HHMales', 'Females_in_Household': 'HHFemales',
                        'Household_Population': 'HHPop'}

    new_col_names_poi = {'GPSLocation__Latitude': 'Latitude', 'GPSLocation__Longitude': 'Longitude',
                         'GPSLocation__Altitude': 'Altitude', 'GPSLocation__Timestamp': 'TimeStamp',
                         'Structure_type_categorisation': 'StructType', 'Structure_Name': 'StructName',
                         'Structure_Institution_occupied': 'StructInstOccup',
                         'Multipurpose_Residential_Building': 'MultResBld',
                         'Multipurpose_Religious_Building': 'MultReligBld',
                         'Multipurpose_Institutional_Building': 'MultInstBld',
                         'Multipurpose_Commercial_Building': 'MultCommBld',
                         'Religious_Building': 'ReligBld',
                         'Institutional_Building': 'InstBld',
                         'Educational_Building': 'EducBld',
                         'Commercial_Building': 'CommBld',
                         'Health_Facility_Hospital_Health_Center': 'HealthFacTyp',
                         'Ownership_of_Institution': 'OwnershipInst',
                         'Status_of_the_Institution': 'StatusInst'}
    cols_params = {"cols_df": columns_to_keep_df, "cols_poi": columns_to_keep_poi,
                   "new_names_df": new_col_names_df, "new_names_poi": new_col_names_poi}
    # Filenames
    ward_level_ea_shp = 'EA_original_do_not_edit'
    ward_level_buildings_footprints_filename = 'building_centroids'
    filenames = {'ward_level_buildings_filename': ward_level_buildings_footprints_filename,
                 'ward_level_ea_filename': ward_level_ea_shp}

    # Misc Params
    # used when aggregating building counts and populaiton at EA level
    crs = {'init': 'epsg:4326'}
    ward_id_col_in_ea_shp = 'WARD'
    struct_type_col = 'Structure_type_categorisation'
    ea_agg_col = 'GEOID'
    residential_struct_category_val = 'Residential Building'
    lat = "Latitude"
    lon = "Longitude"
    misc = {'ea_aggregation_id': ea_agg_col, "lon": lon, "lat": lat, 'ward_id_col': ward_id_col_in_ea_shp,
            'res_struct_val': residential_struct_category_val, "struct_type_col": struct_type_col, "crs": crs}

    return {**misc, **filenames, **cols_params}


def main():
    """
    Run data processor
    :return:
    """
    # ====================================================================
    # MANUALLY EDIT THE PARAMS IN "prepare_processing_parameters" function
    # ====================================================================
    processing_params = prepare_processing_parameters()

    # ====================================================================
    # PREPARE INPUT PATH (RAW CSVS) AND OUTPUT PATH (EA_DEMARCATION)
    # ====================================================================
    # please edit the path accordingly
    working_dir = Path.cwd().parents[2]
    raw_csv_dir = working_dir.joinpath("data", "lusakaProvince", "listingRawFiles", "Districts")
    ea_demarcation_dir = working_dir.joinpath("data", "DEMARCATION_DATA_UPDATED")

    # ====================================================================
    # CREATE DATA PROCESSOR OBJECT AND RUN JOB
    # ====================================================================
    district_csv_files = [d for d in raw_csv_dir.iterdir() if d.suffix == ".csv"]
    for csv_file in district_csv_files:
        prov = "LUSAKA"
        dist = csv_file.parts[-1][:-4].upper()
        print("WORKING ON DISTRICT: {}".format(dist))
        dp = DataProcessor(raw_csv_dir=raw_csv_dir, csv_filename=csv_file, ea_demarcation_dir=ea_demarcation_dir,
                       province=prov, district=dist, params=processing_params)
        dp.process_data()


if __name__ == '__main__':
    main()
