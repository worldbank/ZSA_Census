"""
For each province, the script processes each district and makes the following updates in
each ward directory
1. creates HH.shp - has points for all households in the ward
2. creates PO.shp - has POIs
3. Updated EA_original_do_not_edit.shp by adding the following columns
    - HHPop, TotalHHs,
"""
import os
from ListingDataProcessor import utils as ut
from pathlib import Path
import geopandas as gpd
import pandas as pd

# =====================================
# USEFUL CONSTANTS
# ===================================
CRS = {'init': 'epsg:4326'}
LAT = "GPSLocation__Latitude"
LON = "GPSLocation__Longitude"
RES_STRUCT_VAL = 'Residential Building'
AGG_ID = "GEOID"  # id for aggregating household points at EA level

# ==========================================
# SET UP WORKING DIRECTORIES
# ==========================================
# this is the folder with general inputs such as the Frames excel file
DEMARCATION_INPUT_FILES_DIR = Path("/Users/dmatekenya/Google-Drive/WBG/Zambia/data/EADemarcationInputFiles")

# this is the folder with csv files processed from Survey Solutions data
HH_LISTING_INPUT = Path("/Users/dmatekenya/Google-Drive/WBG/Zambia/data/FromSurveySolutionsPythonProcessed")

# frames file mappings between geographic names and codes (e.g., Province name and codes)
FRAMES_FILE = DEMARCATION_INPUT_FILES_DIR.joinpath('FinalFrame-24-01-2020.csv')

# directory with ea demarcation files to be updated
EA_DEMARCATION_DIR = Path("/Users/dmatekenya/Google-Drive/WBG/Zambia/data/DEMARCATION_DATA_UPDATED")

# directory to dump intermediate processing files
INTERMEDIATE_OUTPUTS_DIR = Path("/Users/dmatekenya/Google-Drive/WBG/Zambia/data/HHListingIntermediateOutputs")


def pick_province_to_process(csv_frames, csv_hh, min_points_to_process=10000):
    """
    """
    # get latest CSV file

    df = pd.read_csv(csv_hh)
    df_frame = pd.read_csv(csv_frames)

    # Get provinces in data
    df = pd.DataFrame(df.PROV.value_counts())
    df['ProvId'] = df.index
    df_sorted = df.sort_values(by=["PROV"], ascending=False)

    # Keep only provinces with points above min_points_to_process
    df_sorted = df_sorted[df_sorted.PROV > min_points_to_process]

    # Return all provinces for processing
    df_frame = df_frame[["Prov_Name", "Prove_Code"]]
    df_frame.drop_duplicates(subset=["Prove_Code"], inplace=True)
    prov_name_code = df_frame.to_dict(orient="records")

    provs = list(df_sorted.ProvId.values)

    prov_to_process = []
    for p in provs:
        for item in prov_name_code:
            if item['Prove_Code'] == p:
                prov_to_process.append({'Prov_Name': item['Prov_Name'], 'Prove_Code': p})

    return prov_to_process


def grab_prov_level_shp_files(input_dir, prov_name, file_type):
    # grab buildings
    bld_dir = input_dir.joinpath("BuildingsByProvince")
    bld_file = None
    for f in bld_dir.iterdir():
        if f.suffix == ".shp":
            prov = f.parts[-1][9:-4]
            if prov.lower() == prov_name.lower():
                bld_file = os.path.abspath(f)

    # grab EAs
    ea_dir = input_dir.joinpath("EAsByProvince")
    ea_file = None
    for f in ea_dir.iterdir():
        if f.suffix == ".shp":
            prov = f.parts[-1][11:-4]
            if prov.lower() == prov_name.lower():
                ea_file = os.path.abspath(f)

    if file_type == "ea":
        return ea_file
    elif file_type == "buildings":
        return bld_file


def append_all_building_attributes_to_ea(ea_shp_file, bldings_file,
                                         hhlisting_dwellings, hhlisting_pois,
                                         ea_aggregation_id):
    """
    A helper function which runs the
    "append_building_attributes_to_ea" function
    """
    # =============================
    # Read shapefiles
    # =============================
    hh = hhlisting_dwellings
    bldings = gpd.read_file(bldings_file)
    poi = hhlisting_pois
    ea = gpd.read_file(ea_shp_file)

    # =========================================
    # Append Dwelling Frames Structure Count
    # =========================================
    df_func = {'Household_': 'sum', 'StructCntHHs': 'sum',
               'Total_Hous': "sum"}  # aggregate total populationn and struct count
    ea_hh = ut.append_building_attributes_to_ea(ea_shp=ea, points_shp=hh, agg_id=ea_aggregation_id,
                                                aggreg_func=df_func,
                                                add_struct_cnt_col=True,
                                                struct_cnt_col_name='StructCntHHs', preferred_crs=CRS)
    ea_hh.rename(columns={'Household_': "HHPop", 'Total_Hous': "TotalHHs"}, inplace=True)

    # =========================================
    # Append POIs Structure Count
    # =========================================
    poi_func = {'StrCntPOIs': 'sum'}  # aggregate total populationn and struct count
    ea_pois = ut.append_building_attributes_to_ea(ea_shp=ea, points_shp=poi, agg_id=ea_aggregation_id,
                                                  aggreg_func=poi_func,
                                                  add_struct_cnt_col=True,
                                                  struct_cnt_col_name='StrCntPOIs', preferred_crs=CRS)

    # =========================================
    # Append building footprints Structure Count
    # =========================================
    # delete n_HH is its already in the EA.shp
    ea_cols = list(ea.columns)
    if 'n_HH' in ea_cols:
        ea.drop(labels=['n_HH'], axis=1, inplace=True)
    bld_func = {'StrCntBlds': 'sum'}  # aggregate total populationn and struct count

    ea_bld = ut.append_building_attributes_to_ea(ea_shp=ea, points_shp=bldings,
                                                 agg_id=ea_aggregation_id,
                                                 aggreg_func=bld_func, struct_cnt_col_name='StrCntBlds',
                                                 add_struct_cnt_col=True, preferred_crs=CRS)

    return ea_hh, ea_bld, ea_pois


def update_ward_demarcation_files(ward_dir, prov_level_poi_shp, prov_level_hh_shp, ea_aggregation_id):
    """
    Updates ward files
    """
    # ===========================
    # GET NECESSARY FILES
    # ===========================
    ea = os.path.abspath((ward_dir.joinpath("EA_original_do_not_edit.shp")))
    bld = os.path.abspath(ward_dir.joinpath("building_centroids.shp"))

    # ============================================
    # CREATE WARD LEVEL SHAPEFILE FOR HH AND POIS
    # ============================================
    out_hh_shp = os.path.abspath(ward_dir.joinpath("HH.shp"))
    out_poi_shp = os.path.abspath(ward_dir.joinpath("POI.shp"))

    hh_prov_pts = gpd.read_file(prov_level_hh_shp)
    poi_prov_pts = gpd.read_file(prov_level_poi_shp)
    ut.extract_points_within_geographic_region(points_shp=hh_prov_pts, polygon_shp=ea, output_prov_shp=out_hh_shp,
                                               preferred_crs=CRS)
    ut.extract_points_within_geographic_region(points_shp=poi_prov_pts, polygon_shp=ea,
                                               output_prov_shp=out_poi_shp, preferred_crs=CRS)

    # ================================================
    # Update EA with building and Households counts
    # ================================================
    hh_ward_pts = gpd.read_file(out_hh_shp)
    poi_ward_pts = gpd.read_file(out_poi_shp)
    ea_hh, ea_bld, ea_pois = append_all_building_attributes_to_ea(ea_shp_file=ea, bldings_file=bld,
                                                  hhlisting_dwellings=hh_ward_pts,
                                                  hhlisting_pois=poi_ward_pts,
                                                  ea_aggregation_id=AGG_ID)

    ea_merged = ea_hh.merge(right=ea_pois, how="inner", on=ea_aggregation_id).merge(right=ea_bld, how='inner',
                                                                                    on=ea_aggregation_id)
    df_ea = gpd.read_file(ea)
    df_ea2 = df_ea.merge(right=ea_merged, on=ea_aggregation_id, how="left")
    # ================================================
    # SAVE UPDATED EA SHP FILE
    # ================================================
    df_ea2.to_file(ea)


def update_province_ea_demarcation_files(ea_demarcation_dir, intermediate_outputs_dir,
                                         prov_name, csv_hh, csv_poi, prov_level_ea_shp):
    # ======================================
    # CREATE PROVINCE LEVEL HH AND POI SHP
    # =====================================
    # create directory for this province if it doesnt exit
    dirname = intermediate_outputs_dir.joinpath(prov_name)
    dirname.mkdir(parents=True, exist_ok=True)
    out_hh_shp = os.path.abspath(dirname.joinpath("{}_HH.shp".format(prov_name)))
    out_poi_shp = os.path.abspath(dirname.joinpath("{}_POI.shp".format(prov_name)))
    # ut.extract_points_within_geographic_region(csv_file=csv_hh, polygon_shp=prov_level_ea_shp,
    #                                            output_prov_shp=out_hh_shp, lat=LAT, lon=LON, preferred_crs=CRS)
    # ut.extract_points_within_geographic_region(csv_file=csv_poi, polygon_shp=prov_level_ea_shp,
    #                                            output_prov_shp=out_poi_shp, lat=LAT, lon=LON, preferred_crs=CRS)

    # =====================================
    # PROCESS ALL DISTRICTS AND WARDS
    # ====================================
    # get list of districts
    districts = [i for i in ea_demarcation_dir.joinpath(prov_name).iterdir() if i.is_dir()]

    for d in districts:
        try:
            wards = [i for i in d.iterdir() if i.is_dir()]
            for w in wards:
                update_ward_demarcation_files(ward_dir=w, prov_level_poi_shp=out_poi_shp,
                                              prov_level_hh_shp=out_hh_shp, ea_aggregation_id=AGG_ID)
        except Exception as e:
            pass


def process_all_provinces():
    """
    Given latest HH Listing data coming from Survey Solutions,
    this function processes all provinces with enough data
    """

    # ==========================================
    # GET PROVINCES TO PROCESS
    # ==========================================
    # TODO: pick the latest file in the folder
    hh_csv = HH_LISTING_INPUT.joinpath("from_RestOfProvinces_2020-03-03_HH.csv")
    poi_csv = HH_LISTING_INPUT.joinpath("from_RestOfProvinces_2020-03-03_POI.csv")
    provs = pick_province_to_process(csv_frames=FRAMES_FILE, csv_hh=hh_csv, min_points_to_process=10000)

    # ==========================================
    # PROCESS PROVINCES
    # ==========================================
    for p in provs:
        province_name = p["Prov_Name"]
        if province_name != "LUAPULA":
            continue
        prov_level_EAs_shp = grab_prov_level_shp_files(input_dir=DEMARCATION_INPUT_FILES_DIR, file_type="ea",
                                                       prov_name=province_name)
        update_province_ea_demarcation_files(ea_demarcation_dir=EA_DEMARCATION_DIR,
                                             intermediate_outputs_dir=INTERMEDIATE_OUTPUTS_DIR,
                                             prov_name=province_name, csv_hh=hh_csv, csv_poi=poi_csv,
                                             prov_level_ea_shp=prov_level_EAs_shp)


if __name__ == '__main__':
    process_all_provinces()
