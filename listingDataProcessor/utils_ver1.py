"""
Miscellaneous data processing utility functions.
"""
import os
import pandas as pd
import geopandas as gpd
import numpy as np
from fuzzywuzzy import fuzz


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

def identify_mismatched_ward(wards_hh_listing, ward_name_in_dir):
    """
    For cases where the ward in the HH listing data has no match in ea demarcation folder
    we attemopt to find its match
    """
    fuzzy_scores = {}
    for w in wards_hh_listing:
        w2 = w.lower().replace(" ", "")
        ward_name2 = ward_name_in_dir.lower().replace(" ", "")
        r = fuzz.ratio(ward_name2, w2)
        fuzzy_scores[w] = {"score": r, "ward_name": ward_name_in_dir}
        if ward_name2 in w2:
            return {"ward_name_hh": w, "ward_name_dir": ward_name_in_dir}

    max_score = 0
    out = None
    for k, v in fuzzy_scores.items():
        if v["score"] > max_score:
            max_score = v["score"]
            out = {"ward_name_hh": k, "ward_name_dir": v["ward_name"]}

    return out

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
    df_dwellings = df3[df3[struct_type_col] == residential_struct_category]
    df_pois = df3[df3[struct_type_col] != residential_struct_category]

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
    wards_hh_listing = list(df[ward_id_col].unique())
    wards_dirs = [w for w in output_folder.iterdir() if w.is_dir()]

    if len(wards_hh_listing) == 1:
        return

    for w in wards_dirs:
        if w.is_dir():
            output_csv = w.joinpath("{}.csv".format(suffix))
            ward_name = w.parts[-1].capitalize()
            if ward_name in wards_hh_listing:
                dfw = df[df.WARD == ward_name]
                dfw.to_csv(output_csv, index=False)
            else:
                res = identify_mismatched_ward(wards_hh_listing=wards_hh_listing, ward_name_in_dir=ward_name)
                dfw = df[df.WARD == res["ward_name_hh"]]
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
        try:
            for f in w.iterdir():
                filename = f.parts[-1]
                if filename == "POI.csv":
                    shp_file_poi = os.path.abspath(w.joinpath("POI.shp"))
                    shpfile_from_csv(csv_file=f, crs=crs, output_shp=shp_file_poi, project=False, lon=lon_col,
                                     lat=lat_col)
                if filename == "HH.csv":
                    shp_file_hh = os.path.abspath(w.joinpath("HH.shp"))
                    shpfile_from_csv(csv_file=f, crs=crs, output_shp=shp_file_hh, project=False, lon=lon_col,
                                     lat=lat_col)
        except Exception as e:
            continue


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
    hh.replace(to_replace="#NULL!", value=0, inplace=True)
    hh["TotalHHs"] = pd.to_numeric(hh["TotalHHs"], errors='coerce')
    hh["HHPop"] = pd.to_numeric(hh["HHPop"], errors='coerce')
    poi = gpd.read_file(hhlisting_pois)
    bldings = gpd.read_file(building_footprints)
    ea = gpd.read_file(ea_shp_file)

    # =========================================
    # Append Dwelling Frames Structure Count
    # =========================================
    df_func = {'HHPop': 'sum', 'StructCntHHs': 'sum', "TotalHHs": "sum"}  # aggregate total populationn and struct count
    ea_hh = append_building_attributes_to_ea(ea_shp=ea, points_shp=hh, agg_id=ea_aggregation_id,
                                             aggreg_func=df_func,
                                             add_struct_cnt_col=True, crs=crs_info,
                                             struct_cnt_col_name='StructCntHHs')

    # =========================================
    # Append POIs Structure Count
    # =========================================
    poi_func = {'StructCntPOIs': 'sum'}  # aggregate total populationn and struct count
    ea_pois = append_building_attributes_to_ea(ea_shp=ea, points_shp=poi, agg_id=ea_aggregation_id,
                                               aggreg_func=poi_func,crs=crs_info,
                                               add_struct_cnt_col=True,
                                               struct_cnt_col_name='StructCntPOIs')

    # =========================================
    # Append building footprints Structure Count
    # =========================================
    # delete n_HH is its already in the EA.shp
    bld_func = {'StructCntBlds': 'sum'}  # aggregate total populationn and struct count

    ea_bld = append_building_attributes_to_ea(ea_shp=ea, points_shp=bldings,
                                              agg_id=ea_aggregation_id,crs=crs_info,
                                              aggreg_func=bld_func, struct_cnt_col_name='StructCntBlds',
                                              add_struct_cnt_col=True)

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
