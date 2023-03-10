from multiprocessing import Pool

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import Point, Polygon, mapping

# Read taxi zone polygon data
taxi_zone_df = pd.read_csv("./data/supplementary/nyc_taxi_zones.csv", engine="pyarrow")

# Build a dictionary of taxi zone polygons: {zone_id: polygon}
taxi_zone_polygons = dict()
for i, row in taxi_zone_df.iterrows():
    poly = Polygon(eval(row["the_geom"])["coordinates"][0][0])
    zone_id = row["location_id"]
    taxi_zone_polygons[zone_id] = poly

# Read borough boundaries
# No `pyarrow` engine used. Due to error:
# ArrowInvalid: straddling object straddles two block boundaries (try to increase block size?)
borough_boundary_df = pd.read_csv("./data/supplementary/nyc_borough_boundaries.csv")
borough_boundary_df["the_geom"] = borough_boundary_df["the_geom"].apply(wkt.loads)

# Build a dictionary of borough boundaries: {borough_name: polygon}
borough_polygons = dict()
for i, row in borough_boundary_df.iterrows():
    poly = row["the_geom"]
    boro_name = row["BoroName"]
    borough_polygons[boro_name] = poly

# Build a dictionary of borough boundaries: {borough_name: set(zone_id)}
borough_zone_ids = (
    taxi_zone_df.groupby("borough")["location_id"].agg(set=set).to_dict()["set"]
)

# Read census tracts
census_tracts_df = pd.read_csv(
    "./data/supplementary/nyc_census_tracts_2020.csv", engine="pyarrow"
)
census_tracts_df["the_geom"] = census_tracts_df["the_geom"].apply(wkt.loads)

# Build a dictionary of census tracts: {index: polygon}
census_tract_polygons = dict()
for i, row in census_tracts_df.iterrows():
    poly = row["the_geom"]
    census_tract_polygons[i] = poly

# Build a dictionary of census tracts in Manhattan: {index: polygon}
manhanttan_tract_polys = {
    tract_idx: poly
    for tract_idx, poly in census_tract_polygons.items()
    if poly.intersection(borough_polygons["Manhattan"])
}

# Create a GeoJSON
geo_objs = {idx: mapping(poly) for idx, poly in manhanttan_tract_polys.items()}
geo_dict = {}
geo_dict["type"] = "FeatureCollection"
geo_dict["features"] = [
    {"type": "Feature", "id": idx, "geometry": geo_obj}
    for idx, geo_obj in geo_objs.items()
]

# # Read street centerlines
# street_centerlines_df = gpd.read_file("./data/supplementary/nyc_street_centerlines.geojson")

# # Build a street centerline object for point snapping
# street_centerline_union = (
#     street_centerlines_df["geometry"].sample(50000).geometry.unary_union
# )

print("[DEBUG] utils.py: Finished loading supplementary data to memory.")


def coordinate_to_zone(coords):
    """Converts a (long, lat) to Taxi Zone ID"""
    point = Point(*coords)
    for zone_id, poly in taxi_zone_polygons.items():
        if poly.contains(point):
            return zone_id
    return 0


def batch_coordinate_to_zone(
    df, longitude_header="pickup_longitude", latitude_header="pickup_latitude"
):
    """Converts a dataframe with longitudes and latitudes to a Series of Taxi Zone ID"""
    return df.apply(
        lambda row: coordinate_to_zone((row[longitude_header], row[latitude_header])),
        axis=1,
    )


def coordinate_to_borough(coords):
    """Converts a (long, lat) to borough name"""
    point = Point(*coords)
    for boro_name, poly in borough_polygons.items():
        if poly.contains(point):
            return boro_name
    return ""


def batch_coordinate_to_borough(
    df, longitude_header="pickup_longitude", latitude_header="pickup_latitude"
):
    """Converts a dataframe with longitudes and latitudes to a Series of borough names"""
    return df.apply(
        lambda row: coordinate_to_borough(
            (row[longitude_header], row[latitude_header])
        ),
        axis=1,
    )


def coordinate_in_manhattan(coords):
    """Check if a (long, lat) is in Manhattan"""
    point = Point(*coords)
    poly = borough_polygons["Manhattan"]
    if poly.contains(point):
        return True
    return False


def batch_coordinate_in_manhattan(
    df, longitude_header="pickup_longitude", latitude_header="pickup_latitude"
):
    """Check if a batch of (long, lat) coordinates are in Manhattan"""
    return df.apply(
        lambda row: coordinate_in_manhattan(
            (row[longitude_header], row[latitude_header])
        ),
        axis=1,
    )


def coordinate_to_census_tract(coords):
    """Converts a (long, lat) to Census Tract Index"""
    point = Point(*coords)
    for census_track_idx, poly in census_tract_polygons.items():
        if poly.contains(point):
            return census_track_idx
    return -1


def batch_coordinate_to_census_tract(
    df, longitude_header="pickup_longitude", latitude_header="pickup_latitude"
):
    """Converts a dataframe with longitudes and latitudes to a Series of Census Tract Index"""
    return df.apply(
        lambda row: coordinate_to_census_tract(
            (row[longitude_header], row[latitude_header])
        ),
        axis=1,
    )


# def snap_point_to_roads(coords):
#     """Snaps a (long, lat) coordinate to the nearest road centerline"""
#     # Reference: https://gis.stackexchange.com/a/306915
#     assert len(coords) == 2
#     assert (
#         coords[0] < 0 and coords[1] > 0
#     ), "Coordinates should be in (lon, lat) format."

#     point = Point(*coords)
#     snapped_point = street_centerline_union.interpolate(
#         street_centerline_union.project(point)
#     )

#     return [snapped_point.x, snapped_point.y]


# def batch_snap_point_to_roads(coords_list):
#     """Snaps a list of (long, lat) coordinates to their nearest road centerlines"""
#     return np.array([snap_point_to_roads(coords) for coords in coords_list])


def parallel_proc(df, func, n_cores=16):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    return df


def vectorize_popularity(popular_times_list):
    """Given a list of popular times of a week for a location, convert
    the popularity into a 2D vectory of dimension [7, 24]

    Args:
        list[dict]
            A list of dictionaries containing two keys: `name` and `data`,
            where the values for `name` is a day of the week, and the values
            for `data` is a 1D list of length 24 representing the hourly popularity
            of the day.
    Return:
        ndarray
            2D array of shape [7, 24] containing data with `int` type.
    """
    pt_vec = []
    for weekday in popular_times_list:
        pt_vec.append(weekday["data"])
    pt_vec = np.vstack(pt_vec)
    return pt_vec
