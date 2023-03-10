import argparse
import json
import os
import shutil

import gdown
import geopandas as gpd
import numpy as np
import pandas as pd

RAW_DATA_ROOT = "./raw"
SAMPLE_DATA_ROOT = "./sample"
CLEAN_DATA_ROOT = "./clean"

RATE_CODES = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
}


def main(args):
    data_name = args.data
    create_sample = args.create_sample

    print(
        "[INFO] Preparing to clean {} data...".format(
            data_name.replace("_", " ").title()
        )
    )

    if not os.path.exists(RAW_DATA_ROOT):
        os.makedirs(RAW_DATA_ROOT)

    if create_sample and not os.path.exists(SAMPLE_DATA_ROOT):
        os.makedirs(SAMPLE_DATA_ROOT)

    # Taxi Trip Data
    if data_name in ["taxi", "all"]:
        taxi_2014_file_name = "2014_nyc_taxi_data.csv.gz"
        taxi_2021_file_name = "2021_nyc_taxi_data.csv"
        taxi_2014_file_path = os.path.join(RAW_DATA_ROOT, taxi_2014_file_name)
        taxi_2021_file_path = os.path.join(RAW_DATA_ROOT, taxi_2021_file_name)

        if not os.path.exists(taxi_2014_file_path):
            print("[INFO] Downloading raw file to: {}".format(taxi_2014_file_path))

            # Download 2014 Taxi Trip data from Google Drive
            file_id = "1NntaPBRM7-z99H0KHSVm_u1O4KY6TrTt"
            gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
            gdown.download(gdrive_url, taxi_2014_file_path, quiet=False)

        else:
            print("[INFO] Loading existing raw file: {}".format(taxi_2014_file_path))

        df = pd.read_csv(taxi_2014_file_path, engine="pyarrow")
        process_taxi_2014_data(df, create_sample)

        if not os.path.exists(taxi_2021_file_path):
            print("[INFO] Downloading raw file to: {}".format(taxi_2021_file_path))

            # Download 2021 Taxi Trip data from Google Drive
            file_id = "1dSx0hR6oJfeGZdZ2CQVsh1S6SjbRXtKf"
            gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
            gdown.download(gdrive_url, taxi_2021_file_path, quiet=False)

        else:
            print("[INFO] Loading existing raw file: {}".format(taxi_2021_file_path))

        df = pd.read_csv(taxi_2021_file_path, engine="pyarrow")
        process_taxi_2021_data(df, create_sample)

    # Popular Times Data
    if data_name in ["popular_times", "all"]:
        popular_times_file_name = "merged_popular_times.json"
        popular_times_file_path = os.path.join(RAW_DATA_ROOT, popular_times_file_name)

        if not os.path.exists(popular_times_file_path):
            print("[INFO] Downloading raw file to: {}".format(popular_times_file_path))
            # Download Popular Times data from Google Drive
            file_id = "1Qv-IS9idWOEhJS_VyInhyfMjiUa5bOj6"
            gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
            gdown.download(gdrive_url, popular_times_file_path, quiet=False)

        else:
            print(
                "[INFO] Loading existing raw file: {}".format(popular_times_file_path)
            )

        pt_file = open(popular_times_file_path)
        j_data = json.load(pt_file)
        process_popular_times_data(j_data, create_sample)

    # Land Use Data
    if data_name in ["land_use", "all"]:
        land_use_folder_name = "nyc_mappluto_22v2_shp"
        land_use_folder_path = os.path.join(RAW_DATA_ROOT, land_use_folder_name)
        shp_file_name = "MapPLUTO.shp"
        shp_file_path = os.path.join(land_use_folder_path, shp_file_name)

        if not os.path.exists(land_use_folder_path):
            print("[INFO] Downloading raw file to: {}".format(shp_file_path))

            # Download land use data from Google Drive and unzip it
            file_id = "1llEQCWCMqF6kkmXVAAb50TgeRmsv9HIw"
            gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
            zip_name = os.path.join(RAW_DATA_ROOT, "temp.zip")
            gdown.download(gdrive_url, zip_name, quiet=False)
            shutil.unpack_archive(zip_name, land_use_folder_path)
            os.remove(zip_name)

        else:
            print("[INFO] Loading existing raw file: {}".format(shp_file_path))

        land_use_df = gpd.read_file(shp_file_path)
        process_land_use_data(land_use_df, create_sample)


def _batch_coordinate_in_manhattan_pickup(df):
    from utils import batch_coordinate_in_manhattan

    return batch_coordinate_in_manhattan(df)


def _batch_coordinate_in_manhattan_dropoff(df):
    from utils import batch_coordinate_in_manhattan

    return batch_coordinate_in_manhattan(
        df, longitude_header="dropoff_longitude", latitude_header="dropoff_latitude"
    )


def _batch_coordinate_to_zone_pickup(df):
    from utils import batch_coordinate_to_zone

    return batch_coordinate_to_zone(df)


def _batch_coordinate_to_zone_dropoff(df):
    from utils import batch_coordinate_to_zone

    return batch_coordinate_to_zone(
        df, longitude_header="dropoff_longitude", latitude_header="dropoff_latitude"
    )


def _batch_coordinate_to_census_tract_pickup(df):
    from utils import batch_coordinate_to_census_tract

    return batch_coordinate_to_census_tract(df)


def _batch_coordinate_to_census_tract_dropoff(df):
    from utils import batch_coordinate_to_census_tract

    return batch_coordinate_to_census_tract(
        df, longitude_header="dropoff_longitude", latitude_header="dropoff_latitude"
    )


def process_taxi_2014_data(df, create_sample=False):
    clean_data_filename = "manhattan_taxi_2014_nov.csv"
    clean_data_path = os.path.join(CLEAN_DATA_ROOT, clean_data_filename)

    if not os.path.exists(clean_data_path):
        print("[INFO] Cleaning 2014 Taxi Trip data...")
        from utils import parallel_proc

        sample_size = 800000

        print("[INFO] Updating column headers.")
        # Unify column headers
        df = df.rename(columns={"vendor_id": "vendor"})

        print("[INFO] Dropping invalid rows.")
        # Select only data from November 2014
        df = df[df["pickup_datetime"].dt.month == 11]
        df = df[df["dropoff_datetime"].dt.month == 11]

        # Drop rows with NaN
        df = df.dropna()

        # Drop rows with incorrect values
        df = df[(df["vendor"] == "CMT") | (df["vendor"] == "VTS")]
        df = df[df["passenger_count"] > 0]
        df = df[df["trip_distance"] > 0]
        df = df[(df["rate_code"] >= 1) & (df["rate_code"] <= 6)]
        df = df[(df["store_and_fwd_flag"] == "Y") | (df["store_and_fwd_flag"] == "N")]
        df = df[df["payment_type"].isin(["CRD", "CSH", "NOC", "DIS"])]
        df = df[df["fare_amount"] >= 0]
        df = df[df["mta_tax"] >= 0]
        df = df[df["tip_amount"] >= 0]
        df = df[df["tolls_amount"] >= 0]
        df = df[df["total_amount"] >= 0]
        df = df[df["surcharge"] >= 0]

        print("[INFO] Dropping non-Manhattan data.")
        # Drop data outside of Manhattan
        if len(df) > sample_size * 3:
            df = df.sample(sample_size * 3)
        df = df[parallel_proc(df, _batch_coordinate_in_manhattan_pickup, n_cores=16)]
        df = df[parallel_proc(df, _batch_coordinate_in_manhattan_dropoff, n_cores=16)]

        # Update cell values
        df["vendor"] = df["vendor"].apply(
            lambda x: "Creative Mobile Technologies, LLC"
            if x == "CMT"
            else "VeriFone Inc."
        )
        payment_types = {
            "CRD": "Credit card",
            "CSH": "Cash",
            "NOC": "No charge",
            "DIS": "Dispute",
        }
        df["payment_type"] = df["payment_type"].apply(lambda x: payment_types[x])
        df["rate_code"] = df["rate_code"].apply(lambda x: RATE_CODES[x])

        print("[INFO] Inserting census tract indices.")
        # Add census tract index to each entry
        df["pickup_census_tract_idx"] = parallel_proc(
            df, _batch_coordinate_to_census_tract_pickup, n_cores=16
        )
        df["dropoff_census_tract_idx"] = parallel_proc(
            df, _batch_coordinate_to_census_tract_dropoff, n_cores=16
        )

        print("[INFO] Inserting taxi zone ids.")
        # Add taxi zone to each entry
        df["pickup_zone"] = parallel_proc(
            df, _batch_coordinate_to_zone_pickup, n_cores=16
        )
        df["dropoff_zone"] = parallel_proc(
            df, _batch_coordinate_to_zone_dropoff, n_cores=16
        )

        # Sample data
        if len(df) > sample_size:
            df = df.sample(sample_size)

        print("[INFO] Writing cleaned data to: {}".format(clean_data_path))
        # Save to folder
        df.to_csv(clean_data_path, index=False)

        if create_sample:
            sample_data_filename = "sample_manhattan_taxi_2014_nov.csv"
            sample_data_path = os.path.join(SAMPLE_DATA_ROOT, sample_data_filename)

            if not os.path.exists(sample_data_path):
                print(
                    "[INFO] Writing sample cleaned data to: {}".format(sample_data_path)
                )
                df_sample = df.sample(1000)
                df_sample.to_csv(sample_data_path, index=False)
        else:
            print("[INFO] Found existing sample data: {}".format(sample_data_path))

    else:
        print("[INFO] Found existing clean file: {}".format(clean_data_path))


def process_taxi_2021_data(df, create_sample=False):
    clean_data_filename = "manhattan_taxi_2021_nov.csv"
    clean_data_path = os.path.join(CLEAN_DATA_ROOT, clean_data_filename)

    if not os.path.exists(clean_data_path):
        print("[INFO] Cleaning 2021 Taxi Trip data...")
        from utils import borough_zone_ids

        sample_size = 1000000

        print("[INFO] Updating column headers.")
        # Unify column headers
        df = df.rename(
            columns={
                "VendorID": "vendor",
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "RatecodeID": "rate_code",
                "PULocationID": "pickup_zone",
                "DOLocationID": "dropoff_zone",
            }
        )

        print("[INFO] Dropping invalid rows.")
        # Select only data from November 2021
        df = df[df["pickup_datetime"].dt.month == 11]
        df = df[df["dropoff_datetime"].dt.month == 11]

        # Drop rows with NaN
        df = df.dropna()

        # Drop rows with incorrect values
        df = df[(df["vendor"] == 1) | (df["vendor"] == 2)]
        df = df[df["passenger_count"] > 0]
        df = df[df["trip_distance"] > 0]
        df = df[(df["rate_code"] >= 1) & (df["rate_code"] <= 6)]
        df = df[(df["store_and_fwd_flag"] == "Y") | (df["store_and_fwd_flag"] == "N")]
        df = df[(df["payment_type"] >= 1) & (df["payment_type"] <= 6)]
        df = df[df["fare_amount"] >= 0]
        df = df[df["extra"] >= 0]
        df = df[df["mta_tax"] >= 0]
        df = df[df["tip_amount"] >= 0]
        df = df[df["tolls_amount"] >= 0]
        df = df[df["improvement_surcharge"] >= 0]
        df = df[df["total_amount"] >= 0]
        df = df[df["congestion_surcharge"] >= 0]

        print("[INFO] Dropping non-Manhattan data.")
        # Drop data outside of Manhattan
        df = df[df["pickup_zone"].isin(borough_zone_ids["Manhattan"])]
        df = df[df["dropoff_zone"].isin(borough_zone_ids["Manhattan"])]

        # Sample data
        df = df.sample(sample_size)

        # Update cell values
        df["vendor"] = df["vendor"].apply(
            lambda x: "Creative Mobile Technologies, LLC" if x == 1 else "VeriFone Inc."
        )
        payment_types = {
            1: "Credit card",
            2: "Cash",
            3: "No charge",
            4: "Dispute",
            5: "Negotiated fare",
            6: "Group ride",
        }
        df["payment_type"] = df["payment_type"].apply(lambda x: payment_types[x])
        df["rate_code"] = df["rate_code"].apply(lambda x: RATE_CODES[x])

        print("[INFO] Writing cleaned data to: {}".format(clean_data_path))
        # Save to folder
        df.to_csv(clean_data_path, index=False)

        if create_sample:
            sample_data_filename = "sample_manhattan_taxi_2021_nov.csv"
            sample_data_path = os.path.join(SAMPLE_DATA_ROOT, sample_data_filename)

            if not os.path.exists(sample_data_path):
                print(
                    "[INFO] Writing sample cleaned data to: {}".format(sample_data_path)
                )
                df_sample = df.sample(1000)
                df_sample.to_csv(sample_data_path, index=False)
        else:
            print("[INFO] Found existing sample data: {}".format(sample_data_path))
    else:
        print("[INFO] Found existing clean file: {}".format(clean_data_path))


def process_popular_times_data(j_data, create_sample=False):
    clean_data_filename = "manhattan_popular_times.json"
    clean_data_path = os.path.join(CLEAN_DATA_ROOT, clean_data_filename)

    if not os.path.exists(clean_data_path):
        print("[INFO] Cleaning Popular Times data...")
        from utils import coordinate_to_borough, coordinate_to_census_tract

        print("[INFO] Calculating the borough and census tract for each location.")

        for place in j_data:
            # Add borough and census tract info to each entry
            long = place["coordinates"]["lng"]
            lat = place["coordinates"]["lat"]
            boro_name = coordinate_to_borough([long, lat])
            census_tract_idx = coordinate_to_census_tract([long, lat])
            place["borough"] = boro_name
            place["census_tract_idx"] = census_tract_idx

        print("[INFO] Dropping non-Manhattan locations.")
        manhattan_places = [
            place for place in j_data if place["borough"] == "Manhattan"
        ]

        print("[INFO] Writing cleaned data to: {}".format(clean_data_path))
        with open(clean_data_path, "w") as out_file:
            json.dump(manhattan_places, out_file)

        if create_sample:
            sample_data_filename = "sample_manhattan_popular_times.json"
            sample_data_path = os.path.join(SAMPLE_DATA_ROOT, sample_data_filename)

            if not os.path.exists(sample_data_path):
                print(
                    "[INFO] Writing sample cleaned data to: {}".format(sample_data_path)
                )
                sample_manhattan_places = np.random.choice(
                    np.array(manhattan_places), 1000
                ).tolist()
                with open(sample_data_path, "w") as out_file:
                    json.dump(sample_manhattan_places, out_file)
            else:
                print("[INFO] Found existing sample data: {}".format(sample_data_path))
    else:
        print("[INFO] Found existing clean file: {}".format(clean_data_path))


def process_land_use_data(df, create_sample=False):
    clean_data_filename = "manhattan_pluto_map.geojson"
    clean_data_path = os.path.join(CLEAN_DATA_ROOT, clean_data_filename)

    if not os.path.exists(clean_data_path):
        print("[INFO] Cleaning Land Use data...")

        # Reproject geodataframe to a geographic CRS (e.g. ESPG: 4326)
        # Reference: https://gis.stackexchange.com/questions/48949/epsg-3857-or-4326-for-googlemaps-openstreetmap-and-leaflet
        print("[INFO] Reprojecting coordinates to ESPG 4236.")
        df = df.to_crs(4236)

        # Keep only important columns
        kept_cols = [
            "Borough",
            "BoroCode",
            "Address",
            "LandUse",
            "LotArea",
            "NumFloors",
            "Latitude",
            "Longitude",
            "geometry",
        ]
        df = df[kept_cols]

        # Rename columns
        df.columns = [
            "borough",
            "borough_code",
            "address",
            "land_use",
            "lot_area",
            "num_floors",
            "latitude",
            "longitude",
            "geometry",
        ]

        # Change borough names to full names
        borough_dict = {
            "BX": "Bronx",
            "BK": "Brooklyn",
            "MN": "Manhattan",
            "QN": "Queens",
            "SI": "Staten Island",
        }
        df["borough"] = df["borough"].map(lambda x: borough_dict[x])

        # Keep only Manhattan data
        df = df[df["borough"] == "Manhattan"]
        df = df.reset_index().rename(columns={"index": "id"})

        print("[INFO] Writing cleaned data to: {}".format(clean_data_path))
        df.to_file(clean_data_path, driver="GeoJSON")

        if create_sample:
            sample_data_filename = "sample_map_pluto.geojson"
            sample_data_path = os.path.join(SAMPLE_DATA_ROOT, sample_data_filename)

            if not os.path.exists(sample_data_path):
                print(
                    "[INFO] Writing sample cleaned data to: {}".format(sample_data_path)
                )
                df_sample = df[df["borough"] == "Manhattan"].sample(1000)
                df_sample.to_file(sample_data_path, driver="GeoJSON")
            else:
                print("[INFO] Found existing sample data: {}".format(sample_data_path))
    else:
        print("[INFO] Found existing clean file: {}".format(clean_data_path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Cleaning Script")

    parser.add_argument(
        "--data",
        default="all",
        help="Name of the dataset.",
        choices=["all", "taxi", "popular_times", "land_use"],
    )

    parser.add_argument("--create_sample", default=False, action="store_true")

    args = parser.parse_args()

    main(args)
