import os
import shutil

import gdown

CLEAN_DATA_ROOT = "./clean"
SAMPLE_DATA_ROOT = "./sample"

if not os.path.exists(CLEAN_DATA_ROOT):
    os.makedirs(CLEAN_DATA_ROOT)

# Download cleaned 2014 taxi data
print("[INFO] Downloading cleaned 2014 taxi data.")

taxi_2014_file_name = "manhattan_taxi_2014_nov.csv"
taxi_2014_dest_path = os.path.join(CLEAN_DATA_ROOT, taxi_2014_file_name)

if not os.path.exists(taxi_2014_dest_path):
    file_id = "1D-jXzeuFt-FDkWqOvO1oSVN1a9skKUqw"
    gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
    gdown.download(gdrive_url, taxi_2014_dest_path, quiet=False)

# Download cleaned 2021 taxi data
print("[INFO] Downloading cleaned 2021 taxi data.")

taxi_2021_file_name = "manhattan_taxi_2021_nov.csv"
taxi_2021_dest_path = os.path.join(CLEAN_DATA_ROOT, taxi_2021_file_name)

if not os.path.exists(taxi_2021_dest_path):
    file_id = "1I6Zf0QM-Mcrj644HBXzONJVxCziu7QQx"
    gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
    gdown.download(gdrive_url, taxi_2021_dest_path, quiet=False)

# Download cleaned land use data
print("[INFO] Downloading cleaned land use data.")

land_use_file_name = "manhattan_pluto.geojson"
land_use_dest_path = os.path.join(CLEAN_DATA_ROOT, land_use_file_name)

if not os.path.exists(land_use_dest_path):
    file_id = "1N57qAagnOqw0iVLJybeDEnjTHFee19Nu"
    gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
    gdown.download(gdrive_url, land_use_dest_path, quiet=False)

# Download all sample data as zip file and unzip
if not os.path.exists(SAMPLE_DATA_ROOT):
    file_id = "19WTOqmY54QqrqgES5G5sMhLqgMDId7KU"
    gdrive_url = "https://drive.google.com/uc?id={}".format(file_id)
    zip_name = SAMPLE_DATA_ROOT + ".zip"
    gdown.download(gdrive_url, zip_name, quiet=False)
    shutil.unpack_archive(zip_name, SAMPLE_DATA_ROOT)
    os.remove(zip_name)
