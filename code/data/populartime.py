# install populartimes library:
# pip install --upgrade git+https://github.com/m-wrzr/populartimes

# data collection procedure
# 1. calculate the coordinates of the tile
# 2. test if it is in NYC on google maps
# 3. scrape data by popular times API
# 4. name the json according to the excel
# 5. Share the data in the shared folder in google drive


import json

import populartimes

# over all NYC
# A - sourth-west point
A_lon = 40.493809
A_lat = -74.266732
# B - north-east point
B_lon = 40.925816
B_lat = -73.687204

# usage calculate coordinate of the tile at n row, n column
def sw_coord(A_lon, A_lat, B_lon, B_lat, row, col):
    # sourth-west point
    lonsw = (B_lon - A_lon) / 10 * (col - 1) + A_lon
    latsw = (B_lat - A_lat) / 10 * (10 - row) + A_lat
    sw = (lonsw, latsw)
    # north-east point
    lonne = (B_lon - A_lon) / 10 * col + A_lon
    latne = (B_lat - A_lat) / 10 * (11 - row) + A_lat
    ne = (lonne, latne)
    return sw, ne


# TODO: Set your API key
APIkey = "your apikey"

# TODO: Set row_number and col_number according to the spreadsheet
# example: coordinate for tile 10, row = 1, col = 10
row_number = 1
col_number = 10
tile_num = (row_number - 1) * 10 + col_number

print(
    "Start scraping data for tile{} at row {} column {}".format(
        tile_num, row_number, col_number
    )
)

coord = sw_coord(A_lon, A_lat, B_lon, B_lat, row_number, col_number)
print(coord)

data = populartimes.get(APIkey, ["*"], coord[0], coord[1])  # wildcard for type
# populartimes.get(APIkey,['store'],(40.750449, -73.987284), (40.747182, -73.985503))

# 40.751468, -73.976158
# 40.745257, -73.998408
# empire state building place id: ChIJaXQRs6lZwokRY6EFpJnhNNE 20 W 34th St., New York, NY 10001, USA

with open(
    "tile{}.json".format(tile_num), "w"
) as outfile:  # output name = tile'n', according to excel doc
    json.dump(data, outfile)
