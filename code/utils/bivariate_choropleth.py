import json
import math
import os
import sys
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import shapely

from utils.utils import manhanttan_tract_polys

# Note: Bad Practice! Should never commit the token.
TOKEN_PATH = "./data/.mapbox_token"
mapbox_access_token = open(TOKEN_PATH).read()

# Define sets of 9 colors to be used
# Order: bottom-left, bottom-center, bottom-right, center-left, center-center, center-right, top-left, top-center, top-right
color_sets = {
    "pink-blue": [
        "#e8e8e8",
        "#ace4e4",
        "#5ac8c8",
        "#dfb0d6",
        "#a5add3",
        "#5698b9",
        "#be64ac",
        "#8c62aa",
        "#3b4994",
    ],
    "teal-red": [
        "#e8e8e8",
        "#e4acac",
        "#c85a5a",
        "#b0d5df",
        "#ad9ea5",
        "#985356",
        "#64acbe",
        "#627f8c",
        "#574249",
    ],
    "blue-organe": [
        "#fef1e4",
        "#fab186",
        "#f3742d",
        "#97d0e7",
        "#b0988c",
        "#ab5f37",
        "#18aee5",
        "#407b8f",
        "#5c473d",
    ],
}


def conf_defaults():
    """
    Function to set default variables
    """
    # Define some variables for later use
    conf = {
        "plot_title": "Bivariate choropleth map using Ploty",  # Title text
        "plot_title_size": 20,  # Font size of the title
        "center_lat": 0,  # Latitude of the center of the map
        "center_lon": 0,  # Longitude of the center of the map
        "map_zoom": 3,  # Zoom factor of the map
        "hover_x_label": "Label x variable",  # Label to appear on hover
        "hover_y_label": "Label y variable",  # Label to appear on hover
        "borders_width": 0.5,  # Width of the geographic entity borders
        "borders_color": "#f8f8f8",  # Color of the geographic entity borders
        # Define settings for the legend
        "top": 1,  # Vertical position of the top right corner (0: bottom, 1: top)
        "right": 1,  # Horizontal position of the top right corner (0: left, 1: right)
        "box_w": 0.04,  # Width of each rectangle
        "box_h": 0.04,  # Height of each rectangle
        "line_color": "#f8f8f8",  # Color of the rectagles' borders
        "line_width": 0,  # Width of the rectagles' borders
        "legend_x_label": "Higher x value",  # x variable label for the legend
        "legend_y_label": "Higher y value",  # y variable label for the legend
        "legend_font_size": 12,  # Legend font size
        "legend_font_color": "#333",  # Legend font color
    }
    return conf


def recalc_vars(new_width, variables, conf=conf_defaults()):
    """
    Function to recalculate values in case width is changed
    """
    # Calculate the factor of the changed width
    factor = new_width / 1000

    # Apply factor to all variables that have been passed to th function
    for var in variables:
        if var == "map_zoom":
            # Calculate the zoom factor
            # Mapbox zoom is based on a log scale. map_zoom needs to be set to value ideal for our map at 1000px.
            # So factor = 2 ^ (zoom - map_zoom) and zoom = log(factor) / log(2) + map_zoom
            conf[var] = math.log(factor) / math.log(2) + conf[var]
        else:
            conf[var] = conf[var] * factor

    return conf


def load_geojson(geojson_url, data_dir="./data/supplementary/", local_file=False):
    """
    Function to load GeoJSON file with geographical data of the entities
    """
    # Make sure data_dir is a string
    data_dir = str(data_dir)

    # Set name for the file to be saved
    if not local_file:
        # Use original file name if none is specified
        url_parsed = urlparse(geojson_url)
        local_file = os.path.basename(url_parsed.path)

    geojson_file = data_dir + "/" + str(local_file)

    # Create folder for data if it does not exist
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Download GeoJSON in case it doesn't exist
    if not os.path.exists(geojson_file):

        # Make http request for remote file data
        geojson_request = requests.get(geojson_url)

        # Save file to local copy
        with open(geojson_file, "wb") as file:
            file.write(geojson_request.content)

    # Load GeoJSON file
    geojson = json.load(open(geojson_file, "r"))

    # Return GeoJSON object
    return geojson


def set_interval_value(x, break_1, break_2):
    """
    Function that assigns a value (x) to one of three bins (0, 1, 2).
    The break points for the bins can be defined by break_a and break_b.
    """
    if x <= break_1:
        return 0
    elif break_1 < x <= break_2:
        return 1
    else:
        return 2


def join_taxi_with_pt_df(taxi_df, popular_times_df):
    # Calculate popularity for each census tract
    by_tract_pt = popular_times_df.groupby("census_tract_idx")["pt_vec"].mean()
    by_tract_pt_mean = by_tract_pt.apply(np.mean)
    by_tract_pt_mean = (
        pd.DataFrame(by_tract_pt_mean)
        .reset_index(drop=True)
        .rename(columns={"pt_vec": "popularity"})
    )

    # Calculate the trip count for each census tract
    by_tract_pickup_cnt = taxi_df["census_tract_idx"].value_counts()
    by_tract_pickup_cnt = pd.DataFrame(by_tract_pickup_cnt).rename(
        columns={"census_tract_idx": "taxi"}
    )

    # Join the dataframes
    joined_df = (
        pd.concat([by_tract_pt_mean, by_tract_pickup_cnt], axis=1, join="outer")
        .reindex(manhanttan_tract_polys.keys())
        .fillna(0)
        .reset_index(names="id")
    )
    joined_df

    return joined_df


def prepare_df(df, x="taxi", y="popularity"):
    """
    Function that adds a column 'biv_bins' to the dataframe containing the
    position in the 9-color matrix for the bivariate colors

    Arguments:
        df: Dataframe
        x: Name of the column containing values of the first variable
        y: Name of the column containing values of the second variable

    """
    # Check if arguments match all requirements
    if df[x].shape[0] != df[y].shape[0]:
        raise ValueError(
            "ERROR: The list of x and y coordinates must have the same length."
        )

    # Calculate break points at percentiles 33 and 66
    x_breaks = np.percentile(df[x], [33, 66])
    y_breaks = np.percentile(df[y], [33, 66])

    # Assign values of both variables to one of three bins (0, 1, 2)
    x_bins = [
        set_interval_value(value_x, x_breaks[0], x_breaks[1]) for value_x in df[x]
    ]
    y_bins = [
        set_interval_value(value_y, y_breaks[0], y_breaks[1]) for value_y in df[y]
    ]

    # Calculate the position of each x/y value pair in the 9-color matrix of bivariate colors
    df["biv_bins"] = [
        str(value_x + 3 * value_y) for value_x, value_y in zip(x_bins, y_bins)
    ]

    return df


def create_legend(fig, colors, conf=conf_defaults()):
    """
    Function to create a color square containig the 9 colors to be used as a legend
    """

    # Reverse the order of colors
    legend_colors = colors[:]
    legend_colors.reverse()

    # Calculate coordinates for all nine rectangles
    coord = []

    # Adapt height to ratio to get squares
    width = conf["box_w"]
    height = conf["box_h"] / conf["ratio"]

    # Start looping through rows and columns to calculate corners the squares
    for row in range(1, 4):
        for col in range(1, 4):
            coord.append(
                {
                    "x0": round(conf["right"] - (col - 1) * width, 4),
                    "y0": round(conf["top"] - (row - 1) * height, 4),
                    "x1": round(conf["right"] - col * width, 4),
                    "y1": round(conf["top"] - row * height, 4),
                }
            )

    # Create shapes (rectangles)
    for i, value in enumerate(coord):
        # Add rectangle
        fig.add_shape(
            go.layout.Shape(
                type="rect",
                fillcolor=legend_colors[i],
                line=dict(
                    color=conf["line_color"],
                    width=conf["line_width"],
                ),
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="top",
                x0=coord[i]["x0"],
                y0=coord[i]["y0"],
                x1=coord[i]["x1"],
                y1=coord[i]["y1"],
            )
        )

        # Add text for first variable
        fig.add_annotation(
            xref="paper",
            yref="paper",
            xanchor="left",
            yanchor="top",
            x=coord[8]["x1"],
            y=coord[8]["y1"],
            showarrow=False,
            text=conf["legend_x_label"],
            font=dict(
                color=conf["legend_font_color"],
                size=conf["legend_font_size"],
            ),
            borderpad=0,
        )

        # Add text for second variable
        fig.add_annotation(
            xref="paper",
            yref="paper",
            xanchor="right",
            yanchor="bottom",
            x=coord[8]["x1"],
            y=coord[8]["y1"],
            showarrow=False,
            text=conf["legend_y_label"],
            font=dict(
                color=conf["legend_font_color"],
                size=conf["legend_font_size"],
            ),
            textangle=270,
            borderpad=0,
        )

    return fig


def create_bivariate_map(
    df,
    colors,
    geojson,
    x="taxi",
    y="popularity",
    ids="id",
    name="name",
    conf=conf_defaults(),
):

    if len(colors) != 9:
        raise ValueError(
            "ERROR: The list of bivariate colors must have a length eaqual to 9."
        )

    # Prepare the dataframe with the necessary information for our bivariate map
    df_plot = prepare_df(df, x, y)

    # Create the figure
    fig = px.choropleth_mapbox(
        df_plot,
        geojson=geojson,
        locations=ids,
        color=df_plot["biv_bins"],
        color_discrete_map={
            "0": colors[0],
            "1": colors[1],
            "2": colors[2],
            "3": colors[3],
            "4": colors[4],
            "5": colors[5],
            "6": colors[6],
            "7": colors[7],
            "8": colors[8],
        },
        custom_data=df_plot[[ids, x, y]],  # Add data to be used in hovertemplate
    )
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        mapbox_style="light",
        mapbox_accesstoken=mapbox_access_token,
        mapbox_zoom=11,
        mapbox_center={"lat": 40.7858, "lon": -73.9800},
        showlegend=False,
        autosize=False,
    )

    fig.update_traces(
        hovertemplate="<br>".join(
            [  # Data to be displayed on hover
                "<b>ID: %{customdata[0]}</b>",
                conf["hover_x_label"] + ": %{customdata[1]:.3f}",
                conf["hover_y_label"] + ": %{customdata[2]:.3f}",
                "<extra></extra>",  # Remove secondary information
            ]
        ),
        marker_line_width=conf[
            "borders_width"
        ],  # Width of the geographic entity borders
        marker_line_color=conf[
            "borders_color"
        ],  # Color of the geographic entity borders
        showscale=False,  # Hide the colorscale
    )

    # Add the legend
    fig = create_legend(fig, colors, conf)

    # Show the correct geo location
    fig.update_geos(fitbounds="locations", visible=False)

    print("[DEBUG] Updated choropleth.")

    return fig
