import json
import os

import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, dcc, html
from dash.exceptions import PreventUpdate

from utils.bivariate_choropleth import (
    color_sets,
    conf_defaults,
    create_bivariate_map,
    join_taxi_with_pt_df,
)
from utils.filtering import (
    PaymentType,
    TaxiCoordType,
    Weekday,
    filter_popular_times,
    filter_taxi_df,
)
from utils.utils import geo_dict, vectorize_popularity

# Set constants and access token
DATA_ROOT = "./data/sample"

# Note: Bad Practice! Should never commit the token.
TOKEN_PATH = "./data/.mapbox_token"
mapbox_access_token = open(TOKEN_PATH).read()
px.set_mapbox_access_token(mapbox_access_token)


# Read main data
taxi_data_path = os.path.join(DATA_ROOT, "sample_manhattan_taxi_2021_nov_final.csv")
popular_times_data_path = os.path.join(DATA_ROOT, "sample_manhattan_popular_times.json")

taxi = pd.read_csv(taxi_data_path, engine="pyarrow")
popular_times = pd.read_json(popular_times_data_path)

# Setting column name as `pt_vec_orig` in case of overwriting the columns during filtering
popular_times["pt_vec_orig"] = popular_times["populartimes"].apply(vectorize_popularity)

taxi["pickup_datetime"] = pd.to_datetime(taxi["pickup_datetime"])
taxi["dropoff_datetime"] = pd.to_datetime(taxi["dropoff_datetime"])

taxi["pickup_weekday"] = taxi["pickup_datetime"].dt.weekday
taxi["dropoff_weekday"] = taxi["dropoff_datetime"].dt.weekday
taxi["pickup_hour"] = taxi["pickup_datetime"].dt.hour
taxi["dropoff_hour"] = taxi["dropoff_datetime"].dt.hour

print("[DEBUG] app.py: Finished loading main data.")

# Load conf defaults
cholopleth_config = conf_defaults()

# Override some variables
cholopleth_config["plot_title"] = "Manhattan Taxi Trip"
cholopleth_config["width"] = 1000  # Width of the final map container
cholopleth_config["ratio"] = 0.8  # Ratio of height to width
cholopleth_config["height"] = (
    cholopleth_config["width"] * cholopleth_config["ratio"]
)  # Width of the final map container
cholopleth_config["center_lat"] = 40.7128  # Latitude of the center of the map
cholopleth_config["center_lon"] = -74.0060  # Longitude of the center of the map
cholopleth_config["map_zoom"] = 5  # Zoom factor of the map
cholopleth_config["hover_x_label"] = "Taxi Pickups"  # Label to appear on hover
cholopleth_config["hover_y_label"] = "Popularity"  # Label to appear on hover

# Define settings for the legend
cholopleth_config["line_width"] = 0.5  # Width of the rectagles' borders
cholopleth_config[
    "legend_x_label"
] = "More Taxi Pickups"  # x variable label for the legend
cholopleth_config[
    "legend_y_label"
] = "Higher Popularity"  # y variable label for the legend

print("[DEBUG] app.py: Finished loading choropleth config.")


def blank_fig():
    fig = go.Figure(go.Scatter(x=[], y=[]))
    fig.update_layout(
        template=None,
        plot_bgcolor="rgba( 0, 0, 0, 0)",
        paper_bgcolor="rgba( 0, 0, 0, 0)",
    )
    fig.update_xaxes(showgrid=False, showticklabels=False, zeroline=False)
    fig.update_yaxes(showgrid=False, showticklabels=False, zeroline=False)

    return fig


blank_config = {
    "displaylogo": False,
    "modeBarButtonsToAdd": [
        "drawline",
        "drawopenpath",
        "drawclosedpath",
        "drawcircle",
        "drawrect",
        "eraseshape",
    ],
}

app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ],
)
server = app.server
app.title = "Manhattan Taxi Data Visualization"

app.layout = html.Div(
    [
        html.Div(
            [html.P("Manhattan Taxi Data Visualization", id="title-text")], id="title"
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.P("Taxi Coordinate Type"),
                                        dcc.Dropdown(
                                            options=[
                                                t.name.capitalize()
                                                for t in TaxiCoordType
                                            ],
                                            multi=False,
                                            value=TaxiCoordType.PICKUP.name,
                                            id="taxi-coord-type",
                                        ),
                                    ],
                                    id="taxi-coord-type-container",
                                ),
                                html.Hr(),
                                html.Div(
                                    [
                                        html.P("Trip Distance"),
                                        dcc.RangeSlider(
                                            min=taxi["trip_distance"].min(),
                                            max=taxi["trip_distance"].max(),
                                            value=[
                                                taxi["trip_distance"].min(),
                                                taxi["trip_distance"].max(),
                                            ],
                                            tooltip={
                                                "placement": "bottom",
                                                "always_visible": True,
                                            },
                                            id="trip-distance",
                                        ),
                                    ],
                                    id="trip-distance-container",
                                ),
                                html.Hr(),
                                html.Div(
                                    [
                                        html.P("Fare Amount"),
                                        dcc.RangeSlider(
                                            min=taxi["fare_amount"].min(),
                                            max=taxi["fare_amount"].max(),
                                            value=[
                                                taxi["fare_amount"].min(),
                                                taxi["fare_amount"].max(),
                                            ],
                                            tooltip={
                                                "placement": "bottom",
                                                "always_visible": True,
                                            },
                                            id="fare-amount",
                                        ),
                                    ],
                                    id="fare-amount-container",
                                ),
                                html.Hr(),
                                html.Div(
                                    [
                                        html.P("Tip Amount"),
                                        dcc.RangeSlider(
                                            min=taxi["tip_amount"].min(),
                                            max=taxi["tip_amount"].max(),
                                            value=[
                                                taxi["tip_amount"].min(),
                                                taxi["tip_amount"].max(),
                                            ],
                                            tooltip={
                                                "placement": "bottom",
                                                "always_visible": True,
                                            },
                                            id="tip-amount",
                                        ),
                                    ],
                                    id="tip-amount-container",
                                ),
                                html.Hr(),
                                html.Div(
                                    [
                                        html.P("Total Amount"),
                                        dcc.RangeSlider(
                                            min=taxi["total_amount"].min(),
                                            max=taxi["total_amount"].max(),
                                            value=[
                                                taxi["total_amount"].min(),
                                                taxi["total_amount"].max(),
                                            ],
                                            tooltip={
                                                "placement": "bottom",
                                                "always_visible": True,
                                            },
                                            id="total-amount",
                                        ),
                                    ],
                                    id="total-amount-container",
                                ),
                                html.Hr(),
                                html.Div(
                                    [
                                        html.P("Payment Type"),
                                        dcc.Dropdown(
                                            options=sorted(
                                                list(taxi["payment_type"].unique())
                                            ),
                                            multi=True,
                                            value=sorted(
                                                list(taxi["payment_type"].unique())
                                            ),
                                            id="payment-type",
                                        ),
                                    ],
                                    id="payment-type-container",
                                ),
                                html.Hr(),
                                html.Div(
                                    [
                                        html.P("Day of the Week"),
                                        dcc.Dropdown(
                                            options=[
                                                t.name.capitalize() for t in Weekday
                                            ],
                                            multi=True,
                                            value=[
                                                t.name.capitalize() for t in Weekday
                                            ],
                                            id="weekday",
                                        ),
                                    ],
                                    id="weekday-container",
                                ),
                                html.Hr(),
                                html.Div(
                                    [
                                        html.P("Hour of the Day"),
                                        dcc.RangeSlider(
                                            0,
                                            24,
                                            1,
                                            value=[0, 24],
                                            id="hour",
                                            tooltip={
                                                "placement": "bottom",
                                                "always_visible": True,
                                            },
                                        ),
                                    ],
                                    id="hour-container",
                                ),
                            ],
                            id="map1-filters",
                        ),
                        html.Div(
                            [
                                dcc.Graph(
                                    id="figure1",
                                    figure=blank_fig(),
                                    config=blank_config,
                                )
                            ],
                            id="map1-fig",
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Img(
                                            src="./assets/taxi_icon_vector.png",
                                            style={"height": "100%"},
                                        )
                                    ],
                                    style={
                                        "marginLeft": 5,
                                        "marginRight": 5,
                                        "marginTop": 5,
                                        "marginBottom": 5,
                                        "padding": "8px 8px 8px 8px",
                                        "textAlign": "center",
                                        "height": "10%",
                                    },
                                ),
                                html.Div(
                                    [
                                        dcc.Markdown(
                                            """
                                        ### Data and Parameters
                                        """
                                        ),
                                        dcc.Markdown(
                                            """
                                        Our project primarily utilizes two datasets: the NYC Taxi Trip Data and the Google Popular Times Data. There are 8 different parameters
                                        in the panel for filtering and exploration the spatial-temporal relationships between the taxi trips and contextual popularities of the region.
                                        """
                                        ),
                                        dcc.Markdown(
                                            """
                                        ### Coordinate Estimation
                                        """
                                        ),
                                        dcc.Markdown(
                                            """
                                        We estimate the pickup and dropoff coordinates of the NYC taxi trips in 2021 using Random Forest, selected from five different 
                                        machine learning models that were evaluated with mean-square error (MSE) and coefficient of determination, and test zone accuracy.
                                        """
                                        ),
                                        dcc.Markdown(
                                            """
                                        ### Bivariate Choropleth
                                        """
                                        ),
                                        dcc.Markdown(
                                            """
                                        A bivariate choropleth map is similar to a basic (univariate) choropleth map, with the exception that it displays two variables 
                                        simultaneously, effectively revealing spatial relationships and patterns between two variables on a single map.
                                        """
                                        ),
                                    ],
                                    style={
                                        "marginLeft": 5,
                                        "marginRight": 5,
                                        "marginTop": 5,
                                        "marginBottom": 5,
                                        "padding": "8px 8px 8px 8px",
                                        "height": "100%",
                                    },
                                ),
                            ],
                            id="map1-dsc",
                        ),
                    ],
                    style={
                        "padding": "15px 15px 15px 15px",
                    },
                    id="map1",
                    className="map-container",
                ),
            ],
            id="main",
        ),
    ],
    id="layout",
)


@app.callback(
    Output("figure1", "figure"),
    [
        Input("taxi-coord-type", "value"),
        Input("trip-distance", "value"),
        Input("fare-amount", "value"),
        Input("tip-amount", "value"),
        Input("total-amount", "value"),
        Input("payment-type", "value"),
        Input("weekday", "value"),
        Input("hour", "value"),
    ],
)
def update_map1(
    taxi_coord_type,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    payment_type,
    weekday,
    hour,
):
    if (
        trip_distance
        and fare_amount
        and tip_amount
        and total_amount
        and payment_type
        and weekday
        and hour
    ):
        if type(payment_type) != list:
            payment_type = [payment_type]
        if type(weekday) != list:
            weekday = [weekday]
        if type(hour) != list:
            hour = [hour]

        taxi_coord_type = TaxiCoordType[taxi_coord_type.upper()]
        payment_type = [PaymentType[s.replace(" ", "").upper()] for s in payment_type]
        weekday = [Weekday[s.upper()] for s in weekday]
        hour = list(range(hour[0], hour[1]))

        taxi_filtered = filter_taxi_df(
            taxi,
            taxi_coord_type,
            trip_distance,
            fare_amount,
            tip_amount,
            total_amount,
            payment_type,
            weekday,
            hour,
        )

        print("[DEBUG] Finished filtering taxi data.")

        popular_times_filtered = filter_popular_times(popular_times, weekday, hour)

        print("[DEBUG] Finished filtering popular times data.")

        joined_df = join_taxi_with_pt_df(taxi_filtered, popular_times_filtered)
        fig = create_bivariate_map(
            joined_df, color_sets["pink-blue"], geo_dict, conf=cholopleth_config
        )

        return fig
    else:
        raise PreventUpdate


if __name__ == "__main__":
    app.run_server(debug=True)
