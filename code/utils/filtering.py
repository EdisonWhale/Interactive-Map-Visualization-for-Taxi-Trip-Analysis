from enum import Enum
from typing import List

import numpy as np
import pandas as pd


class TaxiCoordType(str, Enum):
    PICKUP = 1
    DROPOFF = 2


class PaymentType(Enum):
    CREDITCARD = 1
    CASH = 2
    NOCHARGE = 3
    DISPUTE = 4
    UNKNOWN = 5
    VOIDEDTRIP = 6


class Weekday(Enum):
    MONDAY = 1
    TUESDAY = 2
    WEDNESDAY = 3
    THURSDAY = 4
    FRIDAY = 5
    SATURDAY = 6
    SUNDAY = 7


def filter_taxi_df(
    df: pd.DataFrame,
    taxi_coord_type: TaxiCoordType = TaxiCoordType.PICKUP,
    trip_distance: List[float] = None,
    fare_amount: List[float] = None,
    tip_amount: List[float] = None,
    total_amount: List[float] = None,
    payment_type: List[PaymentType] = None,
    weekday: List[Weekday] = None,
    hour: List[int] = None,
) -> pd.DataFrame:
    assert (
        len(trip_distance) == 2
    ), "[ERROR] `trip_distance` must be of length 2, but get: {}".format(trip_distance)
    assert (
        len(fare_amount) == 2
    ), "[ERROR] `fare_amount` must be of length 2, but get: {}".format(fare_amount)
    assert (
        len(tip_amount) == 2
    ), "[ERROR] `tip_amount` must be of length 2, but get: {}".format(tip_amount)
    assert (
        len(total_amount) == 2
    ), "[ERROR] `total_amount` must be of length 2, but get: {}".format(total_amount)

    payment_type = [t.name.capitalize() for t in payment_type]
    weekday = [d.value - 1 for d in weekday]
    # Make a copy in case of overwriting the original DataFrame
    df = df.copy()

    # Update columns names for the coordinates based on taxi_coord_type
    if taxi_coord_type == TaxiCoordType.PICKUP:
        long_header = "pickup_longitude"
        lat_header = "pickup_latitude"
        census_idx_header = "pickup_census_tract_idx"
        weekday_header = "pickup_weekday"
        hour_header = "pickup_hour"
    else:
        long_header = "dropoff_longitude"
        lat_header = "dropoff_latitude"
        census_idx_header = "dropoff_census_tract_idx"
        weekday_header = "dropoff_weekday"
        hour_header = "dropoff_hour"
    df = df.rename(
        columns={
            long_header: "longitude",
            lat_header: "latitude",
            census_idx_header: "census_tract_idx",
        }
    )

    # Filter based on numerical attributes
    df = df[df["trip_distance"] >= trip_distance[0]]
    df = df[df["trip_distance"] <= trip_distance[1]]
    df = df[df["fare_amount"] >= fare_amount[0]]
    df = df[df["fare_amount"] <= fare_amount[1]]
    df = df[df["tip_amount"] >= tip_amount[0]]
    df = df[df["tip_amount"] <= tip_amount[1]]
    df = df[df["total_amount"] >= total_amount[0]]
    df = df[df["total_amount"] <= total_amount[1]]

    # Filter based on categorical attributes
    df = df[df["payment_type"].str.replace(" ", "").isin(payment_type)]
    df = df[df[weekday_header].isin(weekday)]
    df = df[df[hour_header].isin(hour)]

    return df


def filter_popular_times(
    df: pd.DataFrame,
    weekday: List[Weekday] = None,
    hour: List[int] = None,
) -> pd.DataFrame:
    weekday = [d.value - 1 for d in weekday]
    # Make a copy in case of overwriting the original DataFrame
    df = df.copy()

    df["pt_vec"] = df["pt_vec_orig"].apply(lambda x: x[np.ix_(weekday, hour)])

    return df
