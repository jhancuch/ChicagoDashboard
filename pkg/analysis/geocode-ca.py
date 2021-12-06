"""
Takes trip observations that have lat/long but no community area and finds which community area the pickup/dropoff
occured.
"""

import psycopg2
import sqlalchemy
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import date



# Connect to postgres database
connection = psycopg2.connect(user="",
                             password="",
                             host="",
                             port="",
                             database="")

connection_gis = psycopg2.connect(user="",
                             password="",
                             host="",
                             port="",
                             database="")

cursor = connection.cursor()



# Import data

trip_geocode_query = """\
    SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
    pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude, sum(count) as count \
    FROM ( \
            SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude, sum(count) as count \
            FROM taxi2021 \
            WHERE pickup_community_area = '' OR dropoff_community_area = '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude \
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude, count(*) as count \
            FROM tnc21 \
            WHERE pickup_community_area = '' OR dropoff_community_area = '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude \
            \
            UNION ALL\
            \
             SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude, sum(count) as count \
            FROM taxi2020 \
            WHERE pickup_community_area = '' OR dropoff_community_area = '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude \
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude, count(*) as count \
            FROM tnc20 \
            WHERE pickup_community_area = '' OR dropoff_community_area = '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
            pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude \
    ) as a\
    GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area, pickup_centroid_latitude, \
    pickup_centroid_longitude, dropoff_centroid_latitude, dropoff_centroid_longitude"""

trip_ready_query = """\
    SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, sum(count) as count \
    FROM ( \
    \
            SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, sum(count) as count \
            FROM taxi2021 \
            WHERE pickup_community_area != '' AND dropoff_community_area != '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area \
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, count(*) as count \
            FROM tnc21 \
            WHERE pickup_community_area != '' AND dropoff_community_area != '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area \
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, sum(count) as count \
            FROM taxi2020 \
            WHERE pickup_community_area != '' AND dropoff_community_area != '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area \
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, count(*) as count \
            FROM tnc20 \
            WHERE pickup_community_area != '' AND dropoff_community_area != '' \
            GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area \
    ) as a\
    GROUP BY trip_start_timestamp, pickup_community_area, dropoff_community_area;
    """

boundaries_query = """\
    SELECT * FROM boundaries;
"""

df_geocode_trip = pd.read_sql(trip_geocode_query, connection)
df_ready_trip = pd.read_sql(trip_ready_query, connection)
df_boundaries = gpd.read_postgis(boundaries_query, connection_gis)



# Clean geocode trip dataframe - drop any observations that have a blank vaue for coordinates and use geocoding procedures to determine CA
# Drop observations with blank values
df_geocode_trip_1 = df_geocode_trip[(df_geocode_trip.pickup_centroid_latitude != "") & 
    (df_geocode_trip.pickup_centroid_longitude != "") & 
    (df_geocode_trip.dropoff_centroid_latitude != "") & 
    (df_geocode_trip.dropoff_centroid_longitude != "")]

# Data type conversion
loop_var = ["pickup_centroid_latitude", "pickup_centroid_longitude" ,"dropoff_centroid_latitude", "dropoff_centroid_longitude"]
for i in loop_var:
    df_geocode_trip_1[i] = pd.to_numeric(df_geocode_trip_1[i])
    
# Find CA for pickup locations
df_geocode_trip_pick_up_1 = gpd.GeoDataFrame(df_geocode_trip_1, 
                                     geometry=gpd.points_from_xy(
                                        df_geocode_trip_1.pickup_centroid_longitude, df_geocode_trip_1.pickup_centroid_latitude)).reset_index()
df_geocode_trip_pick_up_1.loc[:, "pickup_geocoded_ca"] = np.nan

for fi in df_geocode_trip_pick_up_1.index:
    for si in df_boundaries.index:
        if df_boundaries.loc[si, "geom"].contains(df_geocode_trip_pick_up_1.loc[fi, "geometry"]):
            df_geocode_trip_pick_up_1.loc[fi, "pickup_geocoded_ca"] = df_boundaries.loc[si, "area_num_1"]

# Find CA for dropoff locations
df_geocode_trip_drop_off_1 = gpd.GeoDataFrame(df_geocode_trip_1, 
                                     geometry=gpd.points_from_xy(
                                        df_geocode_trip_1.dropoff_centroid_longitude, df_geocode_trip_1.dropoff_centroid_latitude)).reset_index()
df_geocode_trip_drop_off_1.loc[:, "dropoff_geocoded_ca"] = np.nan

for fi in df_geocode_trip_drop_off_1.index:
    for si in df_boundaries.index:
        if df_boundaries.loc[si, "geom"].contains(df_geocode_trip_drop_off_1.loc[fi, "geometry"]):
            df_geocode_trip_drop_off_1.loc[fi, "dropoff_geocoded_ca"] = df_boundaries.loc[si, "area_num_1"]



# Join the pickup and dropoff tables that contain the geocoded CA and then bring those geocoded CA's to the initial trip dataframe that was geocoded
df_geocode_trip_pick_up_2 = df_geocode_trip_pick_up_1[["index", "pickup_geocoded_ca"]]
df_geocode_trip_drop_off_2 = df_geocode_trip_drop_off_1[["index", "dropoff_geocoded_ca"]]
temp = df_geocode_trip_pick_up_2.merge(df_geocode_trip_drop_off_2)

df_geocode_trip_2 = pd.merge(df_geocode_trip_1, temp, left_index=True, right_on='index')

# If the CA is missing, we input the geocoded CA
for i, r in df_geocode_trip_2.iterrows():
    if r["pickup_community_area"] == "":
        df_geocode_trip_2.loc[i, "pickup_community_area"] = df_geocode_trip_2.loc[i, "pickup_geocoded_ca"]

    if r["dropoff_community_area"] == "":
        df_geocode_trip_2.loc[i, "dropoff_community_area"] = df_geocode_trip_2.loc[i, "dropoff_geocoded_ca"]

# Create aggregate table with counts for both the # of trips from a CA and the # of trips to a CA
df_geocode_trip_3 = df_geocode_trip_2[["trip_start_timestamp", "pickup_community_area", "dropoff_community_area", "count"]].copy()
df_geocode_trip_4 = df_geocode_trip_3.groupby(["trip_start_timestamp", "pickup_community_area", "dropoff_community_area"])['count'].sum().reset_index()

df_temp = df_ready_trip.append(df_geocode_trip_4)
df_trip = df_temp.groupby(["trip_start_timestamp", "pickup_community_area", "dropoff_community_area"])['count'].sum().reset_index()



# Save tables in postgresql
cursor.execute("DELETE FROM trips_ca_geo")
connection.commit()

tuples = [tuple(x) for x in df_trip.to_numpy()]
cols = ','.join(list(df_trip.columns))

values = [cursor.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
query  = "INSERT INTO trips_ca_geo({0}) VALUES ".format(cols) + ",".join(values)

cursor.execute(query, tuples)
connection.commit()
            
