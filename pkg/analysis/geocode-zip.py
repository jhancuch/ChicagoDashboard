"""
Takes trip observations that have lat/long and determine zipcode of pick-up and drop off
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

# Create queries and query Postgres
trip_query = """\
    SELECT trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
    dropoff_centroid_longitude, sum(count) as count \
    FROM ( \
    \
            SELECT trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude, sum(count) as count
            FROM taxi2021 \
            GROUP BY trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude\
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude, count(*) as count \
            FROM tnc21 \
            GROUP BY trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude\
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude, sum(count) as count
            FROM taxi2020 \
            GROUP BY trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude\
            \
            UNION ALL\
            \
            SELECT trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude, count(*) as count \
            FROM tnc20 \
            GROUP BY trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
            dropoff_centroid_longitude\
    ) as a\
    GROUP BY trip_start_timestamp, pickup_centroid_latitude, pickup_centroid_longitude, dropoff_centroid_latitude,\
    dropoff_centroid_longitude;
"""

zips_query = """\
    SELECT * FROM zip_poly;
"""

df_trips= pd.read_sql(trip_query, connection)
df_zips = gpd.read_postgis(zips_query, connection_gis)

# Clean geocode trip dataframe - drop any observations that have a blank vaue for coordinates. Also reduce the # of polygons to check
df_trips1 = df_trips[(df_trips.pickup_centroid_latitude != "") & 
    (df_trips.pickup_centroid_longitude != "") & 
    (df_trips.dropoff_centroid_latitude != "") & 
    (df_trips.dropoff_centroid_longitude != "")]

df_zips_il = df_zips[df_zips['state'] == 'IL']

# Create new dataframes with only unique lat/long observations to cut down on computation
df_pickup = df_trips1[["pickup_centroid_latitude", "pickup_centroid_longitude"]].drop_duplicates()
df_dropoff = df_trips1[["dropoff_centroid_latitude", "dropoff_centroid_longitude"]].drop_duplicates()

# Find zip for pickup locations
df_pickup1 = gpd.GeoDataFrame(df_pickup, 
                                     geometry=gpd.points_from_xy(
                                        df_pickup.pickup_centroid_longitude, df_pickup.pickup_centroid_latitude)).reset_index()
df_pickup1.loc[:, 'pickup_zip'] = np.nan

for fi in df_pickup1.index:
    for si in df_zips_il.index:
        if df_zips_il.loc[si, "geom"].contains(df_pickup1.loc[fi, "geometry"]):
            df_pickup1.loc[fi, "pickup_zip"] = df_zips_il.loc[si, "zip_code"]

# Find zip for dropoff locations
df_dropoff1 = gpd.GeoDataFrame(df_dropoff, 
                                     geometry=gpd.points_from_xy(
                                        df_dropoff.dropoff_centroid_longitude, df_dropoff.dropoff_centroid_latitude)).reset_index()
df_dropoff1.loc[:, 'dropoff_zip'] = np.nan

for fi in df_dropoff1.index:
    for si in df_zips_il.index:
        if df_zips_il.loc[si, "geom"].contains(df_dropoff1.loc[fi, "geometry"]):
            df_dropoff1.loc[fi, "dropoff_zip"] = df_zips_il.loc[si, "zip_code"]

# Join the aquired zip code information back to the main dataset
df_pickup1_merge = df_pickup1[['pickup_centroid_longitude', 'pickup_centroid_latitude', 'pickup_zip']]
df_trips2 = pd.merge(df_trips1, df_pickup1_merge, how='left', on = ['pickup_centroid_longitude', 'pickup_centroid_latitude'])

df_dropoff1_merge = df_dropoff1[['dropoff_centroid_longitude', 'dropoff_centroid_latitude', 'dropoff_zip']]
df_trips3 = pd.merge(df_trips2, df_dropoff1_merge, how='left', on = ['dropoff_centroid_longitude', 'dropoff_centroid_latitude'])

df_trips4 = df_trips3.groupby(["trip_start_timestamp", "pickup_zip", "dropoff_zip"])['count'].sum().reset_index()



# Save tables in postgresql
cursor.execute("DELETE FROM trips_zip_geo")
connection.commit()

tuples = [tuple(x) for x in df_trips4.to_numpy()]
cols = ','.join(list(df_trips4.columns))

values = [cursor.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
query  = "INSERT INTO trips_zip_geo({0}) VALUES ".format(cols) + ",".join(values)

cursor.execute(query, tuples)
connection.commit()