# Import libraries

import psycopg2

import numpy as np
import pandas as pd
import geopandas as gpd

from shapely.geometry import Polygon
import statsmodels.api as sm

import os
import dataframe_image as dfi



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

ca_query = """\
    SELECT * FROM boundaries;\
"""

zips_query = """\
    SELECT * FROM zip_poly\
    WHERE state = 'IL';\
"""

weekly_query = """\
    SELECT * FROM covid19weekly;\
"""

daily_query = """\
    SELECT * FROM covid19daily;\
"""

ccvi_query = """\
    SELECT community_area_or_zip as community_number, ccvi_score\
    FROM vulnerability\
    WHERE geography_type = 'CA';\
"""

forcasted_trips_query = """\
    SELECT *\
    FROM trips_ca_prediction;\
"""

actual_trips_query = """\
    SELECT *
    FROM trips_ca_geo
"""

# Load data for later
df_weekly = pd.read_sql(weekly_query, connection)
df_weekly['week_start'] = pd.to_datetime(df_weekly['week_start'])
df_weekly['week_end'] = pd.to_datetime(df_weekly['week_end'])

df_daily = pd.read_sql(daily_query, connection)
df_daily['lab_report_date'] = pd.to_datetime(df_daily['lab_report_date'])

df_ccvi = pd.read_sql(ccvi_query, connection)
df_ccvi['ccvi_score'] = df_ccvi['ccvi_score'].astype(float)
df_ccvi['community_number'] = df_ccvi['community_number'].astype(int)

df_forcasted = pd.read_sql(forcasted_trips_query, connection)
df_forcasted['date'] = pd.to_datetime(df_forcasted['date'])
df_forcasted = df_forcasted.rename(columns= {'community_area':'community_number'})
df_forcasted['community_number'] = df_forcasted['community_number'].astype(int)
df_forcasted['pickup_count'] = df_forcasted['pickup_count'].astype(float)
df_forcasted['dropoff_count'] = df_forcasted['dropoff_count'].astype(float)


df_actual = pd.read_sql(actual_trips_query, connection)
df_actual['pickup_community_area'] = df_actual['pickup_community_area'].astype(int)
df_actual['dropoff_community_area'] = df_actual['dropoff_community_area'].astype(int)

# Determine the mapping of each zip code to community areas
# Load data
df_ca = gpd.read_postgis(ca_query, connection_gis)
df_zips = gpd.read_postgis(zips_query, connection_gis)


# determine the area overlap between each zip and each community area
df_crosswalk = pd.DataFrame()

for i in range(0, len(df_zips)):
    for z in range(0, len(df_ca)):
        if df_zips.loc[i, 'geom'].intersection(df_ca.loc[z, 'geom']):
            x = df_zips.loc[i, 'geom'].intersection(df_ca.loc[z, 'geom'])
            temp = pd.DataFrame(data = {'zip': [df_zips.loc[i, 'zip_code']], 'community_name': [df_ca.loc[z, 'community']], 'community_number': [df_ca.loc[z, 'area_num_1']], 'intersection_area': [x.area]})
            df_crosswalk = df_crosswalk.append(temp)

# Create a total area column for each zip code and then compute each CA's percentage of that zip code
df_crosswalk = df_crosswalk.merge((df_crosswalk.groupby(['zip'])['intersection_area'].sum()), how='left', on='zip')
df_crosswalk = df_crosswalk.rename(columns = {'intersection_area_x': 'intersection_area', 'intersection_area_y': 'total_area'})
df_crosswalk['community_area_pct'] = df_crosswalk.intersection_area/df_crosswalk.total_area
df_crosswalk = df_crosswalk.drop_duplicates()

# Create weekly and daily metrics
df_weekly_test = df_weekly[['zip_code', 'week_start', 'week_end', 'cases_weekly']]
df_weekly_test.loc[df_weekly_test['cases_weekly'] == '', 'cases_weekly'] = '0'
df_weekly_test['cases_weekly'] = df_weekly_test['cases_weekly'].astype(float)
df_weekly_test = df_weekly_test[(df_weekly_test['week_start'] == '2021-10-03') & (df_weekly_test['week_end'] == '2021-10-09')]

df_daily_test = df_daily[['lab_report_date', 'cases_total']]
df_daily_test = df_daily_test[(df_daily_test['lab_report_date'] >= '2021-10-03') & (df_daily_test['lab_report_date'] <= '2021-10-09')]

df_daily_test['cases_total'] = df_daily_test['cases_total'].astype(int)
df_daily_test['weekly_total'] = df_daily_test['cases_total'].sum()

df_daily_test['sunday'] = df_daily_test.iloc[0, 1]/df_daily_test['weekly_total']
df_daily_test['monday'] = df_daily_test.iloc[1, 1]/df_daily_test['weekly_total']
df_daily_test['tuesday'] = df_daily_test.iloc[2, 1]/df_daily_test['weekly_total']
df_daily_test['wednesday'] = df_daily_test.iloc[3, 1]/df_daily_test['weekly_total']
df_daily_test['thursday'] = df_daily_test.iloc[4, 1]/df_daily_test['weekly_total']
df_daily_test['friday'] = df_daily_test.iloc[5, 1]/df_daily_test['weekly_total']
df_daily_test['saturday'] = df_daily_test.iloc[6, 1]/df_daily_test['weekly_total']

df_daily_test = df_daily_test.drop(['lab_report_date', 'cases_total', 'weekly_total'], axis = 1).reset_index(drop=True)
dates = ['2021-10-3', '2021-10-4', '2021-10-5', '2021-10-6', '2021-10-7', '2021-10-8', '2021-10-9']
df_daily_test['date'] = pd.to_datetime(dates)


# Merge covid case metrics and the zip/community area crosswalk file
final_test = pd.merge(df_crosswalk, df_weekly_test, how='inner', left_on = 'zip', right_on = 'zip_code')
final_test['community_weekly_cases'] = final_test['cases_weekly'] * final_test['community_area_pct']

final_test = final_test[['community_name', 'community_number', 'community_weekly_cases']].copy()
final_test = final_test.groupby(['community_name', 'community_number'], as_index=False)['community_weekly_cases'].sum()

final_test_1 = pd.DataFrame()
# Create time series
for i in range(0, len(final_test)):
    for x in range(0, len(df_daily_test)):
        day_cases = final_test.loc[i, 'community_weekly_cases'] * df_daily_test.iloc[0, x]
        temp = pd.DataFrame(data = {'community_name': [final_test.loc[i, 'community_name']], 'community_number': [final_test.loc[i, 'community_number']], 'case_count': [day_cases], 'date': [df_daily_test.loc[x, 'date']]})
        final_test_1 = final_test_1.append(temp)
final_test_1['date'] = pd.to_datetime(final_test_1['date'])
final_test_1['community_number'] = final_test_1['community_number'].astype(int)
final_test_1 = final_test_1.reset_index(drop=True)

# Add in forecasted trips
df_forcasted_test = df_forcasted[(df_forcasted['date'] == '2021-10-03') | (df_forcasted['date'] == '2021-10-04') | (df_forcasted['date'] == '2021-10-05') | 
(df_forcasted['date'] == '2021-10-06') | (df_forcasted['date'] == '2021-10-07') | (df_forcasted['date'] == '2021-10-08') | (df_forcasted['date'] == '2021-10-09')]

final_test_2 = pd.merge(final_test_1, df_forcasted_test, how='left', on=['date', 'community_number'])



# Now do train dataset
df_weekly_train = df_weekly[['zip_code', 'week_start', 'week_end', 'cases_weekly']]
df_weekly_train.loc[df_weekly_train['cases_weekly'] == '', 'cases_weekly'] = '0'
df_weekly_train['cases_weekly'] = df_weekly_train['cases_weekly'].astype(float)
df_weekly_train = df_weekly_train[(df_weekly_train['week_start'] == '2021-09-26') & (df_weekly_train['week_end'] == '2021-10-02')]

df_daily_train = df_daily[['lab_report_date', 'cases_total']]
df_daily_train = df_daily_train[(df_daily_train['lab_report_date'] >= '2021-09-26') & (df_daily_train['lab_report_date'] <= '2021-10-02')]

df_daily_train['cases_total'] = df_daily_train['cases_total'].astype(int)
df_daily_train['weekly_total'] = df_daily_train['cases_total'].sum()

df_daily_train['sunday'] = df_daily_train.iloc[0, 1]/df_daily_train['weekly_total']
df_daily_train['monday'] = df_daily_train.iloc[1, 1]/df_daily_train['weekly_total']
df_daily_train['tuesday'] = df_daily_train.iloc[2, 1]/df_daily_train['weekly_total']
df_daily_train['wednesday'] = df_daily_train.iloc[3, 1]/df_daily_train['weekly_total']
df_daily_train['thursday'] = df_daily_train.iloc[4, 1]/df_daily_train['weekly_total']
df_daily_train['friday'] = df_daily_train.iloc[5, 1]/df_daily_train['weekly_total']
df_daily_train['saturday'] = df_daily_train.iloc[6, 1]/df_daily_train['weekly_total']

df_daily_train = df_daily_train.drop(['lab_report_date', 'cases_total', 'weekly_total'], axis = 1).reset_index(drop=True)
dates = ['2021-09-26', '2021-09-27', '2021-09-28', '2021-09-29', '2021-09-30', '2021-10-01', '2021-10-02']
df_daily_train['date'] = pd.to_datetime(dates)


# Merge covid case metrics and the zip/community area crosswalk file
final_train = pd.merge(df_crosswalk, df_weekly_train, how='inner', left_on = 'zip', right_on = 'zip_code')
final_train['community_weekly_cases'] = final_train['cases_weekly'] * final_train['community_area_pct']

final_train = final_train[['community_name', 'community_number', 'community_weekly_cases']].copy()
final_train = final_train.groupby(['community_name', 'community_number'], as_index=False)['community_weekly_cases'].sum()

final_train_1 = pd.DataFrame()
# Create time series
for i in range(0, len(final_train)):
    for x in range(0, len(df_daily_train)):
        day_cases = final_train.loc[i, 'community_weekly_cases'] * df_daily_train.iloc[0, x]
        temp = pd.DataFrame(data = {'community_name': [final_train.loc[i, 'community_name']], 'community_number': [final_train.loc[i, 'community_number']], 'case_count': [day_cases], 'date': [df_daily_train.loc[x, 'date']]})
        final_train_1 = final_train_1.append(temp)
final_train_1['date'] = pd.to_datetime(final_train_1['date'])
final_train_1['community_number'] = final_train_1['community_number'].astype(int)
final_train_1 = final_train_1.reset_index(drop=True)


# Add in actual and forcasted trips
df_forcasted_train = df_forcasted[(df_forcasted['date'] == '2021-10-01') | (df_forcasted['date'] == '2021-10-02')]

df_actual['count'] = df_actual['count'].astype(float).astype(int)

df_actual_pickup = df_actual[["trip_start_timestamp", "pickup_community_area", "count"]].copy()
df_actual_dropoff = df_actual[["trip_start_timestamp", "dropoff_community_area", "count"]].copy()

df_actual_pickup_final = df_actual_pickup[["trip_start_timestamp", "pickup_community_area", "count"]].rename(columns = {"count": "pickup_count", "pickup_community_area": "community_number"})
df_actual_dropoff_final = df_actual_dropoff[["trip_start_timestamp", "dropoff_community_area", "count"]].rename(columns = {"count": "dropoff_count", "dropoff_community_area": "community_number"})

temp = pd.merge(df_actual_pickup_final, df_actual_dropoff_final, how='inner')

aggregate = temp.groupby(["trip_start_timestamp", "community_number"])[['pickup_count', 'dropoff_count']].sum().reset_index()
aggregate["date"] = pd.to_datetime(aggregate["trip_start_timestamp"])
aggregate = aggregate[(aggregate['date'] >= '2021-09-26') & (aggregate['date'] <= '2021-09-30')]
aggregate = aggregate.drop(columns=['trip_start_timestamp'])
df_trips = df_forcasted_train.append(aggregate, ignore_index=True)

final_train_2 = pd.merge(final_train_1, df_trips, how='left', on=['date', 'community_number'])
temp = final_train_2[final_train_2.isna().any(axis=1)]

# CCVI score
final_train_3 = pd.merge(final_train_2, df_ccvi, how = 'inner', on=['community_number'])



# Model work
final_test_2["date"] = final_test_2["date"].apply(lambda x: x.date())
final_test_2.set_index('date', inplace=True)

final_train_3["date"] = final_train_3["date"].apply(lambda x: x.date())
final_train_3.set_index('date', inplace=True)
#final_train_3 = final_train_3.asfreq('d')

community_numbers = list(final_train_3['community_number'].drop_duplicates())

one_day_results = pd.DataFrame()
for i in community_numbers:
    train = final_train_3[final_train_3['community_number'] == i]
    train = train['ccvi_score']

    train_exog = final_train_3[final_train_3['community_number'] == i]
    train_exog = train_exog.drop('ccvi_score', axis = 1)
    train_exog = train_exog.drop(['community_number', 'community_name'], axis = 1)

    test = final_test_2[final_test_2['community_number'] == i]
    test = test.drop(['community_number', 'community_name'], axis = 1)
    test = test.iloc[0, :]

    mod = sm.tsa.statespace.SARIMAX(train, exog = train_exog).fit()
    predictions = mod.predict(start = '2021-10-03', end = '2021-10-03', exog=test)
    temp = predictions.to_frame()
    temp['community_number'] = i
    one_day_results = one_day_results.append(temp)


week_results = pd.DataFrame()
for i in community_numbers:
    train = final_train_3[final_train_3['community_number'] == i]
    train = train['ccvi_score']

    train_exog = final_train_3[final_train_3['community_number'] == i]
    train_exog = train_exog.drop('ccvi_score', axis = 1)
    train_exog = train_exog.drop(['community_number', 'community_name'], axis = 1)

    test = final_test_2[final_test_2['community_number'] == i]
    test = test.drop(['community_number', 'community_name'], axis = 1)

    mod = sm.tsa.statespace.SARIMAX(train, exog = train_exog).fit()
    predictions = mod.predict(start = '2021-10-03', end = '2021-10-09', exog=test)
    temp = predictions.to_frame()
    temp['community_number'] = i
    week_results = week_results.append(temp)


one_day_results.loc[:, 'CCVI'] = ''
one_day_cutoff_1 = one_day_results.loc[:, 0].quantile(.33)
one_day_cutoff_2 = one_day_results.loc[:, 0].quantile(.66)
one_day_results.loc[one_day_results.loc[:, 0] <= one_day_cutoff_1, 'CCVI'] = 'LOW'
one_day_results.loc[(one_day_results.loc[:, 0] > one_day_cutoff_1) & (one_day_results.loc[:, 0] <= one_day_cutoff_2), 'CCVI'] = 'MEDIUM'
one_day_results.loc[one_day_results.loc[:, 0] > one_day_cutoff_2, 'CCVI'] = 'HIGH'

community_names = final_train_3[['community_name', 'community_number']].reset_index(drop=True).drop_duplicates()

one_day_results = one_day_results.merge(community_names, how = 'inner', on='community_number')

day = one_day_results[['community_name', 'CCVI']]
day.sort_values('community_name', inplace=True)

week_results = week_results.groupby(['community_number'], as_index=False)['predicted_mean'].median()

week_results.loc[:, 'CCVI'] = ''
week_cutoff_1 = week_results.iloc[:, 1].quantile(.33)
week_cutoff_2 = week_results.iloc[:, 1].quantile(.66)
week_results.loc[week_results.iloc[:, 1] <= week_cutoff_1, 'CCVI'] = 'LOW'
week_results.loc[(week_results.iloc[:, 1] > week_cutoff_1) & (week_results.iloc[:, 1] <= week_cutoff_2), 'CCVI'] = 'MEDIUM'
week_results.loc[week_results.iloc[:, 1] > week_cutoff_2, 'CCVI'] = 'HIGH'

week_results = week_results.merge(community_names, how = 'inner', on='community_number')

week = week_results[['community_name', 'CCVI']]
print(week)
week.sort_values('community_name', inplace=True)

os.chdir(r"C:\Users\jwnha\Documents\GitHub\chicago-dashboard\figs")
def save(name, strName):
    if os.path.isfile(strName):
        os.remove(strName)  
    dfi.export(name, strName)

save(day,"day-ccvi.png")
save(week,"week-ccvi.png")

