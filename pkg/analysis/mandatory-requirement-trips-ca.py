"""
forecast the daily, weekly, and monthly taxi trips for every community area code
"""

import os
from datetime import date
import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
import holidays



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

"""
CA
"""
def forecast_ca(df, x, z, col):
    dat = df[df['community_area'] == z]
    print("Length of timeseries {}".format(len(dat)))
    dat = dat[['trip_start_timestamp', col]].copy()
    dat.sort_values('trip_start_timestamp', ascending=True, inplace=True)
    
    if len(dat) == 365:
        dat.sort_values('trip_start_timestamp', ascending=True, inplace=True)
        dat.set_index('trip_start_timestamp', inplace=True)
        dat = dat.asfreq('d')
    
    else:
        idx = pd.date_range('2020-10-01', '2021-09-30')
        dat.sort_values('trip_start_timestamp', ascending=True, inplace=True)
        dat.set_index('trip_start_timestamp', inplace=True)
        dat = dat.reindex(idx, fill_value=0)
        dat = dat.asfreq('d')
        print(dat)

    train_exogenous = df[df['community_area'] == z]
    print("Length of exogenous train dataset {}".format(len(train_exogenous)))
    train_exogenous = train_exogenous[['trip_start_timestamp', 'is_weekend_fss', 'day_of_week_0', 'day_of_week_1', 'day_of_week_2', 'day_of_week_3', 
    'day_of_week_4', 'day_of_week_5', 'day_of_week_6', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 
    'month_8', 'month_9', 'month_10', 'month_11', 'month_12', 'holiday']].copy()
    train_exogenous = train_exogenous.replace([np.inf, -np.inf], np.nan)
    train_exogenous = train_exogenous.fillna(0)
    
    if len(train_exogenous) == 365:
        train_exogenous.sort_values('trip_start_timestamp', ascending=True, inplace=True)
        train_exogenous.set_index('trip_start_timestamp', inplace=True)
        train_exogenous = train_exogenous.asfreq('d')

    else:
        idx = pd.date_range('2020-10-01', '2021-09-30')
        train_exogenous.sort_values('trip_start_timestamp', ascending=True, inplace=True)
        train_exogenous.set_index('trip_start_timestamp', inplace=True)
        train_exogenous = train_exogenous.reindex(idx, fill_value=0)
        train_exogenous = train_exogenous.asfreq('d')

    model_exogenous = x[['trip_start_timestamp', 'is_weekend_fss', 'day_of_week_0', 'day_of_week_1', 'day_of_week_2', 'day_of_week_3', 
    'day_of_week_4', 'day_of_week_5', 'day_of_week_6', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 
    'month_8', 'month_9', 'month_10', 'month_11', 'month_12', 'holiday']].copy()
    print("Length of exogenous model dataset {}".format(len(model_exogenous)))
    model_exogenous = model_exogenous.replace([np.inf, -np.inf], np.nan)
    model_exogenous = model_exogenous.fillna(0)
    model_exogenous.sort_values('trip_start_timestamp', ascending=True, inplace=True)
    model_exogenous.set_index('trip_start_timestamp', inplace=True)
    model_exogenous = model_exogenous.asfreq('d')

    mod = sm.tsa.statespace.SARIMAX(dat, exog = train_exogenous, order=(0,1,0), seasonal_order=(1,1,1,52)).fit()
    predictions = mod.predict(start = '2021-10-01', end = '2022-09-30', exog=model_exogenous)
    return predictions.to_frame()


# Pull in data
ca_query = """\
    SELECT *
    FROM trips_ca_geo;
    """

boundaries_query = """\
    SELECT area_num_1, community FROM boundaries;
"""

df_ca = pd.read_sql(ca_query, connection)
df_ca['count'] = df_ca['count'].astype(float).astype(int)

df_boundaries = pd.read_sql(boundaries_query, connection_gis)
df_boundaries['area_num_1'] = df_boundaries['area_num_1'].astype(int)
df_boundaries['community'] = df_boundaries['community'].str.title()


# Modify table so we have a count of trips to and from each community area on each day
df_ca_pickup = df_ca[["trip_start_timestamp", "pickup_community_area", "count"]].copy()
df_ca_dropoff = df_ca[["trip_start_timestamp", "dropoff_community_area", "count"]].copy()

df_ca_pickup_final = df_ca_pickup[["trip_start_timestamp", "pickup_community_area", "count"]].rename(columns = {"count": "pickup_count", "pickup_community_area": "community_area"})
df_ca_dropoff_final = df_ca_dropoff[["trip_start_timestamp", "dropoff_community_area", "count"]].rename(columns = {"count": "dropoff_count", "dropoff_community_area": "community_area"})

temp = pd.merge(df_ca_pickup_final, df_ca_dropoff_final, how='inner')

aggregate = temp.groupby(["trip_start_timestamp", "community_area"])[['pickup_count', 'dropoff_count']].sum().reset_index()

# Feature engineering - time
aggregate["trip_start_timestamp"] = pd.to_datetime(aggregate["trip_start_timestamp"])
aggregate = aggregate[(aggregate['trip_start_timestamp'] >= '2020-10-01') & (aggregate['trip_start_timestamp'] <= '2021-09-30')]

aggregate["month"] = aggregate["trip_start_timestamp"].dt.month
aggregate["day"] = aggregate["trip_start_timestamp"].dt.day
aggregate["day_of_week"] = aggregate["trip_start_timestamp"].dt.day_of_week
aggregate["is_weekend_fss"] = np.where(aggregate['day_of_week'].isin([4,5,6]), 1, 0)

aggregate = pd.get_dummies(aggregate, columns=['day_of_week'])
aggregate = pd.get_dummies(aggregate, columns=['month'])

us_holidays = holidays.US()
aggregate.loc[:, 'holiday'] = 0
for i in aggregate.index:
    if aggregate.loc[i, 'trip_start_timestamp'] in us_holidays:
        aggregate.loc[i, 'holiday'] = 1


# Create time exogenous variables
future = pd.DataFrame(pd.date_range(aggregate["trip_start_timestamp"].max() + pd.DateOffset(days=1), aggregate["trip_start_timestamp"].max() + pd.DateOffset(days=365)), columns=['trip_start_timestamp'])
future["month"] = future["trip_start_timestamp"].dt.month
future["day"] = future["trip_start_timestamp"].dt.day
future["day_of_week"] = future["trip_start_timestamp"].dt.day_of_week
future["is_weekend_fss"] = np.where(future['day_of_week'].isin([4,6]), 1, 0)

future = pd.get_dummies(future, columns=['day_of_week'])
future = pd.get_dummies(future, columns=['month'])

us_holidays = holidays.US()
future.loc[:, 'holiday'] = 0
for i in future.index:
    if future.loc[i, 'trip_start_timestamp'] in us_holidays:
        future.loc[i, 'holiday'] = 1

# Create necessary objects
ca = list(aggregate["community_area"].drop_duplicates())
final_pickup = pd.DataFrame()
final_dropoff = pd.DataFrame()


# Obtain predictions for the next year from the model

for i in ca:
    print(i)
    temp = forecast_ca(aggregate, future, i, 'pickup_count')
    temp['community_area'] = i
    final_pickup = final_pickup.append(temp)

for i in ca:
    print(i)
    temp = forecast_ca(aggregate, future, i, 'dropoff_count')
    temp['community_area'] = i
    final_dropoff = final_dropoff.append(temp)



# General clean up
final_pickup['predicted_mean'] = np.abs(final_pickup['predicted_mean'])
final_dropoff['predicted_mean'] = np.abs(final_dropoff['predicted_mean'])

final_pickup.rename(columns = {'predicted_mean':'pickup_count'}, inplace = True)
final_dropoff.rename(columns = {'predicted_mean':'dropoff_count'}, inplace = True)

final_pickup['date'] = pd.to_datetime(final_pickup.index)
final_pickup["date"] = final_pickup["date"].apply(lambda x: x.date())

final_dropoff['date'] = pd.to_datetime(final_dropoff.index)
final_dropoff["date"] = final_dropoff["date"].apply(lambda x: x.date())

final = pd.merge(final_pickup, final_dropoff, on = ['date', 'community_area'])

final = pd.merge(final, df_boundaries, how='inner', left_on='community_area', right_on='area_num_1')


# Generate day, week, and month charts
final["dif"] = np.nan

for i in range(0, len(final)):
    final.loc[i, "dif"] = date.today() - final.loc[i, "date"]
final = final.sort_values(["dif"])


# Create final datasets for day, week, and month charts
final_day = final.loc[final['dif'] == max(final['dif'])]

week = final[["date", "dif"]].drop_duplicates()
week_list = week.iloc[-7:]
final_week = pd.merge(final, week_list)

month = final[["date", "dif"]].drop_duplicates()
month_list = month.iloc[-30:]
final_month = pd.merge(final, month_list)

# Day Charts
day_table = final_day[["date", "community", "pickup_count", "dropoff_count"]].reset_index(drop=True).sort_values("community")

day1 = plt.figure(figsize=(14,7))

ax = plt.gca()
plt.xlabel('Community Area', figure=day1)
plt.ylabel('Forecasted Number of Trips', figure=day1)
plt.title('Forecasted Number of Trips to and from Community Areas on {0}'.format(min(final_day["date"])), figure=day1)
x = list(range(len(day_table["community"])))
labels = day_table["community"]

plt.bar(x, day_table["pickup_count"], label = 'Pickups', color='#9ecae1', figure=day1)
plt.bar(x, day_table["dropoff_count"], label = 'Dropoffs', bottom = day_table["pickup_count"], color = '#08306b', figure=day1)
plt.ticklabel_format(style='plain')
plt.xticks(x, labels, rotation=90)
plt.tight_layout()
plt.legend(["Forecasted Trips From Community Area", "Forecasted Trips To Community Area"])

# Week Charts
week_table = final_week[["date", "community", "pickup_count", "dropoff_count"]].reset_index(drop=True).sort_values("community")

# Group by CA
chart_by_ca_week = week_table.groupby(["community"])["pickup_count", "dropoff_count"].sum().reset_index()
chart_by_ca_week.loc[:, "Total Trips"] = chart_by_ca_week["pickup_count"] + chart_by_ca_week["dropoff_count"]
chart_by_ca_week = chart_by_ca_week.sort_values("community")

# Charts
week1 = plt.figure(figsize=(14,7))

ax = plt.gca()
plt.xlabel('Community Areas', figure=week1)
plt.ylabel('Forecasted Number of Trips', figure=week1)
plt.title('Forecasted Total Number of Trips to and from Community Areas from {0} to {1}'.format(min(final_week["date"]), max(final_week["date"])), figure=week1)
x = list(range(len(chart_by_ca_week["community"])))
labels = chart_by_ca_week["community"]

plt.bar(x, chart_by_ca_week["pickup_count"], label = 'Pickups', color='#9ecae1', figure=week1)
plt.bar(x, chart_by_ca_week["dropoff_count"], label = 'Dropoffs', bottom = chart_by_ca_week["pickup_count"], color = '#08306b', figure=week1)
plt.ticklabel_format(style='plain')
plt.xticks(x, labels, rotation=90)
plt.tight_layout()
plt.legend(["Forecasted Trips From Community Area", "Forecasted Trips To Community Area"])

# Group by date
chart_by_date_week = week_table.groupby(["date"])["pickup_count", "dropoff_count"].sum().reset_index()
chart_by_date_week.loc[:, "Total Trips"] = chart_by_date_week["pickup_count"] + chart_by_date_week["dropoff_count"]
chart_by_date_week = chart_by_date_week.sort_values("date")

# Charts
week2 = plt.figure(figsize=(14,5))

ax = plt.gca()
plt.xlabel('Date', figure=week2)
plt.ylabel('Forecasted Number of Trips', figure=week2)
plt.title('Forecasted Total Number of Trips to and from Community Areas from {0} to {1}'.format(min(final_week["date"]), max(final_week["date"])), figure=week2)
ax.set_xticklabels(labels=chart_by_date_week["date"], rotation=0, figure=week2)

plt.bar(chart_by_date_week["date"], chart_by_date_week["pickup_count"], color='#9ecae1', figure=week2)
plt.bar(chart_by_date_week["date"], chart_by_date_week["dropoff_count"], bottom = chart_by_date_week["pickup_count"], color = '#08306b', figure=week2)
plt.gcf().axes[0].yaxis.get_major_formatter().set_scientific(False)
plt.tight_layout()
plt.legend(["Trips From Community Area", "Trips To Community Area"])


# Month Charts
month_table = final_month[["date", "community", "pickup_count", "dropoff_count"]].reset_index(drop=True).sort_values("community")

# Group by CA
chart_by_ca_month = month_table.groupby(["community"])["pickup_count", "dropoff_count"].sum().reset_index()
chart_by_ca_month.loc[:, "Total Trips"] = chart_by_ca_month["pickup_count"] + chart_by_ca_month["dropoff_count"]
chart_by_ca_month = chart_by_ca_month.sort_values("community")

# Charts
month1 = plt.figure(figsize=(14,7))

ax = plt.gca()
plt.xlabel('Community Areas', figure=month1)
plt.ylabel('Forecasted Number of Trips', figure=month1)
plt.title('Forecasted Total Number of Trips to and from Community Areas from {0} to {1}'.format(min(final_month["date"]), max(final_month["date"])), figure=month1)
x = list(range(len(chart_by_ca_month["community"])))
labels = chart_by_ca_month["community"]

plt.bar(x, chart_by_ca_month["pickup_count"], label = 'Pickups', color='#9ecae1', figure=month1)
plt.bar(x, chart_by_ca_month["dropoff_count"], label = 'Dropoffs', bottom = chart_by_ca_month["pickup_count"], color = '#08306b', figure=month1)
plt.ticklabel_format(style='plain')
plt.xticks(x, labels, rotation=90)
plt.tight_layout()
plt.legend(["Forecasted Trips From Community Area", "Forecasted Trips To Community Area"])

# Group by date
chart_by_date_month = month_table.groupby(["date"])["pickup_count", "dropoff_count"].sum().reset_index()
chart_by_date_month.loc[:, "Total Trips"] = chart_by_date_month["pickup_count"] + chart_by_date_month["dropoff_count"]
chart_by_date_month = chart_by_date_month.sort_values("date")

# Charts
month2 = plt.figure(figsize=(14,5))

ax = plt.gca()
plt.xlabel('Date', figure=month2)
plt.ylabel('Forecasted Number of Trips', figure=month2)
plt.title('Forecasted Total Number of Trips to and from Community Areas from {0} to {1}'.format(min(final_month["date"]), max(final_month["date"])), figure=month2)
x = list(range(len(chart_by_date_month["date"])))
labels = chart_by_date_month["date"]

plt.bar(x, chart_by_date_month["pickup_count"], color='#9ecae1', figure=month2)
plt.bar(x, chart_by_date_month["dropoff_count"], bottom = chart_by_date_month["pickup_count"], color = '#08306b', figure=month2)
plt.gcf().axes[0].yaxis.get_major_formatter().set_scientific(False)
plt.xticks(x, labels, rotation=90)
plt.tight_layout()
plt.legend(["Trips From Community Area", "Trips To Community Area"])



# Save files to file system following best practices
os.chdir(r"C:\Users\jwnha\Documents\GitHub\chicago-dashboard\figs")

def save(name, strName):
    if os.path.isfile(strName):
        os.remove(strName)  
    name.savefig(strName)

save(day1, "day1-ca.png")
save(week1, "week1-ca.png")
save(week2, "week2-ca.png")
save(month1, "month1-ca.png")
save(month2, "month2-ca.png")


# Save data in Postgresql
insert = final[["date", "community_area", "pickup_count", "dropoff_count"]]

# Save tables in postgresql
cursor.execute("DELETE FROM trips_ca_prediction")
connection.commit()

tuples = [tuple(x) for x in insert.to_numpy()]
cols = ','.join(list(insert.columns))

values = [cursor.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
query  = "INSERT INTO trips_ca_prediction({0}) VALUES ".format(cols) + ",".join(values)

cursor.execute(query, tuples)
connection.commit()