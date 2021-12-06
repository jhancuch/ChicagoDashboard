"""
Generate tables and charts to fullfill the requirements of Requirement 3.
"""

# Import libraries
import psycopg2
import sqlalchemy
import pandas as pd
import numpy as np
from datetime import date
import matplotlib.pyplot as plt
import os


# Connect to postgres database
connection = psycopg2.connect(user="",
                             password="",
                             host="",
                             port="",
                             database="")

cursor = connection.cursor()



# Import data

ccvi_query = """\
    SELECT distinct community_area_or_zip, community_area_name \
    FROM vulnerability \
    WHERE geography_type = 'CA' AND ccvi_category = 'HIGH';
    """

trips_query = """\
    SELECT trip_start_timestamp, pickup_community_area, dropoff_community_area, count
    FROM trips_ca_geo;
    """

df_ccvi = pd.read_sql(ccvi_query, connection)
df_trips = pd.read_sql(trips_query, connection)
df_trips['count'] = df_trips['count'].astype(float).astype(int)



# Create final table by merging with df_ccvi to get CA's with a CCVI category of HIGH

df_trip_pickup = df_trips[["trip_start_timestamp", "pickup_community_area", "count"]].copy()
df_trip_dropoff = df_trips[["trip_start_timestamp", "dropoff_community_area", "count"]].copy()

final_pickup = pd.merge(df_ccvi, df_trip_pickup, how='left', left_on="community_area_or_zip", right_on="pickup_community_area")
final_dropoff = pd.merge(df_ccvi, df_trip_dropoff, how='left', left_on="community_area_or_zip", right_on="dropoff_community_area")

final_pickup = final_pickup[["community_area_or_zip", "community_area_name", "trip_start_timestamp", "count"]].rename(columns = {"count": "pickup_count"})
final_dropoff = final_dropoff[["community_area_or_zip", "community_area_name", "trip_start_timestamp", "count"]].rename(columns = {"count": "dropoff_count"})

final_temp = pd.merge(final_pickup, final_dropoff, how='inner')

final = final_temp.groupby(["community_area_or_zip", "community_area_name", "trip_start_timestamp"])[['pickup_count', 'dropoff_count']].sum().reset_index()

final["trip_start_timestamp"] = pd.to_datetime(final["trip_start_timestamp"])
final = final[(final['trip_start_timestamp'] >= '2020-10-01') & (final['trip_start_timestamp'] <= '2021-09-30')]
final["trip_start_timestamp"] = final["trip_start_timestamp"].apply(lambda x: x.date()) 

# Find the difference between today's date and the timestamp in the dataframe. This value is used to generate monthly, weekly, and daily metrics
final["dif"] = date.today() - final["trip_start_timestamp"]
final = final.sort_values(["dif"])


# Create final datasets for day, week, and month charts
final_day = final.loc[final['dif'] == min(final['dif'])]

week = final[["trip_start_timestamp", "dif"]].drop_duplicates()
week_list = week.iloc[0:7,]
final_week = pd.merge(final, week_list)

month = final[["trip_start_timestamp", "dif"]].drop_duplicates()
month_list = month.iloc[0:30,]
final_month = pd.merge(final, month_list)




# Day Table and Charts

# table
day_table = final_day[["community_area_or_zip", "community_area_name", "pickup_count", "dropoff_count"]].reset_index(drop=True).sort_values("community_area_name")
day_table = day_table.rename(columns = {"community_area_or_zip": "Community Area Number", "community_area_name": "Community Area Name", 
                                        "pickup_count": "Number of Trips From Community Area", 
                                          "dropoff_count":"Number of Trips To Community Area"})
# Charts
day1 = plt.figure(figsize=(14,7))

ax = plt.gca()
plt.xlabel('Number of Trips', figure=day1)
plt.ylabel('Community Areas', figure=day1)
plt.title('Total Number of Trips to and from Community Areas with HIGH CCVI on {0}'.format(min(final_day["trip_start_timestamp"])), figure=day1)

plt.barh(day_table["Community Area Name"], day_table["Number of Trips From Community Area"], color='#9ecae1', figure=day1)
plt.barh(day_table["Community Area Name"], day_table["Number of Trips To Community Area"], left = day_table["Number of Trips From Community Area"], color = '#08306b', figure=day1)

plt.legend(["Trips From Community Area", "Trips To Community Area"])
plt.gcf().axes[0].xaxis.get_major_formatter().set_scientific(False)

#plt.show()




# Week Table and Charts

# table
week_table = final_week[["community_area_or_zip", "community_area_name", "trip_start_timestamp", "pickup_count", "dropoff_count"]].reset_index(drop=True).sort_values("community_area_name")

week_table = week_table.rename(columns = {"community_area_or_zip": "Community Area Number", "community_area_name": "Community Area Name", 
                                          "trip_start_timestamp": "Date", "pickup_count": "Number of Trips From Community Area", 
                                          "dropoff_count":"Number of Trips To Community Area"})
# Group by CA
chart_by_ca_week = week_table.groupby(["Community Area Number", "Community Area Name"])["Number of Trips From Community Area", "Number of Trips To Community Area"].sum().reset_index()
chart_by_ca_week.loc[:, "Total Trips"] = chart_by_ca_week["Number of Trips From Community Area"] + chart_by_ca_week["Number of Trips To Community Area"]
chart_by_ca_week = chart_by_ca_week.sort_values("Community Area Name")

# Charts
week1 = plt.figure(figsize=(14,7))

ax = plt.gca()
plt.xlabel('Number of Trips', figure=week1)
plt.ylabel('Community Areas', figure=week1)
plt.title('Total Number of Trips to and from Community Areas with HIGH CCVI from {0} to {1} by Community Area'.format(min(final_week["trip_start_timestamp"]), max(final_week["trip_start_timestamp"])), figure=week1)

plt.barh(chart_by_ca_week["Community Area Name"], chart_by_ca_week["Number of Trips From Community Area"], color='#9ecae1', figure=week1)
plt.barh(chart_by_ca_week["Community Area Name"], chart_by_ca_week["Number of Trips To Community Area"], left = chart_by_ca_week["Number of Trips From Community Area"], color = '#08306b', figure=week1)
plt.legend(["Trips From Community Area", "Trips To Community Area"])
plt.gcf().axes[0].xaxis.get_major_formatter().set_scientific(False)
#plt.show()

# Group by date
chart_by_date_week = week_table.groupby(["Date"])["Number of Trips From Community Area", "Number of Trips To Community Area"].sum().reset_index()
chart_by_date_week.loc[:, "Total Trips"] = chart_by_date_week["Number of Trips From Community Area"] + chart_by_date_week["Number of Trips To Community Area"]

# Charts
week2 = plt.figure(figsize=(14,5))

ax = plt.gca()
plt.ylabel('Number of Trips', figure=week2)
plt.title('Total Number of Trips to and from Community Areas with HIGH CCVI from {0} to {1}'.format(min(final_week["trip_start_timestamp"]), max(final_week["trip_start_timestamp"])), figure=week2)
ax.set_xticklabels(labels=chart_by_date_week["Date"],rotation=0, figure=week2)

plt.bar(chart_by_date_week["Date"], chart_by_date_week["Number of Trips From Community Area"], color='#9ecae1', figure=week2)
plt.bar(chart_by_date_week["Date"], chart_by_date_week["Number of Trips To Community Area"], bottom = chart_by_date_week["Number of Trips From Community Area"], color = '#08306b', figure=week2)
plt.legend(["Trips From Community Area", "Trips To Community Area"])
plt.gcf().axes[0].xaxis.get_major_formatter().set_scientific(False)

#plt.show()




# Month Table and Charts
month_table = final_month[["community_area_or_zip", "community_area_name", "trip_start_timestamp", "pickup_count", "dropoff_count"]].reset_index(drop=True).sort_values("community_area_name")

month_table = month_table.rename(columns = {"community_area_or_zip": "Community Area Number", "community_area_name": "Community Area Name", 
                                          "trip_start_timestamp": "Date", "pickup_count": "Number of Trips From Community Area", 
                                          "dropoff_count":"Number of Trips To Community Area"})
# Group by CA
chart_by_ca_month = month_table.groupby(["Community Area Number", "Community Area Name"])["Number of Trips From Community Area", "Number of Trips To Community Area"].sum().reset_index()
chart_by_ca_month.loc[:, "Total Trips"] = chart_by_ca_month["Number of Trips From Community Area"] + chart_by_ca_month["Number of Trips To Community Area"]
chart_by_ca_month = chart_by_ca_month.sort_values("Community Area Name")

# Charts
month1 = plt.figure(figsize=(14,7))

ax = plt.gca()
plt.xlabel('Number of Trips', figure=month1)
plt.ylabel('Community Areas', figure=month1)
plt.title('Total Number of Trips to and from Community Areas with HIGH CCVI from {0} to {1} by Community Area'.format(min(final_month["trip_start_timestamp"]), max(final_month["trip_start_timestamp"])), figure=month1)

plt.barh(chart_by_ca_month["Community Area Name"], chart_by_ca_month["Number of Trips From Community Area"], color='#9ecae1', figure=month1)
plt.barh(chart_by_ca_month["Community Area Name"], chart_by_ca_month["Number of Trips To Community Area"], left = chart_by_ca_month["Number of Trips From Community Area"], color = '#08306b', figure=month1)
plt.legend(["Trips From Community Area", "Trips To Community Area"])
plt.gcf().axes[0].xaxis.get_major_formatter().set_scientific(False)

#plt.show()

# Group by date
chart_by_date_month = month_table.groupby(["Date"])["Number of Trips From Community Area", "Number of Trips To Community Area"].sum().reset_index()
chart_by_date_month.loc[:, "Total Trips"] = chart_by_date_month["Number of Trips From Community Area"] + chart_by_date_month["Number of Trips To Community Area"]

# Charts
month2 = plt.figure(figsize=(14,5))

ax = plt.gca()
plt.xlabel('Date', figure=month2)
plt.ylabel('Number of Trips', figure=month2)
plt.title('Total Number of Trips to and from Community Areas with HIGH CCVI from {0} to {1}'.format(min(final_month["trip_start_timestamp"]), max(final_month["trip_start_timestamp"])), figure=month2)
x = list(range(len(chart_by_date_month["Date"])))
labels = chart_by_date_month["Date"]

plt.bar(x, chart_by_date_month["Number of Trips From Community Area"], color='#9ecae1', figure=month2)
plt.bar(x, chart_by_date_month["Number of Trips To Community Area"], bottom = chart_by_date_month["Number of Trips From Community Area"], color = '#08306b', figure=month2)
plt.gcf().axes[0].xaxis.get_major_formatter().set_scientific(False)
plt.xticks(x, labels, rotation=90)
plt.tight_layout()
plt.legend(["Trips From Community Area", "Trips To Community Area"])
#plt.show()



# Save files to file system following best practices
os.chdir(r"C:\Users\jwnha\Documents\GitHub\chicago-dashboard\figs")

def save(name, strName):
    if os.path.isfile(strName):
        os.remove(strName)  
    name.savefig(strName)

save(day1, "day1.png")
save(week1, "week1.png")
save(week2, "week2.png")
save(month1, "month1.png")
save(month2, "month2.png")
