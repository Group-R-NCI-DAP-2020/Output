
import matplotlib.pyplot as plt
import pandas.io.sql as sqlio
import psycopg2
import scipy.spatial as spt

import pandas
import geopandas
import geoplot







totalPrimary = totalPrimary[(totalPrimary['Latitude'] != '.')]
totalPrimary['Latitude'] = (totalPrimary['Latitude']).astype(float)
totalPrimary['Longitude'] =(totalPrimary['Longitude']).astype(float)
totalPrimary = totalPrimary[(totalPrimary.Longitude > -11) & (totalPrimary.Longitude < -5) & (totalPrimary.Latitude > 51) & (totalPrimary.Latitude < 56)]
len(totalPrimary)

totalSecondary = totalSecondary[(totalSecondary['Latitude'] != '.')]
totalSecondary['Latitude'] = (totalSecondary['Latitude']).astype(float)
totalSecondary['Longitude'] =(totalSecondary['Longitude']).astype(float)
totalSecondary = totalSecondary[(totalSecondary['Longitude'] > -11.0) & (totalSecondary['Longitude'] < -5.0) & (totalSecondary['Latitude'] > 51.0) & (totalSecondary['Latitude'] < 56.0)]
len(totalSecondary)




## connect to database and plot it

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import matplotlib.pyplot as plt

sql = """select latitude, longitude from primaryschools;"""
sql2 = """select latitude, longitude from secondaryschools;"""

try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    latlong = sqlio.read_sql_query(sql, dbConnection)
    latlong2 = sqlio.read_sql_query(sql2, dbConnection)

except (Exception, psycopg2.Error) as dbError:
    print("Error while connecting to postgresql: ", dbError)
else:
    print("connection successful")
finally:
    if (dbConnection):
        dbConnection.close()

latlong = latlong[(latlong['longitude'] > -11.0) & (latlong['longitude'] < -5.0) & (latlong['latitude'] > 51.0) & (latlong['latitude'] < 56.0)]
latlong2 = latlong2.loc[(latlong2['longitude'] > -11.0) & (latlong2['longitude'] < -5.0) & (latlong2['latitude'] > 51.0) & (latlong2['latitude'] < 56.0)]

plt.axis ('off')
plt.scatter(latlong['longitude'], latlong['latitude'], c="b" , s=0.8, label ="Primary Schools")
plt.scatter(latlong2['longitude'], latlong2['latitude'], c="r" , s=0.8, label ="Secondary Schools")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.legend("Distribution of Schools across Ireland")
plt.show()



""" 
now lets do the same but use some geopandas on a shapefile of Ireland by Electoral Division 
"""


zipfile = r"D:\Shape2\c578ea0c-404e-4a58-9c6a-b2367344377f2020329-1-1tqhsy4.d3d5.shp"
iremap = geopandas.read_file(zipfile)


g = iremap.plot(figsize=(15,15), column = 'HP2016abs' , edgecolor="purple", facecolor="None")
x,y = latlong['longitude'].values, latlong['latitude'].values
x1,y1 = latlong2['longitude'].values, latlong2['latitude'].values

plt.scatter(x,y, color = 'r', s=15,  label ="Primary Schools")
plt.scatter(x1, y1, color = 'b', s=15, label ="Secondary Schools")
plt.legend(loc = 'best')
plt.show()


#add deprivation index based on ED info in pobal dataset and shapefile
#use this to create a Chorleoplot

pobaldfsub = pobaldf[['ED_Name', 'HP2016abs', 'TOTPOP16', 'EDHIGH16','HLPROF16' ,'LARENT16', 'UNEMPM16', 'UNEMPF16'  ]]

iremap = iremap.merge(pobaldfsub, left_on='ED_ENGLISH', right_on = 'ED_Name')

h = iremap.plot(figsize=(15,15), column = 'UNEMPM16' , edgecolor="purple", legend = True)
plt.scatter(x,y, color = 'w', s=5,  label ="Primary Schools")
plt.scatter(x1, y1, color = 'r', s=5, label ="Secondary Schools")
plt.legend(loc = 'best')
plt.show()

