"""Codesset to connect to a website and download a JSON file of data for class assignment"""

import urllib
import urllib.request
import pandas as pd
import json
import io
import re
import string


"""

Next datasets will all be based on Electoral Divisions to allow merging of various information elements


"""
#Load Pobal deprivation index 2016 from website  -link is via gov.data.ie
#code loads this as a pandas dataframe at ED level - 3409 records
#Interpretative details at http://trutzhaase.eu/wp/wp-content/uploads/The-2016-Pobal-HP-Deprivation-Index-Introduction-07.pdf

depInd = r'http://trutzhaase.eu/wp/wp-content/uploads/HP-Index-2006-2016-HP-Index-Scores-by-ID06b2.xls'
pobaldf = pd.read_excel(depInd,  header = 0)

pobaldf['countyname'] = pobaldf['NUTS4'].str.replace(r' City', '')
pobaldf['countyname'] = pobaldf['countyname'].str.replace(r'Dun Laoghaire/Rathdown|Dublin Fingal|South County Dublin', 'Dublin')



#Townlands.ie is a community mapping project that has estimated co-ordinates for the centre of each
#electoral division.  we can combine this with the Pobal ED level data to help with making these data usable
# data link is https://www.townlands.ie/static/downloads/eds.zip
import numpy as np
EDdata = pd.read_csv('eds.csv')
EDdata['NAME_TAG'] = EDdata['NAME_TAG'].str.upper()
EDdata['CO_NAME'] = EDdata['CO_NAME'].str.replace(r'[0-9]+', '')
EDdata['CO_NAME'].replace('', np.nan, inplace=True)
EDdata['CO_NAME'].replace('', np.nan, inplace=True)


#EDdata = EDdata[EDdata['CO_NAME'] != '']

# combine the two datasets based on ED assigned to Pobal data
pobaldf = pd.merge(pobaldf, EDdata[['WKT', 'NAME_TAG', 'AREA', 'LATITUDE', 'LONGITUDE', 'CO_NAME']], left_on = ['ED_Name', 'countyname'], right_on = ['NAME_TAG', 'CO_NAME'], how = 'left')

pobaldf[290:300]

pobaldf = pobaldf[pobaldf.ID06 > 0]

pobaldf.to_csv('Pobal_data.csv', encoding = 'UTF-8')


"""
Create a file of data that looks at secondary schools across the state.  
Google API is used to get geo coordinates to match to a file of schools 
taken from Department of education website
"""
import pandas as pd
#List of Secondary Schools - 723 schools listed

SecSch = r'https://www.education.ie/en/Publications/Statistics/Data-on-Individual-Schools/post-primary/post-primary-schools-2019-2020.xlsx'
Secdf = pd.read_excel(SecSch,  header = 0, sheet_name='School List', skipfooter = 1)
Secdf.head(4)
#Secdf.to_csv('secondary.csv', encoding = 'utf-8-sig')

#Get a full address to pass to google API

FullAddress = Secdf['Official School Name'] + ', ' + Secdf['Address 1']+ ', ' + Secdf['Address 2']
#key = ********************
schoolCoord, GeoData = GoGetter(FullAddress, key)

sc = pd.DataFrame(schoolCoord, columns = ['School', 'FullAddress', 'Latitude', 'Longitude'])
sc.to_csv("School_coord.csv")
pd.DataFrame(GeoData).to_csv("SchoolGeo.csv")

#sc = pd.read_csv("School_coord.csv")
#sc.head(4)
#Next we'll combine the co-ordinates information with the list of schools

Secdf['School'] = Secdf['Official School Name'] + ', ' + Secdf['Address 1']+ ', ' + Secdf['Address 2']
totalSecondary = pd.merge(Secdf, sc, on = 'School')
totalSecondary.to_csv("SecondarySchoolDataset.csv")
#totalSecondary = pd.read_csv("SecondarySchoolDataset.csv")



#List of Primary Schools - 3108 schools listed

PrimSch = r'https://www.education.ie/en/Publications/Statistics/Data-on-Individual-Schools/primary/primary-schools-2019-2020.xlsx'
Primdf = pd.read_excel(PrimSch,  header = 1, sheet_name='Mainstream Schools', skipfooter = 1)
Primdf.head(5)

#Again Geocode these:
#Get a full address to pass to google API

FullAddress = Primdf['Official Name'] + ', ' + Primdf['Address (Line 1)'] + ', ' + Primdf['Address (Line 2)']

#key = ********************
schoolCoordPrimary, GeoDataPrimary = GoGetter(FullAddress, key)

pc = pd.DataFrame(schoolCoordPrimary, columns = ['School', 'FullAddress', 'Latitude', 'Longitude'])
pc.to_csv("School_coordPrimary.csv")
pd.DataFrame(GeoDataPrimary).to_csv("SchoolGeoPrimary.csv")

#Next we'll combine the co-ordinates information with the list of schools

Primdf['School'] = Primdf['Official Name'] + ', ' + Primdf['Address (Line 1)']+ ', ' + Primdf['Address (Line 2)']
totalPrimary = pd.merge(Primdf, pc, on = 'School')
totalPrimary.to_csv("PrimarySchoolDataset.csv")

#totalPrimary = pd.read_csv("PrimarySchoolDataset.csv", index_col = False)


"""
Next we will obtain the information on best performing secondary schools -a subset of the Secondary schools dataset
Source is Time list of top 500 schools for college admission
"""


#Import PDF table of best schools


# pdf file object
# you can find find the pdf file with complete list in below link
file = r'C:\Users\alber\OneDrive\Documents\Postgrad diploma in data analytics\Database and Analytics Programming\Project\Assignment files\Data\4ae37f2231c180216b94a71160d8aaac.pdf'

BestSch = schPDF(file)


BestSch.to_csv('BestSchools.csv')







#Property price register 2019



PropPrice19 = r'https://data.smartdublin.ie/dataset/b0dd7d39-8eb5-4710-b46c-6a0db49e64af/resource/612b458e-95f6-46d6-8d73-9d06a6c772e3/download/ppr-2018-dublin.csv'
HPrice = pd.read_csv(PropPrice19, encoding = 'unicode_escape')
HPrice.head(2)







