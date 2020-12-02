
#url = r"https://maps.googleapis.com/maps/api/geocode/json?address=" + item + r"&components = country:IE &key=" + key

"""
Function GoGetter
Take the full list of Addresses
and geocode these based on google maps geocoding API.
Create augmented list with latitude and longitude information

"""


#Set up the Google API to read the list


import urllib.request
import urllib
import requests
import json

import time



def GoGetter(list, APIkey = 'None'):
    start = time.time()
    # Access details from Google:
    test = []
    dataP = []

    try:
        for item in list:
            print(item)
            url = r"https://maps.googleapis.com/maps/api/geocode/json?address=" + item + r"&key=" + key

            coord = requests.get(url)
            coord = coord.json()

        # within each record the coord['results'][0] gives us dictionary we can get the information from
        # the coord['status'] value confirms if there is information
            if coord['status'] != 'ZERO_RESULTS':
                test1 = coord['results'][0]
                FormattedAddress = test1.get('formatted_address')
                lat = test1.get('geometry').get('location').get('lat')
                lng = test1.get('geometry').get('location').get('lng')

                dataP.append([item, FormattedAddress, lat, lng])
                test.append(coord['results'])
            else:
                dataP.append([item, ".", ".", "."])

            #Generates a csv file of results at intermediate stages in case of failure during run
            if (len(test) % 250 == 0) or (len(test) == len(list)):
                k1 = "Fulldata_"+str(len(test))+".csv"
                k2 = "Coordinates for schools_" + str(len(test)) + ".csv"
                pd.DataFrame(test).to_csv(k1)
                pd.DataFrame(dataP).to_csv(k2)

        return dataP, test

    except Exception as e:
        print("error encountered at item: ", item)
        print(e)
    else:
        print("Code executed successfully")
    finally:
        end = time.time()
        print(end - start)



""" 
Function schPDF:  uses PyPDF to extract data about best performing schools in Ireland
Output is saved to a pandas dataframe as the return object as well as creating a csv file
to root directory

Input is file that contains the information
"""

import PyPDF2 as pdf
import io
import re

#Create Regex function to clean data
def re_new(pat, new, s):
    schnames = (re.sub(pat, new, s))
    return schnames

def schPDF(file):

    #Read in pdf information using PyPDF2
    pdfFileObj = open(file, 'rb')
    pdfReader = pdf.PdfFileReader(pdfFileObj, strict = False)
    pageObj = pdfReader.getPage(0)
    schooldata = str(pageObj.extractText().encode('utf-8-sig'))

    #start = schooldata.find('Rank')


    #strip out incorrect line breaks then a lot of regex to get data in a usable format
#    schooldata2 = schooldata.rstrip("\n")

    schooldata2 = re_new("\n", " ", schooldata)
    schnames = re_new( r"(?=[0-9][A-Z][a-z][a-z])", "\n ", schooldata2[5:-50])
    schnames = re_new(r"([0-9][,][0-9][0-9][0-9])", "1k+", schnames)
    schnames = re_new( r"(?=O'C)", "\n ", schnames)
    schnames = re_new( r"(?=[0-9][\*][A-Z][a-z][a-z])", "\n ", schnames)
    schnames = re_new( r"(?=[0-9][S][t])", "\n ", schnames)
    schnames = re_new( r"(?=[0-9][D][e][' '])", "\n ", schnames)
    schnames = re_new( r"(?=[0-9][F][C])", "\n ", schnames)
    schnames = re_new( r"(?=[0-9][C][B])", "\n ", schnames)
    schnames = re_new( r"((SD|CK|ND|M|U|L|M)[GBM])", ",", schnames)
    schnames = re_new(r"5CBC", "CBC", schnames)

    schnames = re_new( r"(?=[-][A-Z][a-z][a-z])", "\n ", schnames)
    schnames = re_new( r"(?=[Œ][A-Z \*])", "\n ", schnames)
    schnames = re_new( r"(?=[Œ][,][A-Z][' '][A-Z])", "\n ", schnames)
    schnames = re_new( r"DEFINIT", "\n ", schnames)
    schnames = re_new( r"\n -Ardan", "-Ardan", schnames)
    schnames = re_new( r"\n -Suir", "-Suir", schnames)
    schnames = re_new( r"\n -Shannon", "-Shannon", schnames)

    schnames = re_new(r"\\xc3\\xa9", "é", schnames)
    schnames = re_new(r"\\xc3\\xa1", "á", schnames)
    schnames = re_new(r"\\xc3\\x8d", "Í", schnames)


    schlist =schnames.split("\n")


    sub1 = "GUIDE TO THE TOP"
    sub2 = "RankPrevious"
    sub3 = "These are the top 400 secondary schools"

    for text in schlist:
        counter = 0
        if (sub1 in text) or (sub2 in text) or (sub3 in text):
            schlist.pop(schlist.index(text))
            counter += 1
        if counter > 500:
            break


    #verification file
    with open('checker.txt', 'w') as f:
        for item in schlist:
            f.write("%s\n" % item)

    ad1 = []
    ad2 = []
    ad3 = []
    adrank = []
    count = 0
    for s in schlist:
        p1 = (s.find(','))
        p2 = (s.find(',', p1+1))
        p3 = (s.find(',', p2+1))
        A1 = (s[1: p1])
        A2 = (s[p1 + 1: p2])
        A3 = (s[p2 + 1:p3])
        if A1 == "" and A2 == "":
            continue

        count += 1
        ad1.append(A1)
        ad2.append(A2)
        if A3[:1].isdigit():
            ad3.append(" ")
        else:
            ad3.append(A3)

        adrank.append(count)
        if count == 500:
            break

    #Some additional Regexs now needed to clean
    for i in range(len(ad1)-1):
        ad1[i] = re_new(r"^[0-9]", "", ad1[i])
        ad1[i] = re_new(r"\*", "", ad1[i])
        ad3[i] = re_new("\n", "", ad3[i])

    #Drop data into a dataframe
    BestSch = pd.DataFrame(list(zip(adrank, ad1, ad2, ad3)), columns = ['Ranking', 'Name', 'Address1', 'Address2'])

#    BestSch.to_csv("testout.csv")
    return BestSch





"""
Functions to get distances between pairs of points.

First function applies Haversine equation to get distance between two points

Second sets this into a function that allows you to inout two dataframes and get the minimum pairwise distance 
between a point in dataset 1 and every point in dataset 2 
"""

import math

def distCalc(lat1, long1, lat2, long2):
    """
    Code calculates distance between 2 lat/long coordinates based on the use of the Haversine formula
    """
    REarth = 6371                   #Radius of the earth
    Rlat1 = math.radians(lat1)
    Rlong1 = math.radians(long1)
    Rlat2 = math.radians(lat2)
    Rlong2 = math.radians(long2)

    latdiff = Rlat1 - Rlat2
    longdiff = Rlong1 - Rlong2
    #Apply Haversine formula
    dist = 2 * REarth * math.asin(math.sqrt(math.sin(latdiff/2) ** 2 + math.cos(Rlat1) * math.cos(Rlat2) * math.sin(longdiff / 2) ** 2))
    return dist


dist = distCalc(Latitude1, Longitude1, Latitude2, Longitude2 )

"""
Create loop to allow two dataframes to be run together
Check performance during run
"""

def LoopyMinD(data1, lat1, long1, data2, lat2, long2):
    start = time.time()
    mdist = []
    for index, row1 in data1.iterrows():
        dval = []
        for index, row2 in data2.iterrows():
            dist = distCalc(float(row1[lat1]), float(row1[long1]), float(row2[lat2]), float(row2[long2]))
            dval.append(dist)
        mx = max(dval)
        mdist.append(mx)

    end = time.time()
    print("time taken was:", float(end - start), "  Records per second was:", float((len(data1)*len(data2))/(end - start)))
    return mdist


#example
#k = LoopyMinD(totalSecondary, 'Latitude', 'Longitude', totalSecondary, 'Latitude', 'Longitude')








