#################### Lat-Long Distance Calculator ####################
#
# Function: Calculates the line-of-sight distance between two points defined by latitude and longitude.
#
# Author: Chris Signorelli
# Student: 19181027
# Date: 10th Dec 2020
# Assignment: NCI DAP 2020 terminal assessment.
# Group: R
#
# Uses Lambert's formula.
# See also Bowring's method for short lines.
# https://en.wikipedia.org/wiki/Geographical_distance
#
# Usage: df['Lambert-Distance'] = df.apply(Lambert_dist_calc, axis=1)
# Assumes cols exist in df for 'P-Lat', 'P-Long', 'G-Lat', 'G-Long'. Change function to suit different df col names.
#
######################################################################
import math

def Lambert_dist_calc(row):
    phi_1 = math.radians(row['P-Lat']) # latitude of point 1.
    phi_2 = math.radians(row['G-Lat']) # latitude of point 2.
    d_phi = phi_1 - phi_2

    lambda_1 = math.radians(row['P-Long']) # longitude of point 1.
    lambda_2 = math.radians(row['G-Long']) # longitude of point 2.
    d_lambda = lambda_1 - lambda_2

    # Central angle in radians between two points on a sphere using the Great-circle distance method.
    # https://en.wikipedia.org/wiki/Great-circle_distance
    sigma = math.acos(math.sin(phi_1)*math.sin(phi_2) + \
                      math.cos(phi_1)*math.cos(phi_2)*math.cos(d_lambda))

    # Flattening factor.
    f = 1/298.257223563

    # https://en.wikipedia.org/wiki/Latitude#Parametric_(or_reduced)_latitude
    beta_1 = math.atan((1 - f)*math.tan(phi_1))
    beta_2 = math.atan((1 - f)*math.tan(phi_2))

    P = (beta_1 + beta_2)/2
    Q = (beta_1 - beta_2)/2

    X = (sigma - math.sin(sigma)) * (math.sin(P))**2 * (math.cos(Q))**2 / (math.cos(sigma/2))**2
    Y = (sigma + math.sin(sigma)) * (math.cos(P))**2 * (math.sin(Q))**2 / (math.sin(sigma/2))**2

    # radius of the earth (km)
    a = 6371.009
    dist = a*(sigma - f/2*(X + Y))
    
    return dist