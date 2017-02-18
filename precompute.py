# import data
import sys, csv

# geospatial operations
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads

# monitor performance
import time
start = time.time()
def log(string):
	global start
	print(str(round(time.time() - start, 3)) + 's: ' + string + '\n')
	start = time.time()


"""
step 1: loading in the data and formatting it
"""
filename = 'example_geometries.csv'
data = pd.read_csv(filename)
log('loaded in reference csv')

# update column to a geoseries type
data['geometry'] = data['geometry'].apply(lambda x: loads(x))
data['geometry'] = gpd.GeoSeries(data['geometry'])
log('updated geom column to MultiPolygon type')

gdf = gpd.GeoDataFrame(data[['id', 'value']], 
							crs={'init': 'epsg:32154'}, 
							geometry=data['geometry'])
log('converted dataframe to geodataframe')


"""
step 2: set and sort index
"""
gdf = gdf.set_index(['id']).sort_index()
log('created index based on id attribute and sorted')


"""
step 3: create distance matrix base
"""
dm_base = dict()
vals = gdf.index.values
vals_len = len(vals)
for i in vals:
	dm_base[i] = pd.Series([None] * vals_len)
print(dm_base.head())

raise Stop()


"""
step 2: create a spatial index and buffered geometry
"""
s_index = gdf['geometry'].sindex
log('created spatial indices')

# projection is in meters, buffer itself is unit agnostic
gdf_buff = gdf.copy().envelope.buffer(500)
log('Created buffered geometry column')


"""
step 3: run apply to iterate over all rows with spatial operation
"""
def summarize(row):
	global gdf
	global gdf_buff
	global s_index

	# extract the row geometry and create a buffer of it
	geom = row['geometry']
	geom_buff = geom.buffer(500)

	# first, use the r-tree to drop the definite outliers
	possible_indx = sorted(list(s_index.intersection(geom_buff.bounds)))
	buff_possible = gdf_buff.iloc[possible_indx]
	base_possible = gdf.iloc[possible_indx]

	# get only rows results that are within the r-tree bounds
	potential = base_possible[base_possible.intersects(geom_buff) & buff_possible.intersects(geom)]
	
	# from within subset, return rows that are absolutely within buffer
	abs_match = potential[potential.distance(geom) < 500]
	
	# return as new series
	return pd.Series([abs_match['value'].sum()], index=['value'])

summed = gdf.apply(lambda row: summarize(row), axis=1)
log('completed summarize() application on geodataframe')

print(summed.head())
