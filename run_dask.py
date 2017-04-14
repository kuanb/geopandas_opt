import sys, csv, time
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads

# log/monitor performance
start_time = time.time()
def log(message):
    global start_time
    print('{:,.3f}s: {}'.format(time.time()-start_time, message))
    start_time = time.time()

print('\n\n###################')
print('Running without Dask first.')
print('###################\n')

# load in example geometry data
filepath = './example_geometries.csv'
df = pd.read_csv(filepath)
log('loaded csv')

# default crs projection for example geometries
crs = {'init': 'epsg:32154'}

# create a geometry geoseries from the geometry column's wkt string
geometry = gpd.GeoSeries(df['geometry'].map(lambda x: loads(x)))
log('created geometry')

# take a subset of the pandas df and convert to a gdf via geopandas
gdf = gpd.GeoDataFrame(data=df[['id', 'value']], crs=crs, geometry=geometry)

# ...now index the new gdf
gdf = gdf.set_index('id')
log('converted df to gdf')

# create a spatial index to be used in a lambda/apply loop
sidx = gdf.sindex
log('created spatial index')

# we'll do an aggregation based off of an arbitrary buffer distance
gdf['buffer'] = gdf['geometry'].buffer(500)
log('buffered geometries')

# this is the summary/aggregation process we'll perform on each row
def summarize(buffer):
	# access sidx, gdf from global scope
    possible_matches_index = list(sidx.intersection(buffer.bounds))
    possible_matches = gdf.iloc[possible_matches_index]
    precise_matches = possible_matches[possible_matches.intersects(buffer)]
    return precise_matches['value'].sum()

# execute the summarize operation on each row of the 
# sums = gdf['buffer'].map(summarize)
# log('calculated value sums')

# print(sums.head())

print('\n\n###################')
print('Now utilizing Dask.')
print('###################\n')

# import relevant dask lib
from dask import dataframe as dd

# can we conver from a geodataframe to dask?
dd_gdf = dd.from_pandas(gdf, npartitions=3)
log('converted gdf to dask dataframe')

print(dd_gdf.head())

print(dd_gdf.head().bounds)
