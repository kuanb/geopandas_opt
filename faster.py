import sys, csv, time
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads

# monitor performance
start_time = time.time()
def log(message):
    global start_time
    print('{:,.3f}s: {}'.format(time.time()-start_time, message))
    start_time = time.time()

filepath = './example_geometries.csv'
crs = {'init': 'epsg:32154'}
df = pd.read_csv(filepath)
log('loaded csv')

# create a geometry geoseries from the wkt
geometry = gpd.GeoSeries(df['geometry'].map(lambda x: loads(x)))
log('created geometry')

gdf = gpd.GeoDataFrame(data=df[['id', 'value']], crs=crs, geometry=geometry)
gdf = gdf.set_index('id')
log('converted df to gdf')

sidx = gdf.sindex
log('created spatial index')

# projection is in meters, buffer itself is unit agnostic
gdf['buffer'] = gdf['geometry'].buffer(500)
log('buffered geometries')

def summarize(buffer):
    possible_matches_index = list(sidx.intersection(buffer.bounds))
    possible_matches = gdf.iloc[possible_matches_index]
    precise_matches = possible_matches[possible_matches.intersects(buffer)]
    return precise_matches['value'].sum()

sums = gdf['buffer'].map(summarize)
sums.name = 'value sum'
log('calculated value sums')

print(sums.head())
