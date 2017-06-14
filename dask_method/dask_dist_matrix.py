import dask as dd
from dask.distributed import Client
import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

remote_addr = ''
filepath = './example_geometries.csv'

# we need a reference complete geodataframe (this is not supported by Dask)
df = pd.read_csv(filepath)
geometries = gpd.GeoSeries(df['geometry'].map(lambda x: loads(x)))
gdf_ref = gpd.GeoDataFrame(data=df[['id', 'value']], crs={'init': 'epsg:32154'}, geometry=geometries)
gdf_ref.reset_index(drop=True, inplace=True)
print('Working with a GeoDataFrame of length {}.'.format(len(gdf_ref.index)))

# then we also need a dataframe representation of the same dataframe
gdf = dd.read_csv(filepath, parse_dates=[],
                  storage_options={'anon': True})
gdf = client.persist(gdf)

def summarize(row):
	# recast row geometry as such from string
	geom = loads(row['geometry'])
	buffered = geom.buffer(1000)

	# get a subset of the overall gdf
    intersected = gdf[gdf.intersects(buffer)]
    summed = intersected['value'].sum()
    row['sum'] = summed
    return row

client_db2 = Client(remote_addr)

