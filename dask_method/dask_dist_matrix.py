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
    intersected = gdf_ref[gdf_ref.intersects(buffer)]
    row['sum'] = intersected['value'].sum()
    return row


# initiatlize the client connection for dask distributed
client_db2 = Client(remote_addr)


# optional trim step
gdf_ref = gdf_ref.head(500)
gdf = gdf.head(500)

# two paths, first is old fashioned way
print('calculating normal way')
gdf_ref_2 = gdf_ref.copy()
gdf_ref_2.apply(summarize, axis=1)
print(gdf_ref_2.sum.values)

# second is to use client and dask distributed
print('calculating distributed way')
gdf.apply(summarize, axis=1)
print(gdf.sum.values)
