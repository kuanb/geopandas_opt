from dask.distributed import Client
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from shapely.wkt import loads

filepath = './example_geometries.csv'
buffer_dist = 1000

client_db2 = Client('tcp://10.0.0.50:8786')
print(client_db2)


# we need a reference complete geodataframe (this is not supported by Dask)
df = pd.read_csv(filepath)
geometries = gpd.GeoSeries(df['geometry'].map(lambda x: loads(x)))
gdf_ref = gpd.GeoDataFrame(data=df[['id', 'value']], crs={'init': 'epsg:32154'}, geometry=geometries)
gdf_ref.reset_index(drop=True, inplace=True)
gdf_ref['sum'] = 0.0
print('Working with a GeoDataFrame of length {}.'.format(len(gdf_ref.index)))


# then we also need a dataframe representation of the same dataframe
gdf = dd.read_csv(filepath)
gdf = client_db2.persist(gdf)


# function to summarize all geometries withiin a n-meter buffer
def summarize(row):
    # recast row geometry as such from string
    geom = row['geometry']
    buffered = geom.buffer(buffer_dist)

    # get intersections
    precise_matches = gdf_ref.geometry.intersects(buffered)
    
    return gdf_ref[precise_matches]['value'].sum()


def run_summarize(id):
    print('processing row id {}'.format(id))
    row = gdf_ref[gdf_ref['id'] == id].squeeze()
    return summarize(row)


print('running summarize on all rows')
gdf_ids = gdf_ref['id'].values
sums = client_db2.map(run_summarize, gdf_ids)
fin_sums = client_db2.gather(sums)
print('finished; sum results below')
print(fin_sums)