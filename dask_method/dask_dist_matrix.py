import logging
from dask.distributed import Client
from dask.dataframe.utils import make_meta
import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.wkt import loads
from timeit import timeit

# change for different ref location for geometries csv
filepath = './example_geometries.csv'

# turn distributed use on/off
use_distributed = False
connection_loc = '34.123.123.123:8786' # some dask scheduler (example)

if use_distributed:
    client = Client(connection_loc)
    print(client)

# set this so that you have ~2-4 partitions per worker
# e.g. 60 machines each with 4 workers should have ~500 partitions
nparts = 1
head_count = 100 # set to a # if you want to just do a subset of the total for faster ops, else None

# get original csv as pandas dataframe
pdf = pd.read_csv(filepath)[['id', 'geometry']].reset_index(drop=True)

# convert to geopandas dataframe
geometries = gpd.GeoSeries(pdf['geometry'].map(lambda x: loads(x)))
crs = {'init': 'epsg:32154'},
gdf = gpd.GeoDataFrame(data=pdf[['id']], crs=crs, geometry=geometries)

# trim if desired
if head_count is not None:
    gdf = gdf.head(head_count)
print('Working with a dataframe of length {}.'.format(len(gdf)))

# clean the ids column
gdf = gdf.drop('id', axis=1)
gdf['id'] = gdf.index
gdf['id'] = gdf['id'].astype(int)

# we need some generic column to perform the many-to-many join on
gdf = gdf.assign(tmp_key=0)

# then convert into a dask dataframe
ddf = dd.from_pandas(gdf.copy(), name='ddf', npartitions=nparts)

# merge the pandas and dask dataframes
tall_list = (dd.merge(ddf, gdf,
                     on='tmp_key',
                     suffixes=('_from', '_to'),
                     npartitions=nparts)
               .drop('tmp_key', axis=1))


def calc_distances(grouped_result):
    # we just need one geometry from the left side because
    first_row = grouped_result.iloc[0]
    from_geom = first_row['geometry_from'] # a shapely object

    # and then convert to a GeoSeries
    to_geoms = gpd.GeoSeries(grouped_result['geometry_to'])

    # get an array of distances from the GeoSeries comparison
    distances = to_geoms.distance(from_geom)
    return distances.values


distances = (tall_list
                .groupby('id_from')
                .apply(calc_distances, meta=pd.Series()))


print('Beginning computation...')
computed = distances.compute()
print('Computation completed.')

# do something/explore the results, which is a Pandas Series
print(computed)

