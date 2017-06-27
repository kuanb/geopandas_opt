import logging
from dask.distributed import Client
from dask.dataframe.utils import make_meta
import dask.dataframe as dd
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.wkt import loads
from timeit import timeit


filepath = './example_geometries.csv'


connection_loc = '34.123.123.158:8786' # some dask scheduler (example)
client = Client(connection_loc)
print(client)

# set this so that you have ~2-4 partitions per worker
# e.g. 60 machines each with 4 workers should have ~500 partitions
nparts = 100
head_count = None # set to a # if you want to just do a subset of the total for faster ops

# get original csv as pandas dataframe
pdf = pd.read_csv(filepath)[['id', 'geometry']]
pdf = pdf.reset_index(drop=True).sample(frac=1)


# trim if desired
if head is not None:
    pdf = pdf.head(head_count)
print('Working with a dataframe of length {}.'.format(len(pdf)))

# clean the ids column to make a unique id lookup column
orig_ids_base = pdf['id'].values

pdf = pdf.drop('id', axis=1)
pdf['id'] = pdf.index

# now convert the new index into an array
new_ids_base = np.array(pdf['id'].values)
new_ids_indexed = np.argsort(new_ids_base)
new_ids = new_ids_base[new_ids_indexed]

# sort the original one the same way
orig_ids = orig_ids_base[new_ids_indexed]

# we need some generic column to perform the many-to-many join on
pdf = pdf.assign(tmp_key=0)

# then convert into a dask dataframe
ddf = dd.from_pandas(pdf.copy(), name='ddf', npartitions=nparts)


# merge the pandas and dask dataframes
tall_list = (dd.merge(ddf, pdf,
                     on='tmp_key',
                     suffixes=('_from', '_to'),
                     npartitions=nparts)
               .drop('tmp_key', axis=1))

def calc_distances(grouped_result):
    # we just need one geometry from the left side because
    first_row = grouped_result.iloc[0]
    from_geom = first_row['geometry_from']

    # try to log somehow...
    msg = '...analyzing row {}'.format(first_row['id_from'])
    logging.warning(msg)
    print(msg)

    # and then convert to a GeoSeries
    to_geoms = gpd.GeoSeries(grouped_result['geometry_to'])

    # get an array of distances from the GeoSeries comparison
    distances = to_geoms.distance(from_geom)
    return distances.values


distances = (tall_list
                .rename(columns={'id_from': 'id'})
                .groupby('id')
                .apply(calc_distances, meta=pd.Series()))


print('Beginning computation...')
computed = distances.compute()
print('Computation completed.')

# do something/explore the results, which is a Pandas Series
print(computed)
