import os, sys, csv, time
from dask import dataframe as dd
import geopandas as gpd
import pandas as pd
from shapely.wkt import loads


# log/monitor performance
start_time = time.time()
def log(message):
    global start_time
    print('{:,.3f}s: {}'.format(time.time()-start_time, message))
    start_time = time.time()


# load in example geometry data
filepath = './example_geometries.csv'
init_df = pd.read_csv(filepath)

# determine if we should use Dask multiprocessing with DataFrames
USE_MULTIPROCESSING = int(os.getenv('mproc', 0))
if USE_MULTIPROCESSING == 1:
    dask.set_options(get=dask.multiprocessing.get)

length_options = [500, 1000, 5000, 10000, 20000]
for subset_length in length_options:
    # take a subset of the dataframe per increment
    print('\nRunning on subset length: ' + str(subset_length) + ' rows')
    df = init_df.head(subset_length)

    # =====================
    # First steps: prepare the GeoDataFrame
    # =====================

    # create a geometry geoseries from the geometry column's wkt string
    crs = {'init': 'epsg:32154'}
    geometries = gpd.GeoSeries(df['geometry'].map(lambda x: loads(x)))

    # take a subset of the pandas df and convert to a gdf via geopandas
    gdf = gpd.GeoDataFrame(data=df[['id', 'value']], crs=crs, geometry=geometries)

    # ...now index the new gdf (id is integer dtype)
    gdf = gdf.set_index('id')

    # we'll do an aggregation based off of an arbitrary buffer distance
    gdf['buffer'] = gdf['geometry'].buffer(500)

    print('\tFinished initial set up steps. Running Dask operation...')

    # we can convert from a geodataframe to dask
    dd_gdf = dd.from_pandas(gdf, npartitions=3)

    # =====================
    # Next: define and apply the lambda function
    # =====================

    # update geometries global to preserve same index
    geometries = gdf.geometry
    sidx = gdf.sindex

    # we want to update this column via summarize operation
    dd_gdf['sum'] = 0.0

    # this is the summary/aggregation process we'll perform on each row
    def summarize(row, sidx, dgdf, usecompute):
        # we use dgdf from global scope
        buffer = row['buffer']

        # utilizing precalc'd spatial indices
        possible_matches_index = list(sidx.intersection(buffer.bounds))
        possible_matches = dgdf.iloc[possible_matches_index]

        where_intersects = possible_matches.intersects(buffer)
        dgdf['intersects'] = where_intersects
        precise_matches = dgdf[dgdf['intersects'] == True]
        
        summed = precise_matches['value'].sum()
        if usecompute is True:
            pass
            # summed = summed.compute()    
        row['sum'] = summed
        
        return row

    meta = dd_gdf.loc[5758].compute()
    usecompute = True
    dd_sums_op = dd_gdf.apply(lambda row: summarize(row, sidx, gdf.copy(), usecompute), axis=1, meta=meta)
    dd_sums = dd_sums_op.compute()


    log('Finished Dask run.')
    # print(dd_sums[['value', 'sum']].head())

    print('\tRunning original operation...')
    usecompute = False
    gdf.apply(lambda row: summarize(row, sidx, gdf.copy(), usecompute), axis=1)
    log('Finished normal method run.')
