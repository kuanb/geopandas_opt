import os, sys, csv, time
import dask
from dask import dataframe as dd
import geopandas as gpd
from random import random
import pandas as pd
from shapely.wkt import loads

# NOTE: Pseudocode version to demonstrate more generic example

# load in example geometry data
filepath = './example_geometries.csv'
init_df = pd.read_csv(filepath)

# take a subset of the dataframe per increment
subset_length = 500
df = init_df.head(subset_length)



# =====================
# First steps: prepare the GeoDataFrame
# =====================

# create a geometry geoseries from the geometry column's wkt string
geometries = gpd.GeoSeries(df['geometry'].map(lambda x: loads(x)))

# take a subset of the pandas df and convert to a gdf via geopandas
gdf = gpd.GeoDataFrame(data=df[['id', 'value']], crs={'init': 'epsg:32154'}, geometry=geometries)
gdf = gdf.set_index('id')

# we'll do an aggregation based off of an arbitrary buffer distance
gdf['buffer'] = gdf['geometry'].buffer(5000)

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
def summarize(row, gdf):
    # do a random buffer
    # if this does not work, it's because Dask didn't preserve geometry 
    # so just need to recast shapely.wkt's loads() func on row.geometry

    buffer = row['geometry'].buffer(random() * 1000)
    precise_matches = gdf[gdf.intersects(buffer)]
    summed = precise_matches['value'].sum()
    row['sum'] = summed
    return row

meta = dd_gdf.loc[5758].compute()

# Note: want to just run once? Just run the summarize on a single row
dd_sums_op = dd_gdf.apply(lambda row: summarize(row, gdf.copy()), axis=1, meta=meta)
dd_sums = dd_sums_op.compute()
