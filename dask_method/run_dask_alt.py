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

# we can convert from a geodataframe to dask
dd_gdf = dd.from_pandas(gdf, npartitions=3)



# =====================
# Next: define and apply the lambda function
# =====================

# we want to update this column via summarize operation
dd_gdf['sum'] = 0.0

# this is the summary process we'll perform on each row
def summarize(row, gdf):
    buffer = row['geometry'].buffer(1000)
    precise_matches = gdf[gdf.intersects(buffer)]
    summed = precise_matches['value'].sum()
    row['sum'] = summed
    return row

meta = dd_gdf.loc[5758].compute()


# =====================
# Alternative 1 (comment out to not use)
# =====================

# Note: want to just run once? Just run the summarize on a single row
dd_sums_op = dd_gdf.apply(lambda row: summarize(row, gdf), axis=1, meta=meta)


# =====================
# Alternative 2 (comment out to not use)
# =====================

resulting_sums = []
for row in dd_gdf.iterrows():
    row_sum = summarize(row, gdf)
    resulting_sums.append((row['id'], row_sum))



# actually execute either of the above
dd_sums = dd_sums_op.compute()