# Improving spatial operations
In this doc I will be using as reference the Madison UF project and the S1 scenario. The base table is ~30k rows. All that is relevant is that there is a geometry column and any number of additional columns containing floats. Aggregations such as sums, means, etc. are performed on these columns after a spatial operation has been performed to return a subset of the data frame. See the example geometries `csv` as well as the `run.py` (Python3) file to see a stripped down example dataset and operation logic.

## Current situation
All geospatial operations in Geopandas are costly, this includes but is not limited to: unary unions, intersections, within booleans, distance calculations. The underlying functions that Geopandas is calling are those from GEOS, which is itself a C++ port of the Java Topo Suite. All of this is to say, it is fast and reliable. Parts of PostGIS work with GEOS, too.

So what is causing the slowdown? I can think of two possibilities right now:

1. The bottleneck here is Python itself.
2. There are optimizations that should be implemented that I've failed to identify.

## Example
In the included example Pythons script, I load in a dataset containing geometries and a `value` column (named “value”). The goal is to generate a sum of all values within a certain buffer (in this case, 500 units) of the reference geometry. This operation is to be performed on each row’s geometry.

Presently, the operation performs two tasks intended to speed up the operation. First, it creates a spatial index from the GeoDataFrame based on its `geometry` column. Second, it preprocesses a buffer column.

The `summarize` operation applied to each row performs a comparative buffer (buffering the reference geometry and getting all intersections to that, and then doing the inverse and buffering the other geometries and seeing if the normal geometry intersects in that situation). This methodology replicates the same for `ST_DWithin` from PostGIS (reference in their repo: https://github.com/postgis/postgis/blob/c0998a4e638048196a6abfab1504eca6b1a1462f/postgis/geography.sql.in#L550).

### Results
Running `run.py` will result in the following output (time in seconds before the colon):
```
$ python3 run.py
0.244s: loaded in reference csv

1.787s: updated geom column to MultiPolygon type

0.008s: converted dataframe to geodataframe

3.345s: created spatial indices

5.472s: Created buffered geometry column

563.553s: completed summarize() application on geodataframe

          value
0  10319.032630
1     32.595417
2      3.991619
3     14.131560
4     32.533223
```
Note: Times will vary on each run through, naturally.

The `apply` operation takes a long time; about 9.4 minutes. That's nearly 0.2 seconds per row, which seems terribly slow. You can imagine how a series of spatial operations can quickly amount to cumbersome execution times.

## Question
What, if any, additional optimizations could be used to make geometric operations faster in Python? For example, I've found additional prepartion steps that could potentially be used, such as `shapely.prepared()` (and asked about them on SO: http://stackoverflow.com/questions/42232601/r-tree-v-shapely-prepared-when-comparing-geometries), but from what I can tell they provide no substantive improvements over what is already achieved via the spatial index.
