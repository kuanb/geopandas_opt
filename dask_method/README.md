# Notes on Dask Performance

## Results from initial 5000 unit (meter) buffer apply
Results indicate no significant improvement when using Dask Dataframe over Pandas DataFrame. When doing a 5000 unit buffer, results were as follows below. Results below were the output when running the operation on DB2.
```
500 row DataFrame
4.503s: with  Dask
4.092s: w/out Dask

1000 row DataFrame
9.508s:  with  Dask
10.793s: w/out Dask

5000 row DataFrame
93.965s:  with  Dask
115.837s: w/out Dask

10000 row DataFrame
343.601s: with  Dask
444.108s: w/out Dask

20000 row DataFrame
(did not run)
```

Meanwhile, when using a 5000 unit buffer locally, Dask appeared to deliver ~1.8x performance improvement. You can see the logs from that, below. While the above outputs were the result of running in a Docker container on DB2; the below outputs are the results of running in a virtual environment on my Macbook Pro (2015):
```
500 row DataFrame
3.954s: with  Dask
4.711s: w/out Dask

1000 row DataFrame
9.500s:  with  Dask
13.339s: w/out Dask

5000 row DataFrame
102.168s: with  Dask
160.297s: w/out Dask

10000 row DataFrame
470.869s: with  Dask
723.688s: w/out Dask

20000 row DataFrame
(did not run)
```

Conclusion for this section: It looks like the Dask DataFrame start to deliver performance/time savings when the subset dataframes produced through the buffer intersection get larger, which makes total sense. In the below section, we can see how a 10x reduction in buffer measure reduces the performance gains of the Dask DataFrame, causing it to perform less well when compared with the standard Pandas DataFrame.

## Results from initial 500 unit (meter) buffer apply
Note: All subsequent benchmarks are performed in a Docker container (config via Dockerfile in root directory of this repo) on DB2:

Below, we have results when *not* using any special configurations - just using a Dask DataFrame vs. a Pandas DataFrame.
```
500 row DataFrame
3.730s: with  Dask
2.817s: w/out Dask

1000 row DataFrame
6.836s: with  Dask
5.408s: w/out Dask

5000 row DataFrame
38.385s: with  Dask
33.903s: w/out Dask

10000 row DataFrame
88.179s: with  Dask
78.036s: w/out Dask

20000 row DataFrame
214.678s: with  Dask
201.073s: w/out Dask
```

Now we opt to turn on the global Dask `multiprocessing` config. From the execution notes in the README docs:
```
In some cases you may experience speedups by switching to the multiprocessing or distributed scheduler.
>>> dask.set_options(get=dask.multiprocessing.get)
```

Results are below in this case. Using multiprocessing does deliver substantive performance increases - and one that appears to deliver more bang the larger the dataset (again, expected).
```
500 row DataFrame
2.062s: with  Dask
2.726s: w/out Dask

1000 row DataFrame
3.313s: with  Dask
5.539s: w/out Dask

5000 row DataFrame
16.422s: with  Dask
31.749s: w/out Dask

10000 row DataFrame
33.218s: with  Dask
76.531s: w/out Dask

20000 row DataFrame
69.867s:  with  w/Dask
198.125s: w/out Dask
```

### Looking furher at multiprocessing
Looks like multiprocessing is delivering some utility. Now, let's see what it looks like when dealing with larger subset dataframes. First, let's bump the buffer from 500 to 5000 units and see what the comparative performance is between a Dask DataFrame utilizing multiprocessing and a standard Pandas DataFrame.
```
500 row DataFrame
2.052s: with Dask
4.066s: w/out Dask

1000 row DataFrame
3.330s: with Dask
10.589s: w/out Dask

5000 row DataFrame
15.977s:  with Dask
115.935s: w/out Dask

10000 row DataFrame
32.544s: with Dask
444.108s: w/out Dask

20000 row DataFrame
66.208s: with Dask
(did not run without Dask)
```
