# Notes on Dask Performance

Results indicate no significant improvement when using Dask Dataframe over Pandas DataFrame. When doing a 500 unit buffer, results were as follows:

```
Running on subset length: 500 rows
	Finished initial set up steps. Running Dask operation...
4.503s: Finished Dask run.
	Running original operation...
4.092s: Finished normal method run.

Running on subset length: 1000 rows
	Finished initial set up steps. Running Dask operation...
9.508s: Finished Dask run.
	Running original operation...
10.793s: Finished normal method run.

Running on subset length: 5000 rows
	Finished initial set up steps. Running Dask operation...
93.965s: Finished Dask run.
	Running original operation...
115.837s: Finished normal method run.

Running on subset length: 10000 rows
	Finished initial set up steps. Running Dask operation...
343.601s: Finished Dask run.
	Running original operation...
444.108s: Finished normal method run.
```

Meanwhile, when using a 5000 unit buffer (resulting in large subset DataFrames for each buffer function applied per row in the DataFrame), Dask appeared to deliver ~1.8x performance improvement. You can see the logs from that, below. I should note that the above outputs were the result of running in a Docker container on DB2 whereas the below outputs are the results of running in a virtual environment on my Macbook Pro (2015), as well as using the 5000 unit buffer over the 500 unit buffer above (a 10x increase):

```
Running on subset length: 500 rows
    Finished initial set up steps. Running Dask operation...
3.954s: Finished Dask run.
    Running original operation...
4.711s: Finished normal method run.

Running on subset length: 1000 rows
    Finished initial set up steps. Running Dask operation...
9.500s: Finished Dask run.
    Running original operation...
13.339s: Finished normal method run.

Running on subset length: 5000 rows
    Finished initial set up steps. Running Dask operation...
102.168s: Finished Dask run.
    Running original operation...
160.297s: Finished normal method run.

Running on subset length: 10000 rows
    Finished initial set up steps. Running Dask operation...
470.869s: Finished Dask run.
    Running original operation...
723.688s: Finished normal method run.
```
