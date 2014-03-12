# SAGA-Pilot Baseline Benchmarks

The purpose of these benchmarks is to benchmark SAGA-Pilot with different 
ComputePilot sizes (# cores) and different numbers of ComputeUnits. 

The workload is always zero (`/bin/sleep 0`). The number of input files 
per CU can be controlled, so can the file size. The command-line parameters 
are:

* `--mongodb-url`: The URL of the MongoDB instance to use.
* `--resource`: The name of the resource on which to launch the ComputePilot.
* `--pilot-size`: The number of cores to allocate for the ComputePilot.
* `--pilot-runtime`: The runtime (in minutes) of the ComputePilot.
* `--number-of-cus`
* `--input-files-per-cu`
* `--input-file-size`

For example, to execute 1024 CUs with two 1 MB input files on a 128-core 
pilot on FutureGrid's 'sierra' cluster, run this:

```
python --mongodb-url=$SAGAPILOT_DBURL --resource=sierra.futuregrid.org \
       --pilot-size=128 --pilot-runtime-30 --number-of-cus=1024 \
       --input-files-per-cu=2 --input-file-size=1024
```

>> You can set SAGAPILOT_VERBOSE=info to get more information during execution. 

For longer running benchmarks, you can provide the `--detach` flag. This 
causes the tool to exit as soon as all CUs are submitted. If `--detached` is 
not set (default), the tool waits until all ComputeUnits have finished 
execution.


The tool doesn't create any output. However, all relevant data are stored 
in MongoDB and can be accessed using the session ID returned by the tool. 
You can use the `sagapilot-timeline` tool to extract all relevant information,
but you can also write your own, custom evaluation script.

```

``` 

The benchmarking tool is design to execute a single experiment. If you want to
run a  series of experiments with varying parameters, you can easily wrap the
benchmark into a shell script, like this:

```

```


