
Please provide all error messaages and the output of the following commands:

```
$ python -V
$ radical-stack
```

If you can reproduce the problem, it would greatly simplify debugging if you can provide a minimal code example which we can use to repoduce the problem too.

On **most** tickets we will ask you to provide additional information, most often log files. You can obtain detailed logs by rerunning your code after setting the following environment variables:

```
$ export RADICAL_VERBOSE=DEBUG
$ export RADICAL_LOG_TGT=r.log
```

This will create the logfile r.log. Additional logfiles will be located in your correct directory, in a subdirectory named after your session ID.

Log files are also created on the (remote) host you are targetting with your run. In order to fetch log files from target hosts, please run:

```
$ radical-pilot-fetch-logfile <sid>
```

where `sid` is again the session ID in question.
