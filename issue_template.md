
Please provide all error messaages and the output of the following commands:

```
$ python -V
$ radical-stack
```

If you can reproduce the problem, it would greatly help if you can provide
a minimal code example which we can use to repoduce it, too - that will greatly
simplify debugging.

On **most** tickets we will ask you to provide additional information, such as log
files.  If you can reproduce the problem, 

We will likely ask for more information, and specifically for log files.  You
can obtain details logs with:

```
$ export RADICAL_VERBOSE=DEBUG
$ export RADICAL_LOG_TGT=r.log
```

This will create the logfile r.log.  Additional logfiles will be located in your
correct directory, in a subdirectory named after your session ID.

In order to fetch log files from target hosts, please run:

```
$ radical-pilot-fetch-logfile <sid>
```

where `sid` is again the session ID in question.


