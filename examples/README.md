
Getting Started
===============

A series of `getting_started_[n].py` examples demonstrate the usage of RP, by
executing a bag of simple shell commands over pilot(s).  The different scripts
are all based on the simplest case in `00_getting_started.py`, introducing
different features:

* `00_getting_started.py`: basic example
* `01_unit_details.py`: obtain unit details
* `02_failing_units.py`: handle failing units
* `03_multiple_pilots.py`: use multiple pilots
* `04_scheduler_selection.py`: use a different scheduler
* `05_unit_input_data.py`: stage unit input data
* `06_unit_output_data.py`: stage unit output data
* `07_shared_unit_data.py`: stage shared unit input data
* `08_unit_environment.py`: use environment variables for units
* `09_mpi_units.py`: running MPI units
* `10_pre_and_post_exec.py`: use pre- and post- execution

(script evolution:
 0
 +-- 1
     +-- 2
     +-- 3
     |   +-- 4
     +-- 5
     |   +-- 6
     |   +-- 7
     +-- 8
     +-- 9
     +-- 10
  

All examples use the 'reporting' facility of RADICAL-Utils for output.  That can
be changed to rather verbose debugging output via the `os.environment` setting
at the beginning of the example, by changing

```
os.environ['RADICAL_LOG_LVL'] = 'REPORT'
```

to

```
os.environ['RADICAL_LOG_LVL'] = 'DEBUG'
```

All `[nn]_*.py` examples accept resource targets.  To simplify the
examples, some resource configuration details are moved to a config file
(`config.json`).  Please make sure that it contains valid settings for the
target resources.

All examples assume password-less ssh access to be configured out-of-band (or
gsissh if so indicated in the resource config).

RP requires a MongoDB instance as storage backend.  Please set the environment
variable `RADICAL_PILOT_DBURL` to point to a valid MongoDB.  The value should
have the form:

```
  export RADICAL_PILOT_DBURL="mongodb://some.host.ne:port/database_name/"
```

The specified database does not not need to exist, but is created on the fly.
For MongoDB instances which require user/pass authentication, use

```
  export RADICAL_PILOT_DBURL="mongodb://user:pass@some.host.ne:port/database_name/"
```

Other MongoDB authentication methods are currently not supported.  *Note that
unsecured databases are open to man-in-the-middle attacks!*


