
***************
Getting Started
***************

This guide will introduce the main usage modes of RP by presenting a series of
simple code examples.  Starting from the simplest possible RP application code,
the follow-up examples separately add and discuss more advanced features.



Getting Started
===============

A series of `getting_started_[n].py` examples demonstrate the usage of RP, by
executing a bag of simple shell commands over pilot(s).  The different scripts
all base on the simplest case in `getting_started_00.py`, introducing different
features:

* `getting_started_00.py`: basic example
* `getting_started_01.py`: obtain unit details
* `getting_started_02.py`: handle failing units
* `getting_started_03.py`: use multiple pilots
* `getting_started_04.py`: use a different scheduler
* `getting_started_05.py`: stage unit input data
* `getting_started_06.py`: stage unit output data
* `getting_started_07.py`: stage shared unit input data
* `getting_started_08.py`: use environment variables for units
* `getting_started_09.py`: running MPI units
* `getting_started_10.py`: use pre- and post- execution

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

All `getting_started_[n].py` examples accept resource targets.  To simplify the
examples, some resource configuration details are moved to a config file
(`resources.json`).  Please make sure that it contains valid settings for the
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



