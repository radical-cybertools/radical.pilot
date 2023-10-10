
Getting Started
===============

A series of `getting_started_[n].py` examples demonstrate the usage of RP, by
executing a bag of simple shell commands over pilot(s).  The different scripts
are all based on the simplest case in `00_getting_started.py`, introducing
different features:

* `00_getting_started.py`: basic example
* `01_task_details.py`: obtain task details
* `02_failing_tasks.py`: handle failing tasks
* `03_multiple_pilots.py`: use multiple pilots
* `04_scheduler_selection.py`: use a different scheduler
* `05_task_input_data.py`: stage task input data
* `06_task_output_data.py`: stage task output data
* `07_shared_task_data.py`: stage shared task input data
* `08_task_environment.py`: use environment variables for tasks
* `09_mpi_tasks.py`: running MPI tasks
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
os.environ['RADICAL_VERBOSE'] = 'DEMO'
```

to

```
os.environ['RADICAL_VERBOSE'] = 'DEBUG'
```

All `[nn]_*.py` examples accept resource targets.  To simplify the
examples, some resource configuration details are moved to a config file
(`config.json`).  Please make sure that it contains valid settings for the
target resources.
