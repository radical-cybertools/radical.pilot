# RADICAL-Pilot (RP)

[![Build Status](https://github.com/radical-cybertools/radical.pilot/actions/workflows/ci.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/radical-cybertools/radical.pilot/branch/devel/graph/badge.svg)](https://codecov.io/gh/radical-cybertools/radical.pilot)

RADICAL-Pilot (RP) executes heterogeneous tasks with maximum concurrency and at scale. 

RP is a [Pilot system](https://doi.org/10.1145/3177851), i.e., it separates resource acquisition from using those resources to execute application tasks. Resources are acquired by submitting a job to an HPC platform and RP can directly schedule and launch computational tasks on those resources. Thus, tasks are directly scheduled on the acquired resources, not via the batch system of the HPC platform.

RP is written in Python and exposes a simple yet powerful [API](https://radicalpilot.readthedocs.io/en/stable/apidoc.html). In 15 lines of code you can execute an arbitrary number of executables with maximum concurrency on a [Linux container]() or, by changing `resource`, on the [supported HPC platforms](https://radicalpilot.readthedocs.io/en/stable/supported.html).

```python
import radical.pilot as rp

# Create a session
session = rp.Session()

# Create a pilot manager and a pilot
pmgr    = rp.PilotManager(session=session)
pd_init = {'resource': 'local.localhost',
           'runtime' : 30,
           'cores'   : 4}
pdesc   = rp.PilotDescription(pd_init)
pilot   = pmgr.submit_pilots(pdesc)

# Crate a task manager and describe your tasks
tmgr = rp.TaskManager(session=session)
tmgr.add_pilots(pilot)
tds = list()
for i in range(8):
    td = rp.TaskDescription()
    td.executable     = 'sleep'
    td.arguments      = ['10']
    tds.append(td)

# Submit your tasks for execution
tmgr.submit_tasks(tds)
tmgr.wait_tasks()

# Close your session
session.close(cleanup=True)
```

## Documentation

Full system description and usage examples are available at: https://radicalpilot.readthedocs.io/en/stable/

Additional information is provided in the [wiki](https://github.com/radical-cybertools/radical.pilot/wiki) section of RP GitHub repository.

## Code

Generally, the `master` branch reflects the RP release published on [PyPI](https://pypi.org/project/radical.pilot/), and is considered stable: it should work 'out of the box' for the supported backends. For a list of supported backends, please refer to the documentation.

The `devel` branch (and any branch other than master) may not correspond to the published documentation and, specifically, may have dependencies which need to be resolved manually.

## Integration Tests status
These badges show the state of the current integration tests on different HPCs RADICAL Pilot supports

[![ORNL Summit Integration Tests](https://github.com/radical-cybertools/radical.pilot/actions/workflows/summit.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/summit.yml)
[![PSC Bridges2 Integration Tests](https://github.com/radical-cybertools/radical.pilot/actions/workflows/bridges.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/bridges.yml)
