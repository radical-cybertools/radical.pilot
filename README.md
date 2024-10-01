# RADICAL-Pilot (RP)

[![Build Status](https://github.com/radical-cybertools/radical.pilot/actions/workflows/ci.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/radicalpilot/badge/?version=stable)](http://radicalpilot.readthedocs.io/en/stable/?badge=stable)
[![codecov](https://codecov.io/gh/radical-cybertools/radical.pilot/branch/devel/graph/badge.svg)](https://codecov.io/gh/radical-cybertools/radical.pilot)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8224/badge)](https://www.bestpractices.dev/projects/8224)

RADICAL-Pilot (RP) executes heterogeneous tasks with maximum concurrency and at
scale. RP can concurrently execute up to $10^5$ heterogeneous tasks, including
single/multi core/GPU and MPI/OpenMP. Tasks can be stand-alone executables or
Python functions and both types of task can be concurrently executed.

RP is a [Pilot system](https://doi.org/10.1145/3177851), i.e., it separates
resource acquisition from using those resources to execute application tasks. RP
acquires resources by submitting a job to an HPC platform, and it can directly
schedule and launch computational tasks on those resources. Thus, tasks are
directly scheduled on the acquired resources, not via the batch system of the
HPC platform. RP supports concurrently using single/multiple pilots on
single/multiple
[high performance computing (HPC) platforms](https://radicalpilot.readthedocs.io/en/stable/supported.html).

RP is written in Python and exposes a simple yet powerful
[API](https://radicalpilot.readthedocs.io/en/stable/apidoc.html). In 15 lines of
code, you can execute an arbitrary number of executables with maximum
concurrency on a
[Linux container](https://hub.docker.com/u/radicalcybertools)
or, by changing `resource`, on one of the
[supported HPC platforms](https://radicalpilot.readthedocs.io/en/stable/supported.html).

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

## Quick Start

Run RP's [quick start tutorial](https://mybinder.org/v2/gh/radical-cybertools/radical.pilot/HEAD?labpath=docs%2Fsource%2Fgetting_started.ipynb) directly on Binder. No installation needed.

After going through the tutorial, install RP and start to code your application:

```shell
python -m venv ~/.ve/radical-pilot
. ~/.ve/radical-pilot/bin/activate
pip install radical.pilot
```

Note that other than `venv`, you can also use
[`virtualenv`](https://radicalpilot.readthedocs.io/en/stable/getting_started.html#Virtualenv),
[`conda`](https://radicalpilot.readthedocs.io/en/stable/getting_started.html#Conda)
or
[`spack`](https://radicalpilot.readthedocs.io/en/stable/getting_started.html#Spack). 

For some inspiration, see our RP application 
[examples](https://github.com/radical-cybertools/radical.pilot/tree/devel/examples),
starting from
[00_getting_started.py](https://github.com/radical-cybertools/radical.pilot/blob/devel/examples/00_getting_started.py)
.

## Documentation

[RP user documentation](https://radicalpilot.readthedocs.io/en/stable/) uses Sphinx, and it is published on Read the Docs.

[RP tutorials](https://mybinder.org/v2/gh/radical-cybertools/radical.pilot/HEAD) can be run via Binder.

## Developers

RP development uses Git and
[GitHub](https://github.com/radical-cybertools/radical.pilot). RP **requires**
Python3, a virtual environment and a GNU/Linux OS. Clone, install and
test RP:

```shell
python -m venv ~/.ve/rp-docs
. ~/.ve/rp-docs/bin/activate
git clone git@github.com:radical-cybertools/radical.pilot.git
cd radical.pilot
pip install -r requirements-docs.txt
sphinx-build -M html docs/source/ docs/build/
```

RP documentation uses tutorials coded as Jupyter notebooks. `Sphinx` and
`nbsphinx` run RP locally to execute those tutorials. Successful compilation of
the documentation also serves as a validation of your local development
environment.

## Docker

Please refer to the RADICAL-Cybertools [tutorial repository](https://github.com/radical-cybertools/tutorials) for instructions on how to build, use, and configure the Docker image of RP.

## Provide Feedback

Have a question, feature request or you found a bug? Feel free to open a 
[support ticket](https://github.com/radical-cybertools/radical.pilot/issues).
For vulnerabilities, please draft a private 
[security advisory](https://github.com/radical-cybertools/radical.pilot/security/advisories).

## Contributing

We welcome everyone that wants to contribute to RP development. We are an open
and welcoming community, committed to making participation a harassment-free
experience for everyone. See our
[Code of Conduct](https://radicalpilot.readthedocs.io/en/stable/process/code_of_conduct.html),
relevant
[technical documentation](https://radicalpilot.readthedocs.io/en/stable/process/contributing.html)
and feel free to 
[get in touch](https://github.com/radical-cybertools/radical.pilot/issues).
