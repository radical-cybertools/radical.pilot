# RADICAL-Pilot (RP)

[![Build Status](https://github.com/radical-cybertools/radical.pilot/actions/workflows/python-app.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/python-app.yml)
[![codecov](https://codecov.io/gh/radical-cybertools/radical.pilot/branch/devel/graph/badge.svg)](https://codecov.io/gh/radical-cybertools/radical.pilot)

RADICAL-Pilot (RP) is a Pilot system written in Python and specialized
in executing applications composed of many computational tasks on high
performance computing (HPC) platforms. As a Pilot system, RP separates resource
acquisition from using those resources to execute application tasks. Resources
are acquired by submitting a job to the batch system of an HPC machine. Once
the job is scheduled on the requested resources, RP can directly schedule and
launch application tasks on those resources. Thus, tasks are not scheduled via
the batch system of the HPC platform, but directly on the acquired resources.

## Documentation

Full system description and usage examples are available at:
https://radicalpilot.readthedocs.io/en/stable/

Additional information is provided in the
[wiki](https://github.com/radical-cybertools/radical.pilot/wiki) section of RP
GitHub repository.

## Code

Generally, the `master` branch reflects the RP release published on
[PyPI](https://pypi.org/project/radical.pilot/), and is considered stable:
it should work 'out of the box' for the supported backends. For a list of
supported backends, please refer to the documentation.

The `devel` branch (and any branch other than master) may not correspond to the
published documentation and, specifically, may have dependencies which need to
be resolved manually.

## Integration Tests status
These badges show the state of the current integration tests on different HPCs RADICAL Pilot supports

[![ORNL Summit Integration Tests](https://github.com/radical-cybertools/radical.pilot/actions/workflows/summit.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/summit.yml)
[![PSC Bridges2 Integration Tests](https://github.com/radical-cybertools/radical.pilot/actions/workflows/bridges.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/bridges.yml)
[![SDSC Comet Integration Tests](https://github.com/radical-cybertools/radical.pilot/actions/workflows/comet.yml/badge.svg)](https://github.com/radical-cybertools/radical.pilot/actions/workflows/comet.yml)
