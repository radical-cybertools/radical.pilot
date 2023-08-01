.. py:module:: radical.pilot

*************
RADICAL-Pilot
*************

.. image:: https://img.shields.io/pypi/v/radical_pilot.svg
   :target: https://pypi.python.org/pypi/radical_pilot
   :alt: Pypi Package
.. image:: https://img.shields.io/pypi/l/radical_pilot.svg
   :target: https://pypi.python.org/pypi/radical_pilot/
   :alt: License
.. image:: https://readthedocs.org/projects/radicalpilot/badge/?version=devel
   :target: http://radicalpilot.readthedocs.io/en/devel/?badge=devel
   :alt: Documentation Status

RADICAL-Pilot (RP) is a
`pilot system <https://dl.acm.org/citation.cfm?id=3177851>`_
written in Python and specialized in executing applications composed of many
heterogeneous computational tasks on high performance computing (HPC) platforms.
As a Pilot system, RP separates resource acquisition from using those resources
to execute application tasks. Resources are acquired by submitting a job to the
batch system of an HPC machine. Once the job is scheduled on the requested
resources, RP can directly schedule and launch application tasks on those
resources. Thus, tasks are not scheduled via the batch system of the HPC
platform, but directly on the acquired resources with the maximum degree of
concurrency they afford. See our
`Brief Introduction to RADICAL-Pilot <https://radical-cybertools.github.io/presentations/rp_internals.mp4>`_
to see how RP works on an HPC platform.

RP offers unique features when compared to other pilot systems: (1) concurrent
and sequential execution of heterogeneous tasks on one or more pilots, e.g.,
single/multi-core, single/multi-GPU, MPI/OpenMP; (2) describing executable tasks
and Python function tasks; (3) support of all the major HPC batch systems, e.g.,
slurm, torque, pbs, lsf, etc.; (4) support of more than 16 methods to launch
tasks, e.g., ssh, mpirun, aprun, jsrun, prrte, etc.; and (5) a general purpose
distributed architecture.

* Repository: https://github.com/radical-cybertools/radical.pilot
* Issues: https://github.com/radical-cybertools/radical.pilot/issues


.. toctree::
   :maxdepth: 2

   getting_started.ipynb
   tutorials.rst
   supported.rst
   envs.rst
   glossary.rst
   internals.rst
   apidoc.rst
   release_notes.md
