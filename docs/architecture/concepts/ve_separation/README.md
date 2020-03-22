
RP is written in python and depends on a number of non-standard modules.  At the
same time, RP is expected to fully run in user space, and thus cannot depend on
any specific prepared system environment.

RP, and specifically the RP pilot agent, will thus create a virtualenv (or conda
env) on the fly and run within that environment.

At the same time, RP needs to execute heterogeneous workloads, and specifically
also tasks which use different python deployments or configurations: conda,
other virtualenvs, system python, python modules, etc.

This test attempts to reproduce this setup with minimal amount of code:
`runme.sh` will create a virtualenv to run a minimzalistic RP executor
(executor.py, started via bootstrap.sh), and will further create a conda env and
a virtualenv for tests.  The executor runs different python scripts, attempting
to use the test virtualenv, the test conda env, and system python.  The scripts
will verify correct module loading from the expected environment.
