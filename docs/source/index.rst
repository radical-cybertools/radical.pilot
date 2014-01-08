
##################################
SAGA-Pilot |version| documentation
##################################

############
Introduction
############

SAGA-Pilot is a `Pilot Job <https://en.wikipedia.org/wiki/Pilot_job>`_ system 
written in Python. It allows a user to run large numbers of computational 
tasks ("Jobs") concurrently on one or more HPC clusters. Jobs are often 
single-core / multi-threaded executables, but SAGA-Pilot also supports 
execution of parallel executables, for example based on MPI or OpenMP. 

.. image:: architecture.png

SAGA-Pilot is not a static system, but it rather provides the user with a
programming library ("Pilot-API") that  provides abstractions for resource
access and task management. With this  library, the user can develop everything
from simple "submission scripts" to arbitrarily complex applications, higher-
level services and tools.

#########
Contents:
#########

.. toctree::
   :numbered:
   :maxdepth: 3

   installation.rst
   apidoc.rst
   schedulers.rst
   machconf.rst
   testing.rst

