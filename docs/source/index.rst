
#####################################
RADICAL-Pilot |version| Documentation
#####################################

RADICAL-Pilot (RP) is a `Pilot Job <https://en.wikipedia.org/wiki/Pilot_job>`_ system
written in Python. It allows a user to run large numbers of computational tasks
(called ``ComputeUnits``) concurrently on one or more remote ``ComputePilots``
that RADICAL-Pilot can start transparently on a multitude of different
distributed resources, like HPC clusters and Clouds.

In this model, a part (slice) of a resource is acquired by a user's application
so that the application can directly schedule ``ComputeUnits`` into that
resource slice, rather than going through the system's job scheduler.  In many
cases, this can drastically shorten overall exeuction time as the individual
``ComputeUnits`` don't have to wait in the system's scheduler queue but can
execute directly on the ``ComputePilots``.

``ComputeUnits`` can be sequential, multi-threaded (e.g. OpenMP) or parallel process (e.g. MPI) executables.

RADICAL-Pilot is not a static system, but it rather provides the user with
a programming library ("Pilot-API") that provides abstractions for resource
access and task management. With this library, the user can develop everything
from simple "submission scripts" to arbitrarily complex applications, higher-
level services and tools.

**Links**

* repository:     https://github.com/radical-cybertools/radical.pilot
* user list:      https://groups.google.com/d/forum/radical-pilot-users
* developer list: https://groups.google.com/d/forum/radical-pilot-devel


#########
Contents:
#########

.. toctree::
   :numbered:
   :maxdepth: 2

   intro.rst
   overview.rst
   installation.rst
   user_guide/index.rst
   tutorial/index.rst
   apidoc.rst
   datastaging.rst
   machconf.rst
   schedulers.rst
   testing.rst
   benchmarks.rst
   faq.rst
   developer.rst

