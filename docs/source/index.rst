
#####################################
RADICAL-Pilot |version| Documentation
#####################################

RADICAL-Pilot is a `Pilot Job <https://en.wikipedia.org/wiki/Pilot_job>`_ system
written in Python. It allows a user to run large numbers of computational
tasks (called ``ComputeUnits``) concurrently on one or more remote
``ComputePilots`` that RADICAL-Pilot can start transparently on a multitude of
different distributed resources, like  HPC clusters and Clouds.

In this model, the resource is acquired by a user's application so that the
application can schedule ``ComputeUnits`` into that resource directly, rather
than going through the system's job scheduler.  In many cases, this can
drastically shorten overall exeuction time as the  individual ``ComputeUnits``
don't have to wait in the system's scheduler queue  but can execute directly
on the ``ComputePilots``.

**Mailing Lists**

* For users: https://groups.google.com/d/forum/radical-pilot-users
* For developers: https://groups.google.com/d/forum/radical-pilot-devel


#########
Contents:
#########

.. toctree::
   :numbered:
   :maxdepth: 2

   intro.rst
   installation.rst
   machconf.rst
   datastaging.rst
   examples/index.rst
   tutorial/index.rst
   schedulers.rst
   testing.rst
   benchmarks.rst
   faq.rst
   developer.rst
   apidoc.rst
   release_notes.rst

