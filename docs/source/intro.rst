.. _chapter_intro:

************
Introduction
************

RADICAL-Pilot (RP) is a `Pilot Job <https://en.wikipedia.org/wiki/Pilot_job>`_
system written in Python. It allows a user to run large numbers of computational
tasks (called ``ComputeUnits``) concurrently on one or more remote
``ComputePilots`` that RADICAL-Pilot can start transparently on a multitude of
different distributed resources, like  HPC clusters and Clouds.

In this model, a part (slice) of a resource is acquired by a user's application
so that the application can directly schedule ``ComputeUnits`` into that
resource slice, rather than going through the system's job scheduler.  In many
cases, this can drastically shorten overall execution time as the individual
``ComputeUnits`` don't have to wait in the system's scheduler queue but can
execute directly on the ``ComputePilots``.

``ComputeUnits`` are often single-core / multi-threaded executables, but
RADICAL-Pilot also supports execution of parallel executables, for example based
on MPI, OpenMP, YARN/HADOOP and Spark.

RADICAL-Pilot is not a static system, but it rather provides the user with
a programming library ("Pilot-API") that provides abstractions for resource
access and task management. With this library, the user can develop everything
from simple "submission scripts" to arbitrarily complex applications, higher-
level services and tools.

.. image:: architecture.png
    :width: 600pt

The RP architecture overview image above shows the main components or RP, and
their functional relationships.  The RP system will interpret pilot descriptions
and submit the respective pilot instances on the target resources.  It will then
accept unit descriptions and submit those for execution onto the earlier created
pilots.  The Chapter :ref:`RADICAL-Pilot Overview <chapter_overview>` will
discuss those concepts in some more detail, before then turning to
:ref:`chapter_installation` which will describe how RP is deployed and
configured, so that the reader can follow the upcoming examples.

