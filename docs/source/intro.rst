.. _chapter_intro:

************
Introduction
************

RADICAL-Pilot is a `Pilot Job <https://en.wikipedia.org/wiki/Pilot_job>`_ system
written in Python. It allows a user to run large numbers of computational
tasks (called ``ComputeUnits``) concurrently on one or more remote
``ComputePilots`` that RADICAL-Pilot can start transparently on a multitude of
different distributed resources, like  HPC clusters and Clouds.

In this model, the resource is acquired by a user's application so that the
application can schedule ``ComputeUnits`` into that resource directly, rather than going
through the system's job scheduler. 

In many cases, this can drastically shorten overall exeuction time as the 
individual ``ComputeUnits`` don't have to wait in the system's scheduler queue 
but can execute directly on the ``ComputePilots``.

``ComputeUnits`` are often single-core / multi-threaded executables, but SAGA-
Pilot also supports execution of parallel executables, for example based on
MPI or OpenMP.

.. image:: architecture.png

RADICAL-Pilot is not a static system, but it rather provides the user with a
programming library ("Pilot-API") that  provides abstractions for resource
access and task management. With this  library, the user can develop everything
from simple "submission scripts" to arbitrarily complex applications, higher-
level services and tools.
