
.. _chapter_overview:

*****************************
RADICAL-Pilot (RP) - Overview
*****************************

This section provides an overview of RADICAL-Pilot (RP) and related concepts.
You will learn what problems RP aims to solve for you. You are introduced to
some vocabulary, RP architecture and RP operation.

.. We will keep the information on a very general level, and will avoid any details
.. which will not contribute to the user experience.  Having said that, feel free
.. to skip ahead to the :ref:`chapter_user_guide` if you are more interested in
.. directly diving into the thick of using RP!

What problems does RP solve?
============================

RP supports running applications with many computational tasks on one or more
HPC resources, focusing on the following aspects:

#. Support the efficient concurrent and sequential execution of large
   numbers of tasks.

#. Support the execution of heterogeneous tasks. E.g., single-core, OpenMP,
   MPI, single- and multi-GPU.

#. Support multiple HPC platforms. E.g., XSEDE HPC resources, DoE and NSF
   leadership class machines.

#. Support the execution of tasks on multiple pilots on one or more HPC
   platforms.

.. #. abstract the heterogeneity of distributed resources, so that running
..    applications is uniform across them, from a users perspective;

Summarizing, RP is the right tool if, for example, you want to: 

#. perform whatever type of simulation or analysis via a bag of up to tens of
   thousand of tasks, homogeneous or heterogeneous in size and duration.

#. write an application/service that requires a pilot-based runtime system
   capable of executing tasks on one or more HPC platforms, possibly
   concurrently.


What is a Task?
===============

Tasks are wrappers around self-contained executables, executed as one or more
processes on the operating system of one or more compute nodes of a HPC
cluster. As such, tasks are independent, i.e., don't execute with a shared
memory space and tasks are not methods or functions executed by RP on the
resources of a HPC platform. Task executables can be any 'program' like, for
example, Gromacs, Namd or stress but also sleep, date, etc.


What is a Workload?
===================

Workloads are sets of tasks. RP assumes no priority among the tasks of a
workload so workloads are different from workflows. RP makes no assumption
about when tasks of a workloads are provided. RP schedule, places and launches
the tasks that are available at the moment in which resources are available.
For RP it makes no difference if new tasks arrive while other tasks are
executing.


What is a Compute Unit (CU)?
============================

In RP, tasks are called ``ComputeUnits`` (CU, or 'unit'), indicating that are
independent and self-contained units of computation. Each CU represents a
self-contained, executable part of the application's workload.  A CU is
described by the following attributes (for more details, see the
:class:`API documentation <radical.pilot.ComputeUnitDescription>`):

  * `executable`    : the name of the executable to be run on the target machines
  * `arguments`     : a list of argument strings to be passed to the executable
  * `environment`   : a dictionary of environment variable/value pairs to be set before unit execution
  * `input_staging` : a set of staging directives for input data
  * `output_staging`: a set of staging directives for output data


What is a Pilot?
================

As an abstraction, a pilot is a placeholder for resources on a given platform
and is capable of executing tasks of a workload on those resources. As a
system, pilot is a type of middleware software that implements the pilot
abstraction. 

RP is a pilot system, capable of (1) acquiring resources by submitting jobs to
HPC platforms; (2) managing those resources on the user's (or application's)
behalf; and (3) executing sets and sequences of ``ComputeUnits`` on those
resources.

Usually, applications written with RP: (1) define one or more pilots; (2)
define the HPC platform where each pilot should be submitted; (3) the type and
amount of resources that each pilot should acquire on that resource; and (3)
the time for which each pilot's resources should be available (i.e.,
walltime). Once each pilot is defined, the application can schedule
``ComputeUnits`` for execution on it.

Figure 1 shows a high-level representation of RP architecture (yellow boxes)
when deployed on two HPC platforms (Resource A and B), executing an
application (Application) with 5 pilots (green boxes) and 36 CUs (red
circles). Application contains pilot and CU descriptions; RP Client has two
components: Pilot Manager and Unit Manager. Pilot descriptions are passed to
the Pilot Manager and Unit descriptions to the Unit Manager. The Pilot Manager
uses Pilot Launcher to launch 2 of the 5 described pilots. One pilot is
submitted to the Local Resource Management System (LRMS) of Resource A, the
other pilot to the LRMS of Resource B. Once instantiated, each pilot becomes
available for CU execution. At that point, RP Unit Manager sends 2 units to
Resource A and 5 units to Resource B.

.. figure:: architecture.png
   :width: 600pt
   :alt: RP architecture 

   Figure 1. High-level view of RP architecture when deployed on a simplified
   view of two HPC platforms.


How about data?
===============

Data management is important for executing CUs, both in providing input data,
and staging/sharing output data.  RP has different means to handle data, and
they are specifically covered in sections:
:ref:`in <chapter_user_guide_06>`
:ref:`the <chapter_user_guide_07>`
:ref:`UserGuide <chapter_user_guide_08>`.


Why do I need a MongoDB to run RP?
==================================

The RP application uses a MongoDB database to communicate with the pilots it
created: upon startup, the pilots will connect to the database and look for
CUs to execute.  Similarly, pilots will push information into the database,
such as about units which completed execution. You can run your own MongoDB or
use one provided by the RADICAL group. In each case, the MongoDB server needs
to be accessible by the login node of the target HPC resource and by the host
from which the RP application is executed. More details about MongoDB
requirements and deployment can be found in section
:ref:`chapter_installation`.


How do I monitor pilots and CUs?
================================

Pilots and units progress according to state models. Figure 2 shows the state
models of a pilot (left) and of a CU (right). States ending in ``pending``
(light blue boxes) indicate that pilots or units are queued in one of the RP
components. All the other states (blue boxes) indicate that pilots or units
are managed by an RP component.

.. figure:: global-state-model-plain.png
   :width: 400pt
   :alt: Pilot and CU state models.

   Figure 2. (left) Pilot state model; (right) Compute Unit state model.

When writing an RP application, ``pilot.state`` and ``unit.state`` always
report the current state of the entities. Callbacks can be registered for
notifications on unit and pilot state changes.

Setting the environment variable ``RADICAL_LOG_LVL=INFO`` in the shell from
which the RP application is executed, turns on logging. Logging provides
information about RP's inner functionality.  Pilots running on target
resources also create log files, useful for debugging purposes.


What about logging and profiling?
=================================

RP supports logging to the terminal and to files.  Also, profiles can be
written during runtime. You can set the following environment variables in the
shell from which the RP application is executed:

.. code-block:: bash
   RADICAL_LOG_LVL=DEBUG
   RADICAL_LOG_TGT=/tmp/rp.log
   RADICAL_PROF=True

The defined verbosity levels are the same as defined by Python's logging module.


