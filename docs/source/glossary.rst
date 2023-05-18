.. _chapter_glossary:

========
Glossary
========

This is the main terminology used in RADICAL-Pilot (RP)'s documentation and
application programming interface.

Workload
========

Workloads are sets of tasks. RP assumes no priority among the tasks of a
workload, i.e., workloads are different from workflows. RP makes no assumption
about when tasks of a workload are provided. RP schedules, places and launches
the tasks that are available at the moment in which resources are available. For
RP, it makes no difference if new tasks arrive while other tasks are executing.


Task
====

Tasks are self-contained portions of an application which RP can execute
independently of each other.  Those tasks are usually application executables
(such as `Gromacs` or `NAMD`) whose execution is further parameterized by
command line arguments, input files, environment settings, etc. But those tasks
can also be individual function calls or small snippets of Python code. In that
case, the execution is parameterized by function arguments, Python execution
context, etc.

It is important to note that RP considers tasks independent, i.e., they don't
execute with a shared memory space.

For more details, see the
:class:`API documentation <radical.pilot.TaskDescription>`

.. Task Rank
.. ---------

.. The notion of `rank` is central to RP's `TaskDescription` class.  We use the
.. same notion of rank as the one used in the message pass interface (MPI)
.. `standard <https://www.mpi-forum.org/docs/mpi-4.0/mpi40-report.pdf>`_. The
.. number of `ranks` refers to the number of individual processes to be spawned by
.. the task execution backend. These processes will be near-exact copies of each
.. other: they run in the same working directory and the same `environment`, are
.. defined by the same `executable` and `arguments`, get the same amount of
.. resources allocated, etc. Notable exceptions are:

..   - Rank processes may run on different nodes;
..   - rank processes can communicate via MPI;
..   - each rank process obtains a unique rank ID.

.. It is up to the underlying MPI implementation to determine the exact value of
.. the process' rank ID.  The MPI implementation may also set a number of
.. additional environment variables for each process.

.. It is important to understand that only applications which make use of MPI
.. should have more than one rank, otherwise identical copies of the *same*
.. application instance are launched which will compute the same results, thus
.. wasting resources for all ranks but one.  Worse: I/O-routines of these non-MPI
.. ranks can interfere with each other and invalidate those results.

.. Also: applications with a single rank cannot make effective use of
.. MPI---depending on the specific resource configuration, RP may launch those
.. tasks without providing an MPI communicator.


Pilot
=====

As an abstraction, a pilot is a placeholder for resources on a given platform
and is capable of executing tasks of a workload on those resources. As a system,
pilot is a type of middleware software that implements the pilot abstraction.

RP is a pilot system, capable of: (1) acquiring resources by submitting jobs to
HPC platforms; (2) managing those resources on the user's (or application's)
behalf; and (3) executing sets and sequences of ``Tasks`` on those resources.

Usually, applications written with RP: (1) define one or more pilots; (2) define
the HPC platform where each pilot should be submitted; (3) the type and amount
of resources that each pilot should acquire on that resource; and (3) the time
for which each pilot's resources should be available (i.e., walltime). Once each
pilot is defined, the application can schedule ``Tasks`` for execution on it.
