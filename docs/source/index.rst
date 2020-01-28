
#####################################
RADICAL-Pilot |version| Documentation
#####################################

RADICAL-Pilot (RP) is a Pilot system `[1]
<https://dl.acm.org/citation.cfm?id=3177851>`_ `[2]
<https://ieeexplore.ieee.org/abstract/document/6404423>`_ written in Python
and specialized in executing applications composed of many computational tasks
on high performance computing (HPC) platforms. As a Pilot system, RP separates
resource acquisition from using those resources to execute application
tasks. Resources are acquired by submitting a job to the batch system of an
HPC machine. Once the job is scheduled on the requested resources, RP can
directly schedule and launch application tasks on those resources. Thus, tasks
are not scheduled via the batch system of the HPC platform, but directly on the
acquired resources.

As every Pilot system, RP offers two main benefits: (1) high-throughput task
execution; and (2) concurrent and sequential task executions on the same
pilot. High-throughput is possible because the user exclusively owns the
resources on which those tasks are executed for as long as the job submitted
to the HPC platform remains available. Depending on resource availability,
tasks can be scheduled concurrently and, if more tasks need to be executed,
one after the other. In this way, tasks can execute both concurrently and
sequentially on the same pilot.

RP offers four unique features when compared to other pilot systems or tools
that enable the execution of multi-task applications on HPC platforms: (1)
execution different types of tasks concurrently on the same pilot, e.g.,
single-core, OpenMP, MPI, single- and multi-GPU; (2) support of all the major
HPC batch systems, e.g., slurm, torque, pbs, lsf, etc.; (3) support of more
than 14 methods to launch tasks, e.g., ssh, mpirun, aprun, jsrun, prrte, etc.;
and (4) a general purpose architecture.

RADICAL-Pilot is not a static system, but it rather provides the user with a
programming library ("Pilot-API") that provides abstractions for resource
access and task management. With this library, the user can develop everything
from simple "submission scripts" to arbitrarily complex applications,
higher-level services and tools.

Chapter :ref:`RADICAL-Pilot Overview <chapter_overview>` offers more
information about tasks, workloads, pilot, pilot systems, and RP
implementation. The user is **strongly invited** to carefully read that
section before starting to use RP.

.. It allows a user to run large numbers of computational tasks (called
.. ``ComputeUnits``) concurrently and sequentially, on one or more remote
.. ``ComputePilots`` that RADICAL-Pilot can start transparently on a multitude
.. of different distributed resources, like HPC clusters and Clouds.

.. In this model, a part (slice) of a resource is acquired by a user's
.. application so that the application can directly schedule ``ComputeUnits``
.. into that resource slice, rather than going through the system's job
.. scheduler.  In many cases, this can drastically shorten overall execution
.. time as the individual ``ComputeUnits`` don't have to wait in the system's
.. scheduler queue but can execute directly on the ``ComputePilots``.

.. ``ComputeUnits`` can be sequential, multi-threaded (e.g. OpenMP), parallel
.. process (e.g. MPI) executables, Hadoop or Spark applications.


**Links**

* repository:     https://github.com/radical-cybertools/radical.pilot
* issues:         https://github.com/radical-cybertools/radical.pilot/issues        
* user list:      https://groups.google.com/d/forum/radical-pilot-users
* developer list: https://groups.google.com/d/forum/radical-pilot-devel


#########
Contents:
#########

.. toctree::
   :numbered:
   :maxdepth: 2

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
   release_notes.rst
   intro.rst
   profiles.rst

