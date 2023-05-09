=========================
Design and Implementation
=========================

RADICAL-Pilot is a distributed system that executes both a client and an agent
component. The client component executes on the same machine and the same
Python interpreter on which the application written against the RADICAL-Pilot
API executes. The agent executes either locally into a separate Python
interpreter or, most often, on a remote HPC machine into a dedicated Python
interpreter.

Figure 1 shows a high-level representation of RP architecture (yellow boxes)
when deployed on two HPC platforms (Resource A and B), executing an application
(Application) with 5 pilots (green boxes) and 36 tasks (red circles).
Application contains pilot and Task descriptions; RP Client has two components:
Pilot Manager and Task Manager. Pilot descriptions are passed to the Pilot
Manager and Task descriptions to the Task Manager. The Pilot Manager uses Pilot
Launcher to launch 2 of the 5 described pilots. One pilot is submitted to the
local Resource Management (RM) system of Resource A, the other pilot to the RM
of Resource B. Once instantiated, each pilot becomes available for Task
execution. At that point, RP Task Manager sends 2 tasks to Resource A and 5
tasks to Resource B.

.. figure:: images/architecture.png
   :width: 600pt
   :alt: RP architecture

   Figure 1. High-level view of RP architecture when deployed on a simplified
   view of two HPC platforms.

`PilotManager` and `PilotManager Worker`
----------------------------------------

.. image:: images/architecture_pilotmanager.png

Download :download:`PDF version <images/architecture_pilotmanager.pdf>`.

`TaskManager` and `TaskManager Worker`
--------------------------------------

.. image:: images/architecture_taskmanager.png

Download :download:`PDF version <images/architecture_taskmanager.pdf>`.


Task Scheduling
===============

RP implements client- and agent-level task scheduling. At client-level, RP
schedules tasks across multiple pilots that, in turn, can run on a single or
multiple HPC platforms. At agent-level, RP schedules tasks on the resources
available to a specific pilot. Thus, RP can first schedule tasks across multiple
pilots/HPC platform, and then schedule tasks for each pilot into available
resources, e.g., cores and GPUs.

The :class:`radical.pilot.TaskManager` dispatches tasks to available pilots for
execution.  It does so according to some scheduling algorithm, which can be
selected when constructing an object `radical.pilot.TaskManager`.  Currently, RP
supports two scheduling algorithms: 'Round-Robin' and 'Backfilling'.  New
schedulers can be added to `radical.pilot.TaskManager`. Please Open an issue on
RP's `issue tracker
<https://github.com/radical-cybertools/radical.pilot/issues>`_ for support.

Once a pilot agent takes ownership of tasks assigned to it by a task manager,
the agent scheduler will place tasks on the set of available resources
(cores/GPUs) that the agent is managing.  The agent scheduler can be configured
via agent and resource configuration files (see :ref:`chapter_supported`).

Round-Robin Scheduler (`SCHEDULER_ROUND_ROBIN`)
-----------------------------------------------

The Round-Robin scheduler will fairly distribute arriving tasks over
the set of known pilots, independent of task state, expected workload, pilot
state or pilot lifetime.  As such, it is a fairly simplistic, but also a very
fast scheduler, which does not impose any additional communication round trips
between the task manager and pilot agents.


Backfilling Scheduler (`SCHEDULER_BACKFILLING`)
----------------------------------------------

The backfilling scheduler does a better job at actual load balancing, but at
the cost of additional communication round trips.  It depends on the actual
application workload if that load balancing is beneficial or not.

Backfilling is most beneficial for large numbers of pilots and for relatively
long-running tasks, where the task runtime is significantly longer than the
communication round trip time between task manager and pilot agent.

In general, we do *not* recommend to use backfilling for:
  - a single pilot;
  - large numbers of short-running tasks.

The backfilling scheduler (BF) will only dispatch tasks to pilot agents once
the pilot agent is in 'RUNNING' state.  The tasks will thus get executed even
if one of the pilots never reaches that state: the load will be distributed
between pilots which become 'ACTIVE'.

The BF will only dispatch as many tasks to an agent which the agent can, in
principle, execute concurrently.  No tasks will be waiting in the agent's own
scheduler queue.  The BF will react on task termination events, and will then
backfill (!) the agent with any remaining tasks.  The agent will remain
under-utilized during that communication.

In order to minimize agent under-utilization, the user can set the environment
variable `RADICAL_PILOT_BF_OVERSUBSCRIPTION`, which specifies (in percent)
with how many tasks the BF can overload the pilot agent, without waiting for
task termination notices. This mechanism effectively hides the communication
latencies, as long as task runtimes are significantly larger than the
communication delays.  The default over subscription value is '0%', i.e., no
over subscription.


Advanced Profiling
=================

.. note:: This section is for developers, and should be disregarded for production
          runs and 'normal' users in general.

RADICAL-Pilot allows to tweak the pilot process behavior in many details, and
specifically allows to artificially increase the load on individual
components, for the purpose of more detailed profiling, and identification of
bottlenecks. With that background, a pilot description supports an additional
attribute `_config`, which accepts a dict of the following structure:

.. code-block:: python

        pdesc = rp.PilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime  = 5 # minutes
        pdesc.cores    = 8
        pdesc.cleanup  = False
        pdesc._config  = {'number_of_workers' : {'StageinWorker'   :  1,
                                                 'ExecWorker'      :  2,
                                                 'StageoutWorker'  :  1,
                                                 'UpdateWorker'    :  1},
                          'blowup_factor'     : {'Agent'           :  1,
                                                 'stagein_queue'   :  1,
                                                 'StageinWorker'   :  1,
                                                 'schedule_queue'  :  1,
                                                 'Scheduler'       :  1,
                                                 'execution_queue' : 10,
                                                 'ExecWorker'      :  1,
                                                 'watch_queue'     :  1,
                                                 'Watcher'         :  1,
                                                 'stageout_queue'  :  1,
                                                 'StageoutWorker'  :  1,
                                                 'update_queue'    :  1,
                                                 'UpdateWorker'    :  1},
                          'drop_clones'       : {'Agent'           :  1,
                                                 'stagein_queue'   :  1,
                                                 'StageinWorker'   :  1,
                                                 'schedule_queue'  :  1,
                                                 'Scheduler'       :  1,
                                                 'execution_queue' :  1,
                                                 'ExecWorker'      :  0,
                                                 'watch_queue'     :  0,
                                                 'Watcher'         :  0,
                                                 'stageout_queue'  :  1,
                                                 'StageoutWorker'  :  1,
                                                 'update_queue'    :  1,
                                                 'UpdateWorker'    :  1}}


That configuration tunes the concurrency of some components of the pilot (here
we use two `ExecWorker` instances to spawn tasks).  Further, we request that the
number of tasks handled by the `ExecWorker` is 'blown up' (multiplied) by 10.
This will create 9 near-identical tasks for every task which enters that
component, and thus the load increases on that specific component, but not on
any of the previous ones.  Finally, we instruct all components but the
`ExecWorker`, `watch_queue` and `Watcher` to drop the clones again, so that
later components won't see those clones either.  We thus strain only a specific
part of the pilot.

Setting these parameters requires some understanding of the pilot architecture.
While in general the application semantics remains unaltered, these parameters
do significantly alter resource consumption.  Also, there do exist invalid
combinations which will cause the agent to fail, specifically it will usually be
invalid to push updates of cloned tasks to the client module (via MongoDB).

The pilot profiling (as stored in `agent.prof` in the pilot sandbox) will
contain timings for the cloned tasks.  The task IDs will be based upon the
original task IDs, but have an appendix `.clone.0001` etc., depending on the
value of the respective blowup factor.  In general, only one of the
blowup-factors should be larger than one (otherwise the number of tasks will
grow exponentially, which is probably not what you want).