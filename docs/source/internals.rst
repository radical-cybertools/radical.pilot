.. _chapter_design:

=========================
Design and Implementation
=========================

.. toctree::

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

.. `PilotManager` and `PilotManager Worker`
.. ----------------------------------------

.. .. figure:: images/architecture_pilotmanager.png
..  :width: 600pt
..  :alt: RP PilotManager architecture

.. Download :download:`PDF version <images/architecture_pilotmanager.pdf>`.

.. `TaskManager` and `TaskManager Worker`
.. --------------------------------------

.. .. figure:: images/architecture_taskmanager.png
..  :width: 600pt
..  :alt: RP TaskManager architecture

.. Download :download:`PDF version <images/architecture_taskmanager.pdf>`.

State Model
===========

Pilot and Task progress through linear state models.  The state names indicate
what RP module and component operate on the specific Pilot or Task entity.
Specifically, a Pilot or Task is, at any point in time, either owned by a RP
component or is waiting in a Queue to be communicated between components.

Pilot
-----

.. csv-table:: Pilot States
  :header: "State Name",    "Module",        "Component",      "Action"
  :widths: auto

  "NEW",                    "Pilot Manager", "",               "Creating a pilot"
  "PMGR_LAUNCHING_PENDING", "Pilot Manager", "Launcher queue", "Pilot waits for submission"
  "PMGR_LAUNCHING",         "Pilot Manager", "Pilot Launcher", "Submit a pilot to the batch system"
  "PMGR_ACTIVE_PENDING",    "LRM",           "",               "Pilot is waiting in the batch queue or bootstrapping"
  "PMGR_ACTIVE",            "LRM",           "",               "Pilot is active on the cluster resources"
  "DONE",                   "Pilot Manager", "",               "Pilot marked as done. Final state"
  "CANCELED",               "Pilot Manager", "",               "Pilot marked as cancelled. Final state"
  "FAILED",                 "Pilot Manager", "",               "Pilot marked as failed. Final state"

Task
----

.. csv-table:: Task States
  :header: "State Name",          "Module",       "Component",        "Action"
  :widths: auto

  "NEW",                          "Task Manager", "",                 "Creating a task"
  "TMGR_SCHEDULING_PENDING",      "Task Manager", "Scheduler queue",  "Task queued for scheduling on a pilot"
  "TMGR_SCHEDULING",              "Task Manager", "Scheduler",        "Assigning task to a pilot"
  "TMGR_STAGING_INPUT_PENDING",   "Task Manager", "Stager In queue",  "Task queued for data staging"
  "TMGR_STAGING_INPUT",           "Task Manager", "Stager In",        "Staging task's files to the target platform (if any)"
  "AGENT_STAGING_INPUT_PENDING",  "Agent",        "Stager In queue",  "Task waiting to be picked up by Agent"
  "AGENT_STAGING_INPUT",          "Agent",        "Stager In",        "Staging task's files inside the target platform, making available within the task sandbox"
  "AGENT_SCHEDULING_PENDING",     "Agent",        "Scheduler queue",  "Task waiting for scheduling on resources, i.e., cores and/or GPUs"
  "AGENT_SCHEDULING",             "Agent",        "Scheduler",        "Assign cores and/or GPUs to the task"
  "AGENT_EXECUTING_PENDING",      "Agent",        "Executor queue",   "Cores and/or GPUs are assigned, wait for execution"
  "AGENT_EXECUTING",              "Agent",        "Executor",         "Executing tasks on assigned cores and/or GPUs. Available resources are utilized"
  "AGENT_STAGING_OUTPUT_PENDING", "Agent",        "Stager Out queue", "Task completed and waits for output staging"
  "AGENT_STAGING_OUTPUT",         "Agent",        "Stager Out",       "Staging out task files within the platform (if any)"
  "TMGR_STAGING_OUTPUT_PENDING",  "Task Manager", "Stager Out queue", "Waiting for Task Manager to pick up Task again"
  "TMGR_STAGING_OUTPUT",          "Task Manager", "Stager Out",       "Task's files staged from remote to local resource (if any)"
  "DONE",                         "Task Manager", "",                 "Task marked as done. Final state"
  "CANCELED",                     "Task Manager", "",                 "Task marked as cancelled. Final state"
  "FAILED",                       "Task Manager", "",                 "Task marked as failed. Final state"


.. Advanced Profiling
.. ==================

.. .. note:: This section is for developers, and should be disregarded for production runs and end-users in general.

.. RADICAL-Pilot allows to tweak the pilot process behavior in many details, and
.. specifically allows to artificially increase the load on individual
.. components, for the purpose of more detailed profiling, and identification of
.. bottlenecks. With that background, a pilot description supports an additional
.. attribute `_config`, which accepts a dict of the following structure:

.. .. code-block:: python

..     pdesc = rp.PilotDescription()
..     pdesc.resource = "local.localhost"
..     pdesc.runtime = 5  # minutes
..     pdesc.cores = 8
..     pdesc.cleanup = False
..     pdesc._config = {
..         "number_of_workers": {
..             "StageinWorker": 1,
..             "ExecWorker": 2,
..             "StageoutWorker": 1,
..             "UpdateWorker": 1,
..         },
..         "blowup_factor": {
..             "Agent": 1,
..             "stagein_queue": 1,
..             "StageinWorker": 1,
..             "schedule_queue": 1,
..             "Scheduler": 1,
..             "execution_queue": 10,
..             "ExecWorker": 1,
..             "watch_queue": 1,
..             "Watcher": 1,
..             "stageout_queue": 1,
..             "StageoutWorker": 1,
..             "update_queue": 1,
..             "UpdateWorker": 1,
..         },
..         "drop_clones": {
..             "Agent": 1,
..             "stagein_queue": 1,
..             "StageinWorker": 1,
..             "schedule_queue": 1,
..             "Scheduler": 1,
..             "execution_queue": 1,
..             "ExecWorker": 0,
..             "watch_queue": 0,
..             "Watcher": 0,
..             "stageout_queue": 1,
..             "StageoutWorker": 1,
..             "update_queue": 1,
..             "UpdateWorker": 1,
..         },
..     }


.. That configuration tunes the concurrency of some components of the pilot (here
.. we use two `ExecWorker` instances to spawn tasks). Further, we request that the
.. number of tasks handled by the `ExecWorker` is 'blown up' (multiplied) by 10.
.. This will create 9 near-identical tasks for every task which enters that
.. component, and thus the load increases on that specific component, but not on
.. any of the previous ones. Finally, we instruct all components but the
.. `ExecWorker`, `watch_queue` and `Watcher` to drop the clones again, so that
.. later components won't see those clones either. We thus strain only a specific
.. part of the pilot.

.. Setting these parameters requires some understanding of the pilot architecture.
.. While in general the application semantics remains unaltered, these parameters
.. do significantly alter resource consumption. Also, there do exist invalid
.. combinations which will cause the agent to fail, specifically it will usually be
.. invalid to push updates of cloned tasks to the client module (via MongoDB).

.. The pilot profiling (as stored in `agent.prof` in the pilot sandbox) will
.. contain timings for the cloned tasks. The task IDs will be based upon the
.. original task IDs, but have an appendix `.clone.0001` etc., depending on the
.. value of the respective blowup factor. In general, only one of the
.. blowup-factors should be larger than one (otherwise the number of tasks will
.. grow exponentially, which is probably not what you want).
