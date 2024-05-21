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

State Model
===========

Pilot and Task progress through linear state models.  The state names indicate
what RP module and component operate on the specific Pilot or Task entity.
Specifically, a Pilot or Task is, at any point in time, either owned by a RP
component or is waiting in a Queue to be communicated between components.

Pilot
-----

.. csv-table:: Pilot States
  :header: "State Name",    "Module",        "Component",      "Action",                                              "Sandbox", "Prof file"
  :widths: auto

  "NEW",                    "Pilot Manager", "",               "Creating a pilot",                                     "client", "pmgr.0000.prof"
  "PMGR_LAUNCHING_PENDING", "Pilot Manager", "Launcher queue", "Pilot waits for submission",                           "client", "pmgr.0000.prof"
  "PMGR_LAUNCHING",         "Pilot Manager", "Pilot Launcher", "Submit a pilot to the batch system",                   "client", "pmgr.0000.prof, pmgr_launching.0000.prof"
  "PMGR_ACTIVE_PENDING",    "LRM",           "",               "Pilot is waiting in the batch queue or bootstrapping", "client", "pmgr.0000.prof, pmgr_launching.0000.prof"
  "PMGR_ACTIVE",            "LRM",           "",               "Pilot is active on the cluster resources",             "client", "pmgr.0000.prof, pmgr_launching.0000.prof"
  "DONE",                   "Pilot Manager", "",               "Pilot marked as done. Final state",                    "client", "pmgr_launching.0000.prof"
  "CANCELED",               "Pilot Manager", "",               "Pilot marked as cancelled. Final state",               "client", "pmgr_launching.0000.prof"
  "FAILED",                 "Pilot Manager", "",               "Pilot marked as failed. Final state",                  "client", "pmgr_launching.0000.prof"

Task
----

.. csv-table:: Task States
  :header: "State Name",          "Module",       "Component",        "Action",                                                                                    "Sandbox", "Prof file"
  :widths: auto

  "NEW",                          "Task Manager", "",                 "Creating a task",                                                                           "client",        "tmgr.0000.prof"
  "TMGR_SCHEDULING_PENDING",      "Task Manager", "Scheduler queue",  "Task queued for scheduling on a pilot",                                                     "client",        "tmgr.0000.prof"
  "TMGR_SCHEDULING",              "Task Manager", "Scheduler",        "Assigning task to a pilot",                                                                 "client",        "tmgr_scheduling.0000.prof"
  "TMGR_STAGING_INPUT_PENDING",   "Task Manager", "Stager In queue",  "Task queued for data staging",                                                              "client",        "tmgr_scheduling.0000.prof"
  "TMGR_STAGING_INPUT",           "Task Manager", "Stager In",        "Staging task's files to the target platform (if any)",                                      "client",        "tmgr_staging_input.0000.prof"
  "AGENT_STAGING_INPUT_PENDING",  "Agent",        "Stager In queue",  "Task waiting to be picked up by Agent",                                                     "client, agent", "tmgr_staging_input.0000.prof, agent_0.prof"
  "AGENT_STAGING_INPUT",          "Agent",        "Stager In",        "Staging task's files inside the target platform, making available within the task sandbox", "agent",         "agent_staging_input.0000.prof"
  "AGENT_SCHEDULING_PENDING",     "Agent",        "Scheduler queue",  "Task waiting for scheduling on resources, i.e., cores and/or GPUs",                         "agent",         "agent_staging_input.0000.prof"
  "AGENT_SCHEDULING",             "Agent",        "Scheduler",        "Assign cores and/or GPUs to the task",                                                      "agent",         "agent_scheduling.0000.prof"
  "AGENT_EXECUTING_PENDING",      "Agent",        "Executor queue",   "Cores and/or GPUs are assigned, wait for execution",                                        "agent",         "agent_scheduling.0000.prof"
  "AGENT_EXECUTING",              "Agent",        "Executor",         "Executing tasks on assigned cores and/or GPUs. Available resources are utilized",           "agent",         "agent_executing.0000.prof"
  "AGENT_STAGING_OUTPUT_PENDING", "Agent",        "Stager Out queue", "Task completed and waits for output staging",                                               "agent",         "agent_executing.0000.prof"
  "AGENT_STAGING_OUTPUT",         "Agent",        "Stager Out",       "Staging out task files within the platform (if any)",                                       "agent",         "agent_staging_output.0000.prof"
  "TMGR_STAGING_OUTPUT_PENDING",  "Task Manager", "Stager Out queue", "Waiting for Task Manager to pick up Task again",                                            "agent",         "agent_0.prof, agent_staging_output.0000.prof"
  "TMGR_STAGING_OUTPUT",          "Task Manager", "Stager Out",       "Task's files staged from remote to local resource (if any)",                                "client",        "tmgr_staging_output.0000.prof"
  "DONE",                         "Task Manager", "",                 "Task marked as done. Final state",                                                          "client",        "tmgr_staging_output.0000.prof"
  "CANCELED",                     "Task Manager", "",                 "Task marked as cancelled. Final state",                                                     "client",        "tmgr_staging_output.0000.prof"
  "FAILED",                       "Task Manager", "",                 "Task marked as failed. Final state",                                                        "client",        "tmgr_staging_output.0000.prof"


Event Model
===========

Events marked as ``optional`` depend on the content of task descriptions etc,
all other events will usually be present in 'normal' runs. All events have an
event name, a timestamp, and a component (which recorded the event) defined -
all other fields (uid, state, msg) are optional. The names of the actual
component IDs depend on the exact RP configuration and startup sequence.

The exact order and multiplicity of events is ill-defined, as they depend on
many boundary conditions: system properties, system noise, system
synchronization, RP API call order, application timings, RP configuration,
resource configuration, and noise. However, while a global event model is thus
hard to define, the order presented in the lists below gives some basic
indication on event ordering *within each individual component*.

Format of this file
-------------------

::

   * location            : Sandbox (prof file name)
   * event_name          : semantic event description (details on 'uid', 'msg', 'state' fields) --


All Components
--------------

::

    state                 : component advances  entity state           (uid: eid, state: estate)
    lost                  : component lost   an entity (state error)   (uid: eid, state: estate)
    drop                  : component drops  an entity (final state)   (uid: eid, state: estate)
    component_init        : component child  initializes after start()
    component_init        : component parent initializes after start()
    component_final       : component finalizes

    partial orders
    * per component       : component_init, *, component_final
    * per entity          : get, advance, publish, put


Session (Component)
-------------------

::

    location              : client (rp.session.*.prof)

    session_start         : session is being created (not reconnected) (uid: sid)
    session_close         : session close is requested                 (uid: sid)
    session_stop          : session is closed                          (uid: sid)
    session_fetch_start   : start fetching logs/profs/json after close (uid: sid, [API])
    session_fetch_stop    : stops fetching logs/profs/json after close (uid: sid, [API])

    partial orders
    * per session         : session_start, session_close,  \
                            session_stop,  session_fetch_start, \
                            session_fetch_stop

PilotManager (Component)
------------------------

::

    location              : client (pmgr.0000.prof)

    setup_done            : manager has bootstrapped                   (uid: pmgr)

PMGRLaunchingComponent (Component)
----------------------------------

::

    location              : client (pmgr_launching.0000.prof)

    staging_in_start      : pilot sandbox staging starts               (uid: pilot)
    staging_in_stop       : pilot sandbox staging stops                (uid: pilot)
    submission_start      : pilot job submission starts                (uid: pilot)
    submission_stop       : pilot job submission stops                 (uid: pilot)

    partial orders
    * per pilot           : staging_in_start, staging_in_stop, \
                            submission_start, submission_stop

Pilot (in session profile, all optional)
----------------------------------------

::

    location              : client & agent (rp.session.*.prof)

    staging_in_start      : pilot level input staging request starts   (uid: pilot, msg: did, [PILOT-DS])
    staging_in_fail       : pilot level input staging request failed   (uid: pilot, msg: did, [PILOT-DS])
    staging_in_stop       : pilot level input staging request stops    (uid: pilot, msg: did, [PILOT-DS])

    staging_out_start     : pilot level output staging request starts  (uid: pilot, msg: did, [PILOT-DS])
    staging_out_fail      : pilot level output staging request failed  (uid: pilot, msg: did, [PILOT-DS])
    staging_out_stop      : pilot level output staging request stops   (uid: pilot, msg: did, [PILOT-DS])

    partial orders
    * per file            : staging_in_start,  (staging_in_fail  | staging_in_stop)
    * per file            : staging_out_start, (staging_out_fail | staging_out_stop)

TaskManager (Component)
-----------------------

::

    location              : client (tmgr.0000.prof)

    setup_done            : manager has bootstrapped                   (uid: tmgr)


TMGRSchedulingComponent (Component)
-----------------------------------

TMGRStagingInputComponent (Component)
-------------------------------------

::

    location              : client (pmgr_launching.0000.prof)

    create_sandbox_start  : create_task_sandbox starts                 (uid: task, [Task-DS])
    create_sandbox_stop   : create_task_sandbox stops                  (uid: task, [Task-DS])
    staging_in_start      : staging input request starts               (uid: task, msg: did, [Task-DS])
    staging_in_stop       : staging input request stops                (uid: task, msg: did, [Task-DS])
    staging_tar_start     : tar optimization starts                    (uid: task, msg: did, [Task-DS])
    staging_tar_stop      : tar optimization stops                     (uid: task, msg: did, [Task-DS])

    partial orders
    * per task            : create_sandbox_start, create_sandbox_stop,
                            (staging_in_start | staging_in_stop)*
    * per file            : staging_in_start, staging_in_stop

bootstrap_0.sh
--------------

::

    location              : agent (bootstrap_0.prof)

    bootstrap_0_start     : pilot bootstrapper 1 starts                (uid: pilot)
    tunnel_setup_start    : setting up tunnel    starts                (uid: pilot, [CFG-R])
    tunnel_setup_stop     : setting up tunnel    stops                 (uid: pilot, [CFG-R])
    ve_setup_start        : pilot ve setup       starts                (uid: pilot)
    ve_create_start       : pilot ve creation    starts                (uid: pilot, [CFG-R])
    ve_activate_start     : pilot ve activation  starts                (uid: pilot, [CFG-R])
    ve_activate_stop      : pilot ve activation  stops                 (uid: pilot, [CFG-R])
    ve_update_start       : pilot ve update      starts                (uid: pilot, [CFG-R])
    ve_update_stop        : pilot ve update      stops                 (uid: pilot, [CFG-R])
    ve_create_stop        : pilot ve creation    stops                 (uid: pilot, [CFG-R])
    rp_install_start      : rp stack install     starts                (uid: pilot, [CFG-R])
    rp_install_stop       : rp stack install     stops                 (uid: pilot, [CFG-R])
    ve_setup_stop         : pilot ve setup       stops                 (uid: pilot, [CFG-R])
    client_barrier_start  : wait for client signal                     (uid: pilot, [CFG-R])
    client_barrier_stop   : client signal received                     (uid: pilot, [CFG-R])
    bootstrap_0_ok        : pilot bootstrapper 1 ready                 (uid: pilot)
    sync_rel              : time sync event                            (uid: pilot, msg: 'agent_0 start')
    cleanup_start         : sandbox deletion     starts                (uid: pilot)
    cleanup_stop          : sandbox deletion     stops                 (uid: pilot)
    bootstrap_0_stop      : pilot bootstrapper 1 stops                 (uid: pilot)

    partial orders
    * as above


agent_0 (Component)
-------------------

::

    location              : agent (agent_0.prof)

    sync_rel              : sync with bootstrapper profile             (uid: pilot, msg: 'agent_0 start')
    hostname              : host or nodename for agent_0               (uid: pilot)
    cmd                   : command received from pmgr                 (uid: pilot, msg: command, [API])
    dvm_start             : DVM startup by launch method               (uid: pilot, msg: 'dvm_id=%d') [CFG-DVM])
    dvm_uri               : DVM URI is set successfully                (uid: pilot, msg: 'dvm_id=%d') [CFG-DVM])
    dvm_ready             : DVM is ready for execution                 (uid: pilot, msg: 'dvm_id=%d') [CFG-DVM])
    dvm_stop              : DVM terminated                             (uid: pilot, msg: 'dvm_id=%d') [CFG-DVM])
    dvm_fail              : DVM termination failed                     (uid: pilot, msg: 'dvm_id=%d') [CFG-DVM])


    partial orders
    * per instance        : sync_rel, hostname, (cmd | get)*
    * per instance        : dvm_start, dvm_uri, dvm_ready, (dvm_stop | dvm_fail)

AgentStagingInputComponent (Component)
--------------------------------------

::

    location              : agent (agent_staging_input.0000.prof)

    staging_in_start      : staging input request starts               (uid: task, msg: did, [Task-DS])
    staging_in_skip       : staging input request is not handled here  (uid: task, msg: did, [Task-DS])
    staging_in_fail       : staging input request failed               (uid: task, msg: did, [Task-DS])
    staging_in_stop       : staging input request stops                (uid: task, msg: did, [Task-DS])

    partial orders
    * per file            : staging_in_skip
                          | (staging_in_start, (staging_in_fail | staging_in_stop))


AgentSchedulingComponent (Component)
------------------------------------

::

    location              : agent (agent_scheduling.0000.prof)

    schedule_try          : search for task resources starts           (uid: task, [RUNTIME])
    schedule_fail         : search for task resources failed           (uid: task, [RUNTIME])
    schedule_ok           : search for task resources succeeded        (uid: task)
    unschedule_start      : task resource freeing starts               (uid: task)
    unschedule_stop       : task resource freeing stops                (uid: task)

    partial orders
    * per task            : schedule_try, schedule_fail*, schedule_ok, \
                            unschedule_start, unschedule_stop


AgentExecutingComponent: (Component)
------------------------------------

::

    location              : agent (agent_executing.0000.prof)

    task_start            : executor starts handling task              (uid: task)
    task_mkdir            : creation of sandbox requested              (uid: task, [APP])
    task_mkdir_done       : creation of sandbox completed              (uid: task, [APP])
    task_run_start        : pass to exec layer (orte, ssh, mpi...)     (uid: task)
    task_run_ok           : exec layer accepted task                   (uid: task)
    task_run_fail         : exec layer refused task                    (uid: task, [RUNTIME], optional)
    launch_start          : task launch script starts                  (uid: task)
    launch_pre            : task launch pre exec starts                (uid: task)
    launch_submit         : task launch script submit task             (uid: task)
    exec_start            : task exec script starts                    (uid: task)
    exec_pre              : task exec script pre exec starts           (uid: task)
    rank_start            : task rank starts                           (uid: task)
    app_start             : application executable started             (uid: task, [APP])
    app_*                 : application specific events                (uid: task, [APP], optional)
    app_stop              : application executable stops               (uid: task, [APP])
    rank_stop             : task rank stops                            (uid: task)
    exec_post             : task exec script post exec starts          (uid: task)
    exec_stop             : task exec script stops                     (uid: task)
    launch_collect        : task launch sees exec script complete      (uid: task)
    launch_post           : task launch script post exec starts        (uid: task)
    launch_stop           : task launch script completes               (uid: task)
    unschedule_start      : executor informs scheduler                 (uid: task)
    task_run_stop         : executor completes task handling           (uid: task)

    task_run_cancel_start : try to cancel task via exec layer (kill)  (uid: task, [API])
    task_run_cancel_stop  : did cancel    task via exec layer (kill)  (uid: task, [API])

    partial orders
    * per task            : task_start, task_mkdir, task_mkdir_done,
                            task_run_start, (task_run_ok | task_run_fail),
                            launch_start, launch_pre, launch_submit, exec_start,
                            exec_pre, rank_start, app_start, app_*, app_stop,
                            rank_stop, exec_post, exec_stop, launch_collect,
                            launch_post, launch_stop, unschedule_start,
                            task_run_stop

    * per task            : task_run_cancel_start, task_run_cancel_stop



   NOTE: raptor tasks will not log the complete set of events - they will miss
   the launch_* events (raptor has not separate launcher), and the exec_pre and
   exec_post events (pre and post exec are not supported).

AgentStagingOutputComponent (Component)
---------------------------------------

::

    location              : agent (agent_staging_output.0000.prof)

    staging_stdout_start  : reading task stdout starts                 (uid: task, [APP])
    staging_stdout_stop   : reading task stdout stops                  (uid: task, [APP])
    staging_stderr_start  : reading task stderr starts                 (uid: task, [APP])
    staging_stderr_stop   : reading task stderr stops                  (uid: task, [APP])
    staging_uprof_start   : reading task profile starts                (uid: task, [APP])
    staging_uprof_stop    : reading task profile stops                 (uid: task, [APP])
    staging_out_start     : staging outpout request starts             (uid: task, msg: did, [Task-DS])
    staging_out_skip      : staging outpout request not handled here   (uid: task, msg: did, [Task-DS])
    staging_out_fail      : staging outpout request failed             (uid: task, msg: did, [Task-DS])
    staging_out_stop      : staging outpout request stops              (uid: task, msg: did, [Task-DS])

    partial orders
    * per task            : staging_stdout_start, staging_stdout_stop,
                            staging_stderr_start, staging_stderr_stop,
                            staging_uprof_start,  staging_uprof_stop,
    * per file            : staging_out_skip \
                          | (staging_out_start, (staging_out_fail | staging_out_stop))


TMGRStagingOutputComponent (Component)
--------------------------------------

::

    location              : client (tmgr_staging_output.0000.prof)

    staging_out_start     : staging request starts                     (uid: task, msg: did, [Task-DS])
    staging_out_stop      : staging request stops                      (uid: task, msg: did, [Task-DS])

    partial orders
    * per file            : staging_out_start, staging_out_stop


All profiles
------------

::

   sync_abs            : sets an absolute, NTP synced time stamp               ([INTERNAL])


Conditional events
------------------

::

   - [API]           - only for corresponding RP API calls
   - [CFG]           - only for some RP configurations
     - [CFG-R]       - only for some bootstrapping configurations
     - [CFG-DVM]     - only for launch methods which use a DVM
   - [Task]          - only for some Task descriptions
     - [Task-DS]     - only for tasks specifying data staging directives
     - [Task-PRE]    - only for tasks specifying pre-exec directives
     - [Task-POST]   - only for tasks specifying post-exec directives
   - [PILOT]         - only for certain pilot
   - [APP]           - only for applications writing compatible profiles
   - [RUNTIME]       - only on  certain runtime decisions and system configuration
   - [INTERNAL]      - only for certain internal states


Implementing Executors
======================

Agent executors are at the heart of RADICAL-Pilot: they are responsible for
executing tasks on the target resources. The executor is a Python class that
implements the ``radical.pilot.agent.executing.Executor`` interface.

In order to add a custom executor to RADICAL-Pilot, the following steps are
required:

* Implement the executor class

::

    from .base import AgentExecutingComponent

    class MyExecutor(AgentExecutingComponent):


* Implement ``initialize`` method to set up the executor component

  Note: The ``initialize`` method is called by the agent when the executor
  component is spawned.  As for all RP components, do not rely on ``__init__``
  for initialization, but instead use ``initialize`` as that method will run in
  a separate process.

::

    def initialize(self):

        # call parent class method to set self._log
        super().initialize()


* Implement ``work`` method to execute tasks:

::

    def work(self, tasks):

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)
        ...

That method is called by the agent when tasks are ready to be executed. The task
dictionaries will have a ``slots`` structure attached which describes the
resources assigned to the task.  It is important to call the ``advance`` method
as shown above to communicate to RP (and to the application) that the task
reached the execution component.

The work method will usually not wait for task completion as that would limit
task throughput significantly.  Instead, the executor should start a separate
thread or process which watches for task completion.  Make sure that the task is
marked as completed by calling ``advance``, and to also publish an
``unschedule`` message to ensure that the resources used by the task are
released.

::

        self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)
        self.advance_tasks(task, rps.DONE, publish=True, push=False)


* Implement ``cancel`` method to cancel tasks:

::

    def cancel_task(self, uid):
        pass

Again, make sure that the new task state is communicated via a call to
``advance`` (unless the state change is picked up elsewhere).


* Register executor

The last step is to register the executor in the executor base class - look at
the ``create`` method defined there and add the task class as a new instance type.

