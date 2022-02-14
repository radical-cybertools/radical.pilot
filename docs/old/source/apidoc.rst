
.. _chapter_api_reference:

*************
API Reference
*************

.. toctree::

Sessions and Security Contexts
==============================

Sessions
--------
.. autoclass:: radical.pilot.Session
   :members:
   :special-members: __init__

.. py:module:: radical.pilot.context

Security Contexts
-----------------
.. autoclass:: radical.pilot.Context
   :members:
   :special-members: __init__

.. seealso:: :ref:`ssh_config`

Pilots and PilotManagers
========================

PilotManagers
-------------
.. autoclass:: radical.pilot.PilotManager
   :members:
   :special-members: __init__

PilotDescription
-----------------------
.. autoclass:: radical.pilot.PilotDescription
   :members:

Pilots
------
.. autoclass:: radical.pilot.Pilot
   :members:

Tasks and TaskManagers
=============================

TaskManager
-----------
.. autoclass:: radical.pilot.TaskManager
   :members:
   :special-members: __init__

TaskDescription
-----------------------
.. autoclass:: radical.pilot.TaskDescription
   :members:

Task
-----------
.. autoclass:: radical.pilot.Task
   :members:


.. comment

    State Models
    ============

    .. If this doc section is reactivated, it could become the target for :py:mod:`radical.pilot.states` instead of overview.rst
    .. .. py:module:: radical.pilot.states

    Task State Model
    -----------------------

    .. image:: images/cu_state_model.png

    Pilot State Model
    ------------------------

    .. image:: images/pilot_state_model.png

    #. A new pilot is launched via :func:`radical.pilot.PilotManager.submit_pilots`

    #. The pilot is submitted to the remote resource and enters ``LAUNCHING`` state.

    #. The pilot has been succesfully launched on the remote machine and is now waiting to become ``ACTIVE``.

    #. The pilot has been launched by the queueing system and is now in ``ACTIVE STATE``.

    #. The pilot has finished execution regularly and enters ``DONE`` state.

    #. An error has occured during preparation for pilot launching and the pilot enters ``FAILED`` state.


    #. An error has occured during pilot launching and the pilot enters ``FAILED`` state.

    #. An error has occured on the backend and the pilot couldn't become active and the pilot enters ``FAILED`` state.

    #. An error has occured during pilot runtime and the pilot enters ``FAILED`` state.

    #. The active pilot has been canceled via the :func:`radical.pilot.Pilot.cancel` call and enters ``CANCELED`` state.


    State Model Evolution
    ----------------------

    The RADICAL-Pilot state model is evolving over time.  Below are the past,
    current and future Task states, and their (expected) definitions and mappings:

    Task states up to version 0.27:

    .. code-block:: python

          NEW                     - created in    tmgr
          UNSCHEDULED             - passed  to    tmgr scheduler, but not assigned to a pilot
          PENDING_INPUT_STAGING   - passed  to    staging-in, assigned to a pilot, waiting for file staging
          STAGING_INPUT           - picked  up by staging-in, transfering files (in tmgr and/or agent)
          PENDING_EXECUTION       - passed  to    agent scheduler, agent waiting to assign cores
          SCHEDULING              - picked  up by agent scheduler, attempts to assign cores for execution
          EXECUTING               - picked  up by agent exec worker, cu is consuming cores
          PENDING_OUTPUT_STAGING  - passed  to    staging-out, execution done, waiting for file transfer
          STAGING_OUTPUT          - picked  up by staging-out, transferring files  (in tmgr and/or agent)
          DONE                    - final
          CANCELED                - final
          FAILED                  - final

    Task states in version 0.28:

    .. code-block:: python

          NEW                     - created in    tmgr
          UNSCHEDULED             - passed  to    tmgr scheduler, but not assigned to a pilot
          SCHEDULING              - picked  up by tmgr scheduler, assigning cu to a pilot
          PENDING_INPUT_STAGING   - passed  to    staging-in, assigned to a pilot, waiting for file staging
          STAGING_INPUT           - picked  up by staging-in, transfering files (in tmgr and/or agent)
          PENDING_EXECUTION       - passed  to    agent scheduler, agent waiting to assign cores
          ALLOCATING              - picked  up by agent scheduler, attempts to assign cores for execution
          EXECUTING               - picked  up by agent exec worker, cu is consuming cores
          PENDING_OUTPUT_STAGING  - passed  to    staging-out, execution done, waiting for file transfer
          STAGING_OUTPUT          - picked  up by staging-out, transferring files  (in tmgr and/or agent)
          DONE                    - final
          CANCELED                - final
          FAILED                  - final

    Task states after module refactoring:

    .. code-block:: python

          NEW                          - created in    tmgr
          UMGR_SCHEDULING_PENDING      - passed  to    tmgr  scheduler
          UMGR_SCHEDULING              - picked  up by tmgr  scheduler, assigning cu to a pilot
          UMGR_STAGING_INPUT_PENDING   - passed  to    tmgr  staging-in, pilot is assigned
          UMGR_STAGING_INPUT           - picked  up by tmgr  staging-in, performing file staging
          AGENT_STAGING_INPUT_PENDING  - passed  to    agent staging-in
          AGENT_STAGING_INPUT          - picked  up by agent staging-in, performing file staging
          AGENT_SCHEDULING_PENDING     - passed  to    agent scheduler, agent did not assign cores
          AGENT_SCHEDULING             - picked  up by agent scheduler, attempts to assign cores for execution
          EXECUTING_PENDING            - passed  on to exec  worker, cores are assigned
          EXECUTING                    - picked  up by exec  worker, cores are 'consumed'
          AGENT_STAGING_OUTPUT_PENDING - passed  to    agent staging-out
          AGENT_STAGING_OUTPUT         - picked  up by agent staging-out, performing file staging
          UMGR_STAGING_OUTPUT_PENDING  - passed  to    tmgr  staging-out
          UMGR_STAGING_OUTPUT          - picked  up by tmgr  staging-out, performing file staging
          DONE                         - final
          CANCELED                     - final
          FAILED                       - final


    +-------------------------+------------------------+------------------------------+
    |  `< 0.27`               | `0.28`                 | after module refactoring     |
    +=========================+========================+==============================+
    |  NEW                    | NEW                    | NEW                          |
    +-------------------------+------------------------+------------------------------+
    |  UNSCHEDULED            | UNSCHEDULED            | UMGR_SCHEDULING_PENDING      |
    +-------------------------+------------------------+------------------------------+
    |                         | SCHEDULING             | UMGR_SCHEDULING              |
    +-------------------------+------------------------+------------------------------+
    |  PENDING_INPUT_STAGING  | PENDING_INPUT_STAGING  | UMGR_STAGING_INPUT_PENDING   |
    +-------------------------+------------------------+------------------------------+
    |  STAGING_INPUT          | STAGING_INPUT          | UMGR_STAGING_INPUT           |
    +-------------------------+------------------------+------------------------------+
    |                         |                        | AGENT_STAGING_INPUT_PENDING  |
    +-------------------------+------------------------+------------------------------+
    |  STAGING_INPUT          | STAGING_INPUT          | AGENT_STAGING_INPUT          |
    +-------------------------+------------------------+------------------------------+
    |  PENDING_EXECUTION      | PENDING_EXECUTION      | AGENT_SCHEDULING_PENDING     |
    +-------------------------+------------------------+------------------------------+
    |  SCHEDULING             | ALLOCATING             | AGENT_SCHEDULING             |
    +-------------------------+------------------------+------------------------------+
    |                         |                        | EXECUTING_PENDING            |
    +-------------------------+------------------------+------------------------------+
    |  EXECUTING              | EXECUTING              | EXECUTING                    |
    +-------------------------+------------------------+------------------------------+
    |  PENDING_OUTPUT_STAGING | PENDING_OUTPUT_STAGING | AGENT_STAGING_OUTPUT_PENDING |
    +-------------------------+------------------------+------------------------------+
    |  STAGING_OUTPUT         | STAGING_OUTPUT         | AGENT_STAGING_OUTPUT         |
    +-------------------------+------------------------+------------------------------+
    |                         |                        | UMGR_STAGING_OUTPUT_PENDING  |
    +-------------------------+------------------------+------------------------------+
    |  STAGING_OUTPUT         | STAGING_OUTPUT         | UMGR_STAGING_OUTPUT          |
    +-------------------------+------------------------+------------------------------+
    |  DONE                   | DONE                   | DONE                         |
    +-------------------------+------------------------+------------------------------+
    |  CANCELED               | CANCELED               | CANCELED                     |
    +-------------------------+------------------------+------------------------------+
    |  FAILED                 | FAILED                 | FAILED                       |
    +-------------------------+------------------------+------------------------------+
