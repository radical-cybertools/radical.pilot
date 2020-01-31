
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

Security Contexts
-----------------
.. autoclass:: radical.pilot.Context
   :members: 
   :special-members: __init__

Pilots and PilotManagers
========================

PilotManagers
-------------
.. autoclass:: radical.pilot.PilotManager
   :members: 
   :special-members: __init__

ComputePilotDescription
-----------------------
.. autoclass:: radical.pilot.ComputePilotDescription
   :members: 

Pilots
------
.. autoclass:: radical.pilot.ComputePilot
   :members: 

ComputeUnits and UnitManagers
=============================

UnitManager
-----------
.. autoclass:: radical.pilot.UnitManager
   :members: 
   :special-members: __init__

ComputeUnitDescription
-----------------------
.. autoclass:: radical.pilot.ComputeUnitDescription
   :members: 

ComputeUnit
-----------
.. autoclass:: radical.pilot.ComputeUnit
   :members: 


.. comment

    State Models
    ============
    
    ComputeUnit State Model
    -----------------------
    
    .. image:: images/cu_state_model.png
    
    ComputePilot State Model
    ------------------------
    
    .. image:: images/pilot_state_model.png
    
    #. A new compute pilot is launched via :func:`radical.pilot.PilotManager.submit_pilots`
    
    #. The pilot is submitted to the remote resource and enters ``LAUNCHING`` state.
    
    #. The pilot has been succesfully launched on the remote machine and is now waiting to become ``ACTIVE``.
    
    #. The pilot has been launched by the queueing system and is now in ``ACTIVE STATE``.
    
    #. The pilot has finished execution regularly and enters ``DONE`` state.
    
    #. An error has occured during preparation for pilot launching and the pilot enters ``FAILED`` state.
    
    
    #. An error has occured during pilot launching and the pilot enters ``FAILED`` state.
    
    #. An error has occured on the backend and the pilot couldn't become active and the pilot enters ``FAILED`` state.
    
    #. An error has occured during pilot runtime and the pilot enters ``FAILED`` state.
    
    #. The active pilot has been canceled via the :func:`radical.pilot.ComputePilot.cancel` call and enters ``CANCELED`` state.
    
    
    State Model Evolution
    ----------------------
    
    The RADICAL-Pilot state model is evolving over time.  Below are the past,
    current and future Compute unit states, and their (expected) definitions and mappings:
    
    Unit states up to version 0.27:
    
    .. code-block:: python
    
          NEW                     - created in    umgr
          UNSCHEDULED             - passed  to    umgr scheduler, but not assigned to a pilot
          PENDING_INPUT_STAGING   - passed  to    staging-in, assigned to a pilot, waiting for file staging
          STAGING_INPUT           - picked  up by staging-in, transfering files (in umgr and/or agent)
          PENDING_EXECUTION       - passed  to    agent scheduler, agent waiting to assign cores
          SCHEDULING              - picked  up by agent scheduler, attempts to assign cores for execution
          EXECUTING               - picked  up by agent exec worker, cu is consuming cores
          PENDING_OUTPUT_STAGING  - passed  to    staging-out, execution done, waiting for file transfer
          STAGING_OUTPUT          - picked  up by staging-out, transferring files  (in umgr and/or agent)
          DONE                    - final
          CANCELED                - final
          FAILED                  - final
    
    Unit states in version 0.28:
    
    .. code-block:: python
    
          NEW                     - created in    umgr
          UNSCHEDULED             - passed  to    umgr scheduler, but not assigned to a pilot
          SCHEDULING              - picked  up by umgr scheduler, assigning cu to a pilot
          PENDING_INPUT_STAGING   - passed  to    staging-in, assigned to a pilot, waiting for file staging
          STAGING_INPUT           - picked  up by staging-in, transfering files (in umgr and/or agent)
          PENDING_EXECUTION       - passed  to    agent scheduler, agent waiting to assign cores
          ALLOCATING              - picked  up by agent scheduler, attempts to assign cores for execution
          EXECUTING               - picked  up by agent exec worker, cu is consuming cores
          PENDING_OUTPUT_STAGING  - passed  to    staging-out, execution done, waiting for file transfer
          STAGING_OUTPUT          - picked  up by staging-out, transferring files  (in umgr and/or agent)
          DONE                    - final
          CANCELED                - final
          FAILED                  - final
    
    Unit states after module refactoring:
    
    .. code-block:: python
    
          NEW                          - created in    umgr
          UMGR_SCHEDULING_PENDING      - passed  to    umgr  scheduler
          UMGR_SCHEDULING              - picked  up by umgr  scheduler, assigning cu to a pilot
          UMGR_STAGING_INPUT_PENDING   - passed  to    umgr  staging-in, pilot is assigned
          UMGR_STAGING_INPUT           - picked  up by umgr  staging-in, performing file staging
          AGENT_STAGING_INPUT_PENDING  - passed  to    agent staging-in
          AGENT_STAGING_INPUT          - picked  up by agent staging-in, performing file staging
          AGENT_SCHEDULING_PENDING     - passed  to    agent scheduler, agent did not assign cores
          AGENT_SCHEDULING             - picked  up by agent scheduler, attempts to assign cores for execution
          EXECUTING_PENDING            - passed  on to exec  worker, cores are assigned
          EXECUTING                    - picked  up by exec  worker, cores are 'consumed'
          AGENT_STAGING_OUTPUT_PENDING - passed  to    agent staging-out
          AGENT_STAGING_OUTPUT         - picked  up by agent staging-out, performing file staging
          UMGR_STAGING_OUTPUT_PENDING  - passed  to    umgr  staging-out
          UMGR_STAGING_OUTPUT          - picked  up by umgr  staging-out, performing file staging
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
    
