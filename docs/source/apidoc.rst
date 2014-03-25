
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
   :members: Session
   :special-members: __init__

SSHCredentials
--------------
.. autoclass:: radical.pilot.SSHCredential
   :members: SSHCredential
   :special-members: __init__

Pilots and PilotManagers
========================

PilotManagers
-------------
.. autoclass:: radical.pilot.PilotManager
   :members: PilotManager
   :special-members: __init__

ComputePilotDescription
-----------------------
.. autoclass:: radical.pilot.ComputePilotDescription
   :members: ComputePilotDescription

Pilots
------
.. autoclass:: radical.pilot.ComputePilot
   :members: ComputePilot

ComputeUnits and UnitManagers
=============================

UnitManager
-----------
.. autoclass:: radical.pilot.UnitManager
   :members: UnitManager
   :special-members: __init__

ComputeUnitDescription
-----------------------
.. autoclass:: radical.pilot.ComputeUnitDescription
   :members: ComputeUnitDescription

ComputeUnit
-----------
.. autoclass:: radical.pilot.ComputeUnit
   :members: ComputeUnit

Exceptions
==========

.. autoclass:: radical.pilot.PilotException
   :members: PilotException

.. autoclass:: radical.pilot.DatabaseError
   :members: DatabaseError

