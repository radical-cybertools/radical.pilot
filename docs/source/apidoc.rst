
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

Pilots and PilotManagers
========================

PilotManagers
-------------
.. autoclass:: radical.pilot.PilotManager
   :members:
   :special-members: __init__

PilotDescription
----------------
.. autoclass:: radical.pilot.PilotDescription
   :members:

Pilots
------
.. autoclass:: radical.pilot.Pilot
   :members:

Tasks and TaskManagers
======================

TaskManager
-----------
.. autoclass:: radical.pilot.TaskManager
   :members:
   :special-members: __init__

TaskDescription
---------------
.. autoclass:: radical.pilot.TaskDescription
   :members:

Task
----
.. autoclass:: radical.pilot.Task
   :members:

Raptor
------
.. autoclass:: radical.pilot.raptor.Master
   :members:
.. autoclass:: radical.pilot.raptor.Worker
   :members:

A `radical.pilot.Task` managing a `radical.pilot.raptor.Master` instance is created using
:py:attr:`radical.pilot.TaskDescription.mode` 
``rp.RAPTOR_MASTER``, or through :py:func:`~radical.pilot.Pilot.submit_raptors()`.
The object returned to the client is a `Task` subclass with additional features.

.. autoclass:: radical.pilot.raptor_tasks.Raptor
    :members:

Utilities and helpers
=====================
.. automodule:: radical.pilot.utils.component
   :members:
.. automodule:: radical.pilot.utils.db_utils
   :members:
.. automodule:: radical.pilot.utils.prof_utils
   :members:
.. automodule:: radical.pilot.utils.serializer
   :members:
.. automodule:: radical.pilot.utils.session
   :members:
.. automodule:: radical.pilot.utils.misc
   :members:
