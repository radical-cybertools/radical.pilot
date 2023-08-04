
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

A :py:class:`radical.pilot.Task` managing a :py:class:`radical.pilot.raptor.Master` instance is created using
:py:attr:`radical.pilot.TaskDescription.mode` ``rp.RAPTOR_MASTER``,
or through the :py:func:`~radical.pilot.TaskManager.submit_raptors()` method of either
:py:class:`radical.pilot.Pilot` or :py:class:`radical.pilot.TaskManager`.
The object returned to the client is a :py:class:`~radical.pilot.Task` subclass with additional features.

.. autoclass:: radical.pilot.raptor_tasks.Raptor
    :members:

Raptor
------

A :py:attr:`~radical.pilot.TaskDescription.raptor_id` refers to the
:py:attr:`~radical.pilot.Task.uid` of a :py:class:`radical.pilot.raptor_tasks.Raptor` task
managing a :py:class:`~radical.pilot.raptor.Master` instance,
which dispatches tasks to workers that it manages. See :doc:`/tutorials/raptor`.

.. autoclass:: radical.pilot.raptor.Master
   :members:
.. autoclass:: radical.pilot.raptor.Worker
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
