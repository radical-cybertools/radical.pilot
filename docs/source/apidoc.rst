
.. _chapter_api_reference:

*************
API Reference
*************

.. toctree::

Session and Managers
--------------------
.. autoclass:: radical.pilot.Session
   :members:
   :special-members: __init__

.. autoclass:: radical.pilot.PilotManager
   :members:
   :special-members: __init__

.. autoclass:: radical.pilot.TaskManager
   :members:
   :special-members: __init__

Pilot
-----
.. autoclass:: radical.pilot.PilotDescription
   :members:

.. autoclass:: radical.pilot.Pilot
   :members:

Task
----
.. autoclass:: radical.pilot.TaskDescription
   :members:

.. autoclass:: radical.pilot.Task
   :members:

Raptor
------
.. autoclass:: radical.pilot.raptor.Master
   :members:
.. autoclass:: radical.pilot.raptor.Worker
   :members:

A :py:class:`radical.pilot.Task` managing a
:py:class:`radical.pilot.raptor.Master` instance is created using
:py:attr:`radical.pilot.TaskDescription.mode`
``rp.RAPTOR_MASTER``, or through :py:func:`radical.pilot.Pilot.submit_raptors()`.
The object returned to the client is a `Task` subclass with additional features.

.. autoclass:: radical.pilot.raptor_tasks.Raptor
    :members:

Utilities and helpers
---------------------
.. autoclass:: radical.pilot.agent.scheduler.base.AgentSchedulingComponent
   :members:
.. autoclass:: radical.pilot.agent.scheduler.continuous.Continuous
   :members:
   :private-members:
.. automodule:: radical.pilot.utils.component
   :members:
.. automodule:: radical.pilot.utils.prof_utils
   :members:
.. automodule:: radical.pilot.utils.serializer
   :members:
.. automodule:: radical.pilot.utils.session
   :members:
.. automodule:: radical.pilot.utils.misc
   :members:
