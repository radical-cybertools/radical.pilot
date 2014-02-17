
.. _chapter_developers:

***********************
Developer Documentation 
***********************


Debugging 
=========

The `SAGAPILOT_VERBOSE` environment variable controls the debug output of 
a SAGA-Pilot application. Possible values are:

  * `debug`
  * `info`
  * `warning`
  * `error`

You can set `SAGAPILOT_GCDEBUG=1` to enable garbage collection debugging. If 
garbage collection debugging is enabled, all object destructors (`__del__`)
write debug messages to the log stream. 


SAGA-Pilot Architecture
=======================

Describe architecture overview here.


PilotManager and PilotManager Worker
------------------------------------

.. image:: images/architecture_pilotmanager.png

Download :download:`PDF version <images/architecture_pilotmanager.pdf>`.

UnitManager and UnitManager Worker
----------------------------------

.. image:: images/architecture_unitmanager.png

Download :download:`PDF version <images/architecture_unitmanager.pdf>`.