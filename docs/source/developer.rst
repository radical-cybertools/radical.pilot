
.. _chapter_developers:

***********************
Developer Documentation 
***********************

Installation from Source
========================

If you are planning to contribute to the RADICAL-Pilot codebase, or if you want 
to use the latest and greatest development features, you can download
and install RADICAL-Pilot directly from the sources.

First, you need to check out the sources from GitHub.

.. code-block:: bash

    git clone git@github.com:radical-cybertools/radical.pilot.git

Next, run the installer directly from the source directoy (assuming you have 
set up a vritualenv):

.. code-block:: bash
 
    python setup.py install


Debugging 
=========

The `RADICAL_PILOT_VERBOSE` environment variable controls the debug output of 
a RADICAL-Pilot application. Possible values are:

  * `debug`
  * `info`
  * `warning`
  * `error`


RADICAL-Pilot Architecture
==========================

Describe architecture overview here.


PilotManager and PilotManager Worker
------------------------------------

.. image:: images/architecture_pilotmanager.png

Download :download:`PDF version <images/architecture_pilotmanager.pdf>`.

UnitManager and UnitManager Worker
----------------------------------

.. image:: images/architecture_unitmanager.png

Download :download:`PDF version <images/architecture_unitmanager.pdf>`.

