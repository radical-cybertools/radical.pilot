.. _chapter_example_execution_profiling:

*******************
Execution Profiling  
*******************

Execution profiling describes the task of examining the runtime properties 
and behavior of a SAGA-Pilot session. 

.. warning:: If you want to profile your SAGA-Pilot application, make sure that you
             don't call :func:`sagapilot.Session.destroy` as this would remove all
             traces of your session from MongoDB.

Post-Mortem Profiling
---------------------

SAGA-Pilot provides a simple tool (``sagapilot-profiler``) that allows you to
create a post-mortem execution profile of a SAGA-Pilot session. For a given
Session UID, it returns a JSON data structure containing timing information,
execution locations and general properties of all ComputePilots and
ComputeUnits.  The ``sagapilot-profiler`` tool is installed automatically with
SAGA-Pilot and added to your ``PATH``. The ``--help`` flag lists the 
available command line options.

.. code-block:: text

    Usage: sagapilot-profiler -d -s [-n]

    Options:
      -h, --help            show this help message and exit

      -d URL, --mongodb-url=URL
                            specifies the url of the MongoDB database.

      -n URL, --database-name=URL
                            specifies the name of the database [default: sagapilot].

      -s SID, --session-id=SID
                            specifies the id of the session you want to inspect.

A typical invocation looks like this:

.. code-block:: bash

    sagapilot-profiler --mongodb-url=mongodb://my-mongodb-server.edu:27017 \
                  --session-id=52e94e4ff2291a294c689f53
