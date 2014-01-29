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
                  --session-id=52e95c496bf88b0a0263ddc5

The result is a JSON string containing informations about ComputePilots and ComputeUnits. 

.. code-block:: JSON

    {'ComputePilots': [{'cores': 8,
                        'description': {u'Cores': 2,
                                        u'Resource': u'localhost',
                                        u'Runtime': 15,
                                        u'Sandbox': u'/tmp/sagapilot.sandbox'},
                        'id': '52e95c4a6bf88b0a0263ddc8',
                        'nodes': [u'localhost'],
                        'timing': {'scheduled': datetime.datetime(2014, 1, 29, 19, 53, 48, 60000),
                                   'started': datetime.datetime(2014, 1, 29, 19, 53, 53, 347000),
                                   'stopped': datetime.datetime(2014, 1, 29, 19, 54, 7, 770000)}}],
     'ComputeUnits': [{'description': {u'Arguments': [u'$NAP_TIME'],
                                       u'Cores': 1,
                                       u'Environment': {u'NAP_TIME': u'10'},
                                       u'Executable': u'/bin/sleep',
                                       u'Name': None,
                                       u'WorkingDirectoryPriv': None},
                       'id': '52e95c4c6bf88b0a0263ddca',
                       'locations': [u'localhost:0'],
                       'timing': {'scheduled': datetime.datetime(2014, 1, 29, 19, 53, 48, 140000),
                                  'started': datetime.datetime(2014, 1, 29, 19, 53, 54, 351000),
                                  'stopped': datetime.datetime(2014, 1, 29, 19, 54, 4, 505000)}},

                      . . . . . . . 

                      {'description': {u'Arguments': [u'$NAP_TIME'],
                                       u'Cores': 1,
                                       u'Environment': {u'NAP_TIME': u'10'},
                                       u'Executable': u'/bin/sleep',
                                       u'Name': None,
                                       u'WorkingDirectoryPriv': None},
                       'id': '52e95c4c6bf88b0a0263ddd1',
                       'locations': [u'localhost:7'],
                       'timing': {'scheduled': datetime.datetime(2014, 1, 29, 19, 53, 48, 140000),
                                  'started': datetime.datetime(2014, 1, 29, 19, 53, 54, 367000),
                                  'stopped': datetime.datetime(2014, 1, 29, 19, 54, 4, 505000)}}]}
