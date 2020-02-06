
.. _chapter_user_guide_00:

***************
Getting Started
***************

In this section we walk you through the basics of using  RADICAL-Pilot
(RP).  We describe how to launch a local ``ComputePilot`` and use a 
``UnitManager`` to schedule and run ``ComputeUnits`` (i.e., tasks) on 
local and remore resources.

.. note:: The reader is assumed to be familiar with the general concepts 
          of RP, as described in :ref:`chapter_overview` for reference.

.. note:: This chapter assumes that the reader has successfully installed
          RADICAL-Pilot and configured access to the resources on which to
          execute the code examples (see chapter :ref:`chapter_installation`).

.. note:: We colloquially refer to RADICAL-Pilot as `RP`, ``ComputePilot`` as 
          `pilot`, and ``ComputeUnit`` as `unit`.

Download the basic code example :download:`00_getting_started.py
<../../../examples/00_getting_started.py>`.  The text below explains the
most important sections of that code, showing the expected output from the
execution of the example.  Please look carefully at the code comments as they
explain some aspects of the code which we do not explicitly cover in the text.

Loading the RP Module, Follow the Application Execution
-------------------------------------------------------

In order to use RADICAL-Pilot, you need to import the ``radical.pilot`` module 
in your Python script or application. Note that we use the `rp` abbreviation for 
the module name:

.. code-block:: python

    import radical.pilot as rp


All code examples of this guide use the 
`reporter <https://github.com/radical-cybertools/radical.utils/blob/devel/src/radical/utils/reporter.py>`_ 
facility of 
`RADICAL-Utils <https://github.com/radical-cybertools/radical.utils/>`_ 
to print runtime and progress information.  You can control that output with the 
``RADICAL_PILOT_VERBOSE`` variable, which can be set to the normal Python logging 
levels, and to the value ``REPORT`` to obtain well formatted output.  We assume the 
``REPORT`` setting to be used when referencing any output in this chapter.

.. code-block:: python

    os.environ['RADICAL_PILOT_VERBOSE'] = 'REPORT'

    import radical.pilot as rp
    import radical.utils as ru

    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)


Creating a Session
------------------

A :class:`radical.pilot.Session` is the root object for all other objects in
RADICAL-Pilot.  :class:`radical.pilot.PilotManager` and
:class:`radical.pilot.UnitManager` instances are always attached to a Session,
and their lifetime is controlled by the session.

A Session also encapsulates the connection(s) to a backend `MongoDB
<http://www.mongodb.org/>`_ server which facilitates the communication between
the RP application and the remote pilot jobs.  More information about how
RADICAL-Pilot uses MongoDB can be found in the :ref:`chapter_overview` section.

To create a new Session, you need to provide the URL of
a MongoDB server.  If no MongoDB URL is specified on session creation, RP
attempts to use the value specified via the ``RADICAL_PILOT_DBURL`` environment
variable.

.. code-block:: python

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://<host>:<port>/<db_name>'

    session = rp.Session()


.. warning:: Always call  :func:`radical.pilot.Session.close` before your
   application terminates to terminate all lingering pilots. You can use 
   the function argument `cleanup=True` to delete the entries of the session 
   from the database. If you need to retain thosedata, use the function 
   argument `download=True`.


Creating ComputePilots
----------------------

.. :class:`radical.pilot.ComputePilot` represents a resource overlay, i.e., a 
.. pilot, on a local or remote resource. On a cluster, each pilot can span a 
.. single node or a large number of nodes.

Pilots are created via a :class:`radical.pilot.PilotManager`, by passing
a :class:`radical.pilot.ComputePilotDescription`.  The most important elements
of the ``ComputePilotDescription`` are:

    * `resource`: a label which specifies the target resource, either local or 
      remote, on which to run the pilot, i.e., the machine on which the pilot 
      executes;
    * `cores`   : the number of CPU cores the pilot is expected to manage, i.e.,
      the size of the pilot;
    * `runtime` : the numbers of minutes the pilot is expected to be active,
      i.e., the runtime of the pilot.

Depending on the specific target resource and use case, other properties need
to be specified.  In our user guide examples, we use a separate
:download:`config.json <../../../examples/config.json>` file to store a
number of properties per resource label, to simplify the code of the examples. The
examples themselves then accept one or more resource labels, and create the
pilots on those resources:

.. code-block:: python

    # read the config
    config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))

    # use the resource specified as an argument, fall back to localhost
    try   : resource = sys.argv[1]
    except: resource = 'local.localhost'

    # create a pilot manager in the session
    pmgr = rp.PilotManager(session=session)

    # define an [n]-core pilot that runs for [x] minutes
    pdesc = rp.ComputePilotDescription({
            'resource'      : resource,
            'cores'         : 64,  # pilot size
            'runtime'       : 10,  # pilot runtime (min)
            'project'       : config[resource]['project'],
            'queue'         : config[resource]['queue'],
            'access_schema' : config[resource]['schema']
            })

    # submit the pilot for launching
    pilot = pmgr.submit_pilots(pdesc)


For a list of available resource labels, see :ref:`chapter_resources` (not all
of those resources are configured for the user guide examples).  For further
details on the pilot description, please check the :class:`API Documentation
<radical.pilot.ComputePilotDescription>`.


.. note:: Pilots terminate when calling the function 
   :func:`radical.pilot.Session.close` or :func:`radical.pilot.Pilot.cancel`. 
   The argument ``terminate=False`` of :func:`radical.pilot.Session.close` let 
   the pilot terminate at the end of its indicated duration, possibly after 
   that the Python application has exited.


Submitting ComputeUnits
-----------------------

.. After launching a pilot, you can generate
.. :class:`radical.pilot.ComputeUnit`  objects for the pilot to execute. You
.. can think of 

Each ``ComputeUnit`` is similar to an operating system process, consisting 
of an ``executable``, a list of ``arguments``, and an ``environment`` along 
with some runtime requirements.

Analogous to pilots, a unit is described via a
:class:`radical.pilot.ComputeUnitDescription` object. This object has two 
mandatory properties:

   * ``executable`` - the executable to launch
   * ``cores``      - the number of cores required by the executable

Our example creates 128 units, each running the executable `/bin/date`:

.. code-block:: python

        n    = 128   # number of units to run
        cuds = list()
        for i in range(0, n):
            # create a new CU description, and fill it.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/date'
            cuds.append(cud)


Units are executed by pilots. The :class:`radical.pilot.UnitManager`
class is responsible for routing those units from the application to the
available pilots.  The ``UnitManager`` accepts ``ComputeUnitDescriptions`` as we
created above and assigns them, according to some scheduling algorithm, to the
set of available pilots for execution (pilots are made available to a 
``UnitManager`` via the ``add_pilot`` call):

.. code-block:: python

        # create a unit manager, submit units, and wait for their completion
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)
        umgr.submit_units(cuds)
        umgr.wait_units()


Running the Example
-------------------

.. note:: Remember to set `RADICAL_PILOT_DBURL` in you environment (see chapter
          :ref:`chapter_installation`).

Running the example should result in an output similar to the one shown below:

.. image:: 00_getting_started.png

The runtime of the example can vary significantly. Typically, the first run on 
any resource for a specific user is the longest because RP requires to set up 
a Python virtualenv for the pilot.  Subsequent runs may
update that virtualenv, or may install additional components as needed, but that
should take less time than its creation.  The Virtualenv creation process should 
take few minutes on the first execution, depending on your network connectivity, 
the connectivity of the target resource, and the location of the MongoDB service.



What's Next?
------------

The next section (:ref:`chapter_user_guide_01`) describes how an
application can inspect completed units to extract information about
states, exit codes, and standard output and error.

