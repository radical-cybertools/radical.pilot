
.. _chapter_example_getting_started:

***************
Getting Started
***************

In this chapter we will walk the reader through the most basic RP application
example.  After you have worked through this chapter, you will understand how to
launch a local ComputePilot and use a UnitManager to schedule and run
ComputeUnits (tasks) on it.

.. note:: The reader is assumed to be familiar with the general RP concepts as
          described in :ref:`chapter_overview` for reference.

.. note:: This chapter assumes that you have successfully installed
          RADICAL-Pilot, and also configured access to the resources you intent
          to use for the examples (see chapter :ref:`chapter_installation`).


Loading the Module, Follow the application execution
------------------

In order to use RADICAL-Pilot in your Python application, you need to import the
``radical.pilot`` module (we use the `rp` abbreviation for the module name).

.. code-block:: python

    import radical.pilot as rp


All example application scripts used in this user guide use the `LogReporter`
facility of RADICAL-Utils to print runtime and progress information.  You can
control that output with the `RADICAL_PILOT_VERBOSE` variable, which can be set
to the normal Python logging levels, and to the value `REPORT` to obtain well
formatted output.  We assume the `REPORT` setting to be used when referencing
any output in this chapter.

.. code-block:: python

    os.environ['RADICAL_PILOT_VERBOSE'] = 'REPORT'

    import radical.pilot as rp
    import radical.utils as ru

    report = ru.LogReporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)



Creating a Session
------------------

A :class:`radical.pilot.Session` is the root object for all other objects in
RADICAL- Pilot.  :class:`radical.pilot.PilotManager` and
:class:`radical.pilot.UnitManager` instances are always attached to a session,
and their lifetime is controlled by the session.

A Session also encapsulates the connection(s) to a backend `MongoDB
<http://www.mongodb.org/>`_ server which facilitates the communication between
the RP application and the remote pilot jobs.  More information about how
RADICAL-Pilot uses MongoDB can be found in the :ref:`chapter_intro` section.

To create a new Session, the only thing you need to provide is the URL of
a MongoDB server.  If no MongoDB URL is specified on session creation, RP
attempts to use the value specified via the `RADICAL_PILOT_DBURL` environment
variable.

.. code-block:: python

    os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://db.host.net:27017/<db_name>'

    session = rp.Session()


.. warning:: Always call  :func:`radical.pilot.Session.close` before your
   application terminates. This will terminate all lingering pilots and cleans
   out the database entries of the session.


Creating ComputePilots
----------------------

A :class:`radical.pilot.ComputePilot` is responsible for ComputeUnit execution.
ComputePilots can be launched either locally or remotely, and they can manage
a single node or a large number of nodes on a cluster.

Pilots are created via a :class:`radical.pilot.PilotManager`, by passing
a :class:`radical.pilot.ComputePilotDescription`.  The most important elements
of that description are

    * `resource`: a label which specifies the target resource to run the pilot
      on, ie. the location of the pilot;
    * `cores`   : the number of CPU cores the pilot is expected to manage, ie.
      the size of the pilot;
    * `runtime` : the numbers of minutes the pilot is expected to be active, ie.
      the runtime of the pilot.

Depending on the specific target resource and use case, other properties need to
be specified -- for more details see TODO.  In our user guide examples, we use
a separate `config.json` file to store a number of properties per resource
label, to simplify the example code.  The examples themselves then accept one or
more resource labels, and create the pilots on those resources:


.. code-block:: python

    # use the resource specified as argument, fall back to localhost
    try   : resource = sys.argv[1]
    except: resource = 'local.localhost'

    # create a pilot manage in the session
    pmgr = rp.PilotManager(session=session)

    # define an [n]-core local pilot that runs for [x] minutes
    pdesc = rp.ComputePilotDescription({
            'resource'      : resource,
            'cores'         : 64,  # pilot size
            'runtime'       : 10,  # pilot runtime (min)
            'project'       : config[resource]['project'],
            'queue'         : config[resource]['queue'],
            'access_schema' : config[resource]['schema']
            }

    # submit the pilot for launching
    pilot = pmgr.submit_pilots(pdesc)

TODO: ref resource list
TODO: ref pilot description details
TODO: ref multi pilot example

.. warning:: Note that ``submit_pilots()`` is a non-blocking call and that the
    submitted ComputePilot agent **will not terminate** when your Python scripts
    finishes. ComputePilot agents terminate only after they have reached their
    ``runtime`` limit, are killed by the target system, or if you explicitly
    cancel them via :func:`radical.pilot.Pilot.cancel`,
    :func:`radical.pilot.PilotManager.cancel_pilots`, or
    :func:`radical.pilot.Session.close(terminate=True)`, 


Creating ComputeUnits
---------------------

After you have launched a ComputePilot, you can now generate
:class:`radical.pilot.ComputeUnit`  objects for the ComputePilot to execute. You
can think of a ComputeUnit as something very similar to an operating system
process that consists of an ``executable``, a list of ``arguments``, and an
``environment`` along with some runtime requirements.

Analogous to ComputePilots, a ComputeUnit is described via a
:class:`radical.pilot.ComputeUnitDescription` object. The mandatory properties
that you need to define are:

   * ``executable`` - the executable to launch
   * ``cores``      - the number of cores required by the executable

Our basic example creates 128 ComputeUnits which each run `/bin/date`:

.. code-block:: python

        n    = 128   # number of units to run
        cuds = list()
        for i in range(0, n):
            # create a new CU description, and fill it.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/date'
            cuds.append(cud)


Submitting ComputeUnits
-----------------------

Compute units are executed by pilots -- but how does a pilot know which units to
execute?  That relation is established by the `:class:radical.pilot.UnitManager`
class which accepts ComputeUnitDescriptions as we created above, and assigns
them, according to some scheduling algorithm, to the set of available pilots.
It returns the respectively create `:class:radical.pilot.ComputeUnit` handles,
for whose completion the application can then wait:

.. code-block:: python

        # create a unit manager, submit units, and wait for their completion
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)
        umgr.submit_units(cuds)
        umgr.wait_units()


What's Next?
------------

Now that you understand the basic mechanics of RADICAL-Pilot, it's time to dive into some of the more advanced topics. We suggest that you check out the following chapters next: 

* :ref:`chapter_example_errorhandling`. Error handling is crucial for any RADICAL-Pilot application! This chapter captures everything from exception handling to state callbacks. 
* :ref:`chapter_example_remote_and_hpc_pilots`. In this chapter we explain how to launch ComputePilots on remote HPC clusters, something you most definitely want to do.
* :ref:`chapter_example_disconnect_reconnect`. This chapter is very useful for example if you work with long-running tasks that don't need continuous supervision. 

The Complete Example
--------------------

Below is a complete and working example that puts together everything we
discussed in this section. You can download the sources from :download:`here <../../../examples/getting_started_local.py>`.

.. literalinclude:: ../../../examples/getting_started_local.py
