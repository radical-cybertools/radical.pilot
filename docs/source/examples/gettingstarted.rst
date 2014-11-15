.. _chapter_example_gettinstarted:

***************
Getting Started 
***************

**This is where you should start if you are new to RADICAL-Pilot. It is highly
recommended that you carefully read and understand all of this before you go
off and start developing your own applications.**

In this chapter we explain the main components of RADICAL-Pilot and the
foundations of their function and their interplay. For your convenience, you can find a fully working example at the end of this page.

After you have worked through this chapter, you will understand how to launch
a local ComputePilot and use a UnitManager to schedule and run ComputeUnits
(tasks) on it. Throughout this chapter you will also find links to more
advanced topics like launching ComputePilots on remote HPC clusters and 
scheduling. 

.. note:: This chapter assumes that you have successfully installed RADICAL-Pilot on
          (see chapter :ref:`chapter_installation`).


Loading the Module
------------------

In order to use RADICAL-Pilot in your Python application, you need to import the
``radical.pilot`` module.

.. code-block:: python

    import radical.pilot

You can check / print the version of your RADICAL-Pilot installation via the
``version`` property.

.. code-block:: python

    print radical.pilot.version

Creating a Session
------------------

A :class:`radical.pilot.Session` is the root object for all other objects in RADICAL-
Pilot. You can think of it as a *tree* or a *directory structure* with a
Session as root. Each Session can have  zero or more
:class:`radical.pilot.Context`, :class:`radical.pilot.PilotManager` and
:class:`radical.pilot.UnitManager` attached to it.

.. code-block:: text

     (~~~~~~~~~)
     (         ) <---- [Session]
     ( MongoDB )       |
     (         )       |---- Context
     (_________)       |---- ....
                       |
                       |---- [PilotManager]
                       |     |
                       |     |---- ComputePilot
                       |     |---- ComputePilot
                       |  
                       |---- [UnitManager]
                       |     |
                       |     |---- ComputeUnit
                       |     |---- ComputeUnit
                       |     |....
                       |
                       |---- [UnitManager]
                       |     |
                       |     |....
                       |
                       |....


A Session also encapsulates the connection(s) to a back end `MongoDB
<http://www.mongodb.org/>`_ server which is the *brain* and *central nervous
system* of RADICAL-Pilot. More information about how RADICAL-Pilot uses MongoDB can
be found in the :ref:`chapter_intro` section.

To create a new Session, the only thing you need to provide is the URL of a
MongoDB server:

.. code-block:: python

    session = radical.pilot.Session(database_url="mongodb://my-mongodb-server.edu:27017")

Each Session has a unique identifier (`uid`) and methods to traverse its
members. The  Session `uid` can be used to disconnect and reconnect to a
Session as required. This  is covered in :ref:`chapter_example_disconnect_reconnect`.

.. code-block:: python

    print "UID           : %s" % session.uid
    print "Contexts      : %s" % session.list_contexts()
    print "UnitManagers  : %s" % session.list_unit_managers()
    print "PilotManagers : %s" % session.list_pilot_managers()

.. warning:: Always call  :func:`radical.pilot.Session.close` before your application 
   terminates. This will ensure that RADICAL-Pilot shuts down properly.


Creating a ComputePilot
-----------------------

A :class:`radical.pilot.ComputePilot` is responsible for ComputeUnit (task)
execution. ComputePilots can be launched either locally or remotely, on a single
machine or on one or more HPC clusters. In this example we just use local
ComputePilots, but more on remote ComputePilots and how to launch them on HPC
clusters can be found in :ref:`chapter_example_remote_and_hpc_pilots`.

As shown in the hierarchy above, ComputePilots are grouped in
:class:`radical.pilot.PilotManager` *containers*, so before you can launch a
ComputePilot, you need to add a PilotManager to your Session. Just like a
Session, a PilotManager has a unique id (`uid`) as well as a traversal method
(`list_pilots`).

.. code-block:: python

    pmgr = radical.pilot.PilotManager(session=session)
    print "PM UID        : %s" % pmgr.uid
    print "Pilots        : %s" % pmgr.list_pilots()


In order to create a new ComputePilot, you first need to describe its
requirements and properties. This is done with the help of a
:class:`radical.pilot.ComputePilotDescription` object. The mandatory properties
that you need to define are:

   * `resource` - The name (hostname) of the target system or ``localhost`` to launch a local ComputePilot.
   * `runtime` - The runtime (in minutes) of the ComputePilot agent.
   * `cores` - The number or cores the ComputePilot agent will try to allocate.

You can define and submit a 2-core local pilot that runs for 5 minutes like this:

.. code-block:: python

    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource  = "local.localhost"
    pdesc.runtime   = 5 # minutes
    pdesc.cores     = 2

A ComputePilot is launched by passing the ComputePilotDescription to the 
``submit_pilots()`` method of the PilotManager. This automatically adds the 
ComputePilot to the PilotManager. Like any other object in RADICAL-Pilot, a 
ComputePilot also has a unique identifier (``uid``)

.. code-block:: python

    pilot = pmgr.submit_pilots(pdesc)
    print "Pilot UID     : %s" % pilot.uid

.. warning:: Note that ``submit_pilots()`` is a non-blocking call and that 
   the submitted ComputePilot agent **will not terminate** when your Python
   scripts finishes. ComputePilot agents terminate only after they have 
   reached their ``runtime`` limit or if you call :func:`radical.pilot.PilotManager.cancel_pilots`
   or :func:`radical.pilot.ComputePilot.cancel`.



.. note:: You can change to the ComputePilot sandbox directory
        (``/tmp/radical.pilot.sandbox`` in the above example) to see the raw logs and output
        files of the ComputePilot agent(s) ``[pilot-<uid>]`` as well as the working
        directories and output of the individual ComputeUnits (``[task-<uid>]``).

        .. code-block:: text

            [/<sandbox-dir>/]
            |
            |----[pilot-<uid>/]
            |    |
            |    |---- STDERR
            |    |---- STDOUT
            |    |---- AGENT.LOG
            |    |---- [task-<uid>/]
            |    |---- [task-<uid>/]
            |    |....
            |
            |....

        *Knowing where to find these files might come in handy for
        debugging  purposes but it is not required for regular RADICAL-Pilot usage.*

Creating ComputeUnits (Tasks)
-----------------------------

After you have launched a ComputePilot, you can now generate a few
:class:`radical.pilot.ComputeUnit`  objects for the ComputePilot to execute. You
can think of a ComputeUnit as something very similar to an operating system
process that consists of an ``executable``, a list of ``arguments``, and an
``environment`` along with some runtime requirements.

Analogous to ComputePilots, a ComputeUnit is described via a
:class:`radical.pilot.ComputeUnitDescription` object. The mandatory properties
that you need to define are:

   * ``executable`` - The executable to launch.
   * ``arguments`` - The arguments to pass to the executable.
   * ``cores`` - The number of cores required by the executable.

For example, you can create a workload of 8 '/bin/sleep' ComputeUnits like this:

.. code-block:: python

    compute_units = []

    for unit_count in range(0, 8):
        cu = radical.pilot.ComputeUnitDescription()
        cu.environment = {"SLEEP_TIME" : "10"}
        cu.executable  = "/bin/sleep"
        cu.arguments   = ["$SLEEP_TIME"]
        cu.cores       = 1

        compute_units.append(cu)

.. note:: The example above uses a single executable that requires only one core. It is 
          however possible to run multiple commands in one ComputeUnit. This is described
          in :ref:`chapter_example_multiple_commands`. If you want to run multi-core 
          executables, like for example MPI programs, check out :ref:`chapter_example_multicore`.


Input- / Output-File Transfer
-----------------------------

Often, a computational task doesn't just consist of an executable with some 
arguments but also needs some input data. For this reason, a 
:class:`radical.pilot.ComputeUnitDescription` allows the definition of ``input_staging``
and ``output_staging``:

    * ``input_staging`` defines a list of local files that need to be transferred 
      to the execution resource before a ComputeUnit can start running. 

    * ``output_staging`` defines a list of remote files that need to be
      transferred back to the local machine after a ComputeUnit has finished
      execution. 

See  :ref:`chapter_data_staging` for more information on data staging.

Furthermore, a ComputeUnit provides two properties 
:data:`radical.pilot.ComputeUnit.stdout` and :data:`radical.pilot.ComputeUnit.stderr`
that can be used to access a ComputeUnit's STDOUT and STDERR files after it
has finished execution. 

Example: 

.. code-block:: python

      cu = radical.pilot.ComputeUnitDescription()
      cu.executable    = "/bin/cat"
      cu.arguments     = ["file1.dat", "file2.dat"]
      cu.cores         = 1
      cu.input_staging = ["./file1.dat", "./file2.dat"]


Adding Callbacks 
----------------

Events in RADICAL-Pilot are mostly asynchronous as they happen at one or more
distributed components, namely the ComputePilot agents. At any time during the 
execution of a workload, ComputePilots and ComputeUnits can begin or finish 
execution or fail with an error. 

RADICAL-Pilot provides callbacks as a method to react to these events
asynchronously when they occur. ComputePilots, PilotManagers, ComputeUnits
and UnitManagers all have a ``register_callbacks`` method:

  * :func:`radical.pilot.UnitManager.register_callback`
  * :func:`radical.pilot.PilotManager.register_callback`
  * :func:`radical.pilot.ComputePilot.register_callback`
  * :func:`radical.pilot.ComputeUnit.register_callback`

A simple callback that prints the state of all pilots would look something 
like this:

.. code-block:: python

      def pilot_state_cb(pilot, state):
          print "[Callback]: ComputePilot '%s' state changed to '%s'."% (pilot.uid, state)

      pmgr = radical.pilot.PilotManager(session=session)
      pmgr.register_callback(pilot_state_cb)


.. note:: Using callbacks can greatly improve the performance of an application
          since it eradicates the  necessity for global / blocking ``wait()`` 
          calls and state polling. More about callbacks can be read in 
          :ref:`chapter_programming_with_callbacks`.


Scheduling ComputeUnits 
-----------------------

In the previous steps we have created and launched a ComputePilot (via a
PilotManager) and created a list of ComputeUnitDescriptions. In order to put
it all together and execute the ComputeUnits on the ComputePilot, we need to
create a :class:`radical.pilot.UnitManager` instance.

As shown in the diagram below, a UnitManager combines three things: the
ComputeUnits, added via :func:`radical.pilot.UnitManager.submit_units`, one or
more ComputePilots, added via :func:`radical.pilot.UnitManager.add_pilots` and a
:ref:`chapter_schedulers`. Once instantiated, a UnitManager assigns the
submitted CUs to one of its ComputePilots based on the selected scheduling
algorithm.

.. code-block:: text

      +----+  +----+  +----+  +----+       +----+ 
      | CU |  | CU |  | CU |  | CU |  ...  | CU |
      +----+  +----+  +----+  +----+       +----+
         |       |       |       |            |
         |_______|_______|_______|____________|
                           |
                           v submit_units()
                   +---------------+
                   |  UnitManager  |
                   |---------------|
                   |               |
                   |  <SCHEDULER>  |
                   +---------------+
                           ^ add_pilots()
                           |
                 __________|___________
                 |       |            |
              +~~~~+  +~~~~+       +~~~~+  
              | CP |  | CP |  ...  | CP |
              +~~~~+  +~~~~+       +~~~~+ 

Since we have only one ComputePilot, we don't need any specific scheduling 
algorithm for our example. We choose ``SCHED_DIRECT_SUBMISSION`` which simply 
passes the ComputeUnits on to the ComputePilot.

.. code-block:: python

    umgr = radical.pilot.UnitManager(session=session, scheduler=radical.pilot.SCHED_DIRECT_SUBMISSION)

    umgr.add_pilots(pilot)
    umgr.submit_units(compute_units)

    umgr.wait_units()

The :func:`radical.pilot.UnitManager.wait_units` call blocks until all ComputeUnits have
been  executed by the UnitManager. Simple control flows / dependencies can be
realized with ``wait_units()``, however, for more complex control flows it can
become inefficient due to its blocking nature. To address this, RADICAL-Pilot also
provides mechanisms for asynchronous notifications and callbacks. This is 
discussed in more detail in :ref:`chapter_example_async`.

.. note:: The ``SCHED_DIRECT_SUBMISSION`` only works with a sinlge ComputePilot. If you add more
          than one ComputePilot to a UnitManager, you will end up with an error. If you want to
          use RADICAL-Pilot to run multiple ComputePilots concurrently, possibly on different 
          machines, check out :ref:`chapter_example_remote_and_hpc_pilots`.

Results and Inspection
----------------------

.. code-block:: python

    for unit in umgr.get_units():
        print "unit id  : %s" % unit.uid
        print "  state  : %s" % unit.state
        print "  history:" 
        for entry in unit.state_history :
            print "           %s : %s" (entry.timestamp, entry.state)

Cleanup and Shutdown
--------------------

When your application has finished executing all ComputeUnits, it should make an
attempt to cancel the ComputePilot. If a ComputePilot is not canceled, it will 
continue running until it reaches its ``runtime`` limit, even if application 
has terminated. 

An individual ComputePilot is canceled by calling :func:`radical.pilot.ComputePilot.cancel`.
Alternatively, all ComputePilots of a PilotManager can be canceled by calling 
:func:`radical.pilot.PilotManager.cancel_pilots`.

.. code-block:: python 

    pmgr.cancel_pilots()

Before your application terminates, you should always call :func:`radical.pilot.Session.close`
to ensure that your RADICAL-Pilot session terminates properly. If you haven't 
canceled the pilots before explicitly, ``close()`` will take care of that
implicitly (control it via the `terminate` parameter).  ``close()`` will also
delete all traces of the session from the database (control this with the
`cleanup` parameter). 

.. code-block:: python 

    session.close(cleanup=True, terminate=True)

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
