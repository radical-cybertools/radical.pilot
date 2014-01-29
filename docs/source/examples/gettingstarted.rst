.. _chapter_example_gettinstarted:

***************
Getting Started 
***************

In this chapter we explain the main components of SAGA-Pilot and the
foundations of their function and their interplay. **It is highly recommended
that you carefully read and understand all of this** before you go off and
start developing your own applications. For your convenience, you can find a
fully working example at the end of this page.

After you have worked through this chapter, you will understand how to launch
a local ComputePilot and use a UnitManager to schedule and run ComputeUnits
(task) on it. Throughout this chapter you will also find links to more
advanced topics like launching ComputePilots on remote HPC clusters and 
scheduling. 

.. note:: This chapter assumes that you have successfully installed SAGA-Pilot on
          (see chapter :ref:`chapter_installation`).


Loading the Module
------------------

In order to use SAGA-Pilot in your Python application, you need to import the
``sagapilot`` module.

.. code-block:: python

    import sagapilot

You can check / print the version of your SAGA-Pilot installation via the
``version`` property.

.. code-block:: python

    print sagapilot.version

Creating a Session
------------------

A :class:`sagapilot.Session` is the root object for all other objects in SAGA-
Pilot. You can think of it as a *tree* or a *directory structure* with a
Session as root. Each Session can have  zero or more
:class:`sagapilot.SSHCredential`, :class:`sagapilot.PilotManager` and
:class:`sagapilot.UnitManager` attached to it.

.. code-block:: bash

     (~~~~~~~~~)
     (         ) <---- [Session]
     ( MongoDB )       |
     (         )       |---- SSHCredential
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


A Session also encapsulates the connection(s) to a backend `MongoDB
<http://www.mongodb.org/>`_ server which is the *brain* and *central nervous
system* of SAGA-Pilot. More information about how SAGA-Pilot uses MongoDB can
be found in the :ref:`chapter_intro` section.

To create a new Session, the only thing you need to provide is the URL of a
MongoDB server:

.. code-block:: python

    session = sagapilot.Session(database_url="mongodb://my-mongodb-server.edu:27017")

Each Session has a unique identifier (`uid`) and methods to traverse its
members. The  Session `uid` can be used to disconnect and reconnect to a
Session as required. This  is covered in
:ref:`chapter_example_disconnect_reconnect`.

.. code-block:: python

    print "UID           : {0} ".format( session.uid )
    print "Credentials   : {0} ".format( session.list_credentials() )
    print "UnitManagers  : {0} ".format( session.list_unit_managers() )
    print "PilotManagers : {0} ".format( session.list_pilot_managers() )

.. note:: In this introduction we don't use :class:`sagapilot.SSHCredential`
   since they are not required for local ComputePilots. SSHCredentials
   are described in :ref:`chapter_example_remote_and_hpc_pilots`.


Creating a ComputePilot
-----------------------

A :class:`sagapilot.ComputePilot` is responsible for ComputeUnit (task)
execution. ComputePilots can be launched either locally or remotely, on a single
machine or on one or more HPC clusters. In this example we just use local
ComputePilots, but more on remote ComputePilots and how to launch them on HPC
clusters can be found in :ref:`chapter_example_remote_and_hpc_pilots`.

As shown in the hierarchy above, ComputePilots are grouped in
:class:`sagapilot.PilotManager` *containers*, so before you can launch a
ComputePilot, you need to add a PilotManager to your Session. Just like a
Session, a PilotManager has a unique id (`uid`) as well as a traversal method
(`list_pilots`).

.. code-block:: python

    pmgr = sagapilot.PilotManager(session=session)
    print "PM UID        : {0} ".format( pmgr.uid )
    print "Pilots        : {0} ".format( pmgr.list_pilots() )


In order to create a new ComputePilot, you first need to describe its
requirements and properties. This is done with the help of a
:class:`sagapilot.ComputePilotDescription` object. The mandatory properties
that you need to define are:

   * `resource` - The name (hostname) of the target system or ``localhost`` to launch a local ComputePilot.
   * `sandbox` - The sandbox (working directory) under which the ComputePilot agent will run.
   * `runtime` - The runtime (in minutes) of the ComputePilot agent.
   * `cores` - The number or cores the ComputePilot agent will try to allocate.

You can define and submit a 2-core local pilot that runs in
/tmp/sagapilot.sandbox for 5 minutes like this:

.. code-block:: python

    pdesc = sagapilot.ComputePilotDescription()
    pdesc.resource  = "localhost"
    pdesc.sandbox   = "/tmp/sagapilot.sandbox"
    pdesc.runtime   = 5 # minutes
    pdesc.cores     = 2

A ComputePilot is launched by passing the ComputePilotDescription to the 
``submit_pilots()`` method of the PilotManager. This automatically adds the 
ComputePilot to the PilotManager. Like any other object in SAGA-Pilot, a 
ComputePilot also has a unique identifier (``uid``)

.. code-block:: python

    pilot = pmgr.submit_pilots(pdesc)
    print "Pilot UID     : {0} ".format( pilot.uid )

.. warning:: Note that ``submit_pilots()`` is a non-blocking call and that 
   the submitted ComputePilot agent **will not terminate** when your Python
   scripts finishes. ComputePilot agents terminate only after they have 
   reached their ``runtime`` limit or if you call :func:`sagapilot.PilotManager.cancel_pilots`
   or :func:`sagapilot.ComputePilot.cancel`.



.. note:: You change to the ComputePilot sandbox directory
        (``/tmp/sagapilot.sandbox`` in the above example) to see the raw logs and output
        files of the ComputePilot agent(s) ``[pilot-<uid>]`` as well as the working
        directories and output of the individual ComputeUnits (``[task-<uid>]``).

        .. code-block:: bash

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
        debugging  purposes but it is not required for regular SAGA-Pilot usage.*


Creating ComputeUnits (Tasks)
-----------------------------

After you have launched a ComputePilot, you can now generate a few
:class:`sagapilot.ComputeUnit`  objects for the ComputePilot to execute. You
can think of a ComputeUnit as something very similar to an operating system
process that consists of an ``executable``, a list of ``arguments``, and an
``environment`` along with some runtime requirements.

Analogous to ComputePilots, a ComputeUnit is described via a
:class:`sagapilot.ComputeUnitDescription` object. The mandatory properties
that you need to define are:

   * ``executable`` - The executable to launch.
   * ``arguments`` - The arguments to pass to the executable.
   * ``cores`` - The number of cores required by the executable.

For example, you can create a workload of 8 '/bin/sleep' ComputeUnits like this:

.. code-block:: python

    compute_units = []

    for unit_count in range(0, 8):
        cu = sagapilot.ComputeUnitDescription()
        cu.environment = {"SLEEP_TIME" : "10"}
        cu.executable  = "/bin/sleep"
        cu.arguments   = ["$SLEEP_TIME"]
        cu.cores       = 1

        compute_units.append(cu)

.. note:: The example above uses a single executable that requires only one core. It is 
          however possible to run multiple commands in one ComputeUnit. This is described
          in :ref:`chapter_example_multiple_commands`. If you want to run multi-core 
          executables, like for example MPI programs, check out :ref:`chapter_example_multicore`.


Scheduling ComputeUnits 
-----------------------

In the previous steps we have created and launched a ComputePilot (via a
PilotManager) and created a list of ComputeUnitDescriptions. In order to put
it all together and execute the ComputeUnits on the ComputePilot, we need to
create a :class:`sagapilot.UnitManager` instance.

As shown in the diagram below, a UnitManager combines three things: the
ComputeUnits, added via :func:`sagapilot.UnitManager.submit_units`, one or
more ComputePilots, added via :func:`sagapilot.UnitManager.add_pilots` and a
:ref:`chapter_schedulers`. Once instantiated, a UnitManager assigns the
submitted CUs to one of its ComputePilots based on the selected scheduling
algorithm.

.. code-block:: bash

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

    umgr = sagapilot.UnitManager(session=session, scheduler=sagapilot.SCHED_DIRECT_SUBMISSION)

    umgr.add_pilots(pilot)
    umgr.submit_units(compute_units)

    umgr.wait_units()

The :func:`sagapilot.UnitManager.wait_units` call blocks until all ComputeUnits have
been  executed by the UnitManager. Simple control flows / dependencies can be
realized with ``wait_units()``, however, for more complex control flows it can
become inefficient due to its blocking nature. To address this, SAGA-Pilot also
provides mechanisms for asynchronous notifications and callbacks. This is 
discussed in more detail in :ref:`chapter_example_async`.

.. note:: The ``SCHED_DIRECT_SUBMISSION`` only works with a sinlge ComputePilot. If you add more
          than one ComputePilot to a UnitManager, you will end up with an error. If you want to
          use SAGA-Pilot to run multiple ComputePilots concurrently, possibly on different 
          machines, check out :ref:`chapter_example_remote_and_hpc_pilots`.

Results and Inspection
----------------------

.. code-block:: python

    for unit in umgr.get_units():
        print "UID: {0}, STATE: {1}, START_TIME: {2}, STOP_TIME: {3}".format(
            unit.uid, unit.state, unit.start_time, unit.stop_time)

Cleanup and Shutdown
--------------------

When your application has finished executing all ComputeUnits, it should make an
attempt to cancel the ComputePilot. If a ComputePilot is not canceled, it will 
continue running until it reaches is ``runtime`` limit, even if application 
has terminated. 

An individual ComputePilot is canceled by calling :func:`sagapilot.ComputePilot.cancel`.
Alternatively, all ComputePilots of a PilotManager can be canceled by calling 
:func:`sagapilot.PilotManager.cancel_pilots`.

.. code-block:: python 

    pmgr.cancel_pilots()

Even if the ComputePilots haven been canceled and your application has
terminated,  your Session information is kept persistently in MongoDB. Keeping
the information in the database allows you to reconnect (see
:ref:`chapter_example_disconnect_reconnect`) to your Session at a later point
in time. This can for example  be useful for
:ref:`chapter_example_execution_profiling`. However, if you don't plan to use
the Session data any further, you should call
:func:`sagapilot.Session.destroy`. This will remove it **permanently** from
the database.

.. code-block:: python

    session.destroy()

The Complete Example
--------------------

After putting it all together, your first SAGA-Pilot application will look somewhat 
like the script below.

.. literalinclude:: ../../../examples/getting_started.py
