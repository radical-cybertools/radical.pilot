.. _chapter_example_gettinstarted:

***************
Getting Started 
***************

After you have successfully installed SAGA-Pilot (see chapter :ref:`chapter_installation`) on your system, let's write our first SAGA-Python application. 

[Description of what the getting started example does]

 .. note:: The following sections explain the individual components of 
           SAGA-Pilot in detail. It is highly recommended that you carefully 
           read and understand all of this. For your convenience, you can find
           a fully working example at the end of this page.

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

     (MongoDB) <---- [Session]
                     |
                     |---- SSHCredential
                     |---- ....
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
    print "Crentials     : {0} ".format( session.list_credentials() )
    print "UnitManagers  : {0} ".format( session.list_unit_managers() )
    print "PilotManagers : {0} ".format( session.list_pilot_managers() )


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


**TIP:** You change to the ComputePilot sandbox directory
(``/tmp/sagapilot.sandbox`` in the above example) to see the raw logs and output
files of the ComputePilot agent(s) ``[pilot-<uid>]`` as well as the working
directories and output of the individual ComputeUnits (``[task-<uid>]``).
*Please note that knowing  where to find these files might come in handy for
debugging  purposes but it is not required for regular SAGA-Pilot usage.*

.. code-block:: bash

    [/sandbox-dir/]
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


Creating ComputeUnits (Tasks)
-----------------------------

The Complete Example
--------------------

Putting it all together, your first SAGA-Pilot application will look somewhat 
like the script below.

.. literalinclude:: ../../../examples/getting_started.py
