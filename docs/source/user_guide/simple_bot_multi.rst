
.. _chapter_example_simple_bag_multi_pilots:

*****************************************
Simple Bag-of-Tasks on Multiple Machines
*****************************************

This example assumes that you are familiar with submitting at least one RADICAL-Pilot
to a remote resource and moves forward explaining how to submit multiple pilots
to multiple resources.

The simplest usage of a pilot-job system is to submit multiple identical tasks
(a 'Bag of Tasks') collectively, i.e. as one big job! Such usage arises for 
example to perform either a parameter sweep job or a set of ensemble simulation.

We will create an example which submits N jobs using RADICAL-Pilot to M different
resources. The jobs are all identical, except that they each record their number and 
where they run in their output. This type of run is very useful if you are running 
many jobs using the same executable (but perhaps with different input files). 
Rather than submit each job individually to the queuing systems and then wait for 
every job to become active and complete, you submit multiple container jobs (called Pilots) 
that reserve the number of cores needed to run all of your jobs across multiple platforms. 
When this pilots become active, your tasks (which are named 'Compute Units' or 'CUs') are pulled by
RADICAL-Pilot from the MongoDB server and executed.


Launching Multiple ComputePilots
--------------------------------

You can describe multiple :class:`radical.pilot.ComputePilot` save them to a list and submit them via a :class:`radical.pilot.ComputePilotDescription` to the PilotManager:

.. code-block:: python

    pilot_list=list()

    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource  = "xsede.comet"
    pdesc.runtime   = 10
    pdesc.cores     = 12 

    pilot_list.append(pdesc)

    pdesc2 = radical.pilot.ComputePilotDescription()
    pdesc2.resource  = "xsede.gordon"
    pdesc2.runtime   = 10
    pdesc2.cores     = 16 

    pilot_list.append(pdesc2)   

    pilots = pmgr.submit_pilots(pilot_list)


.. warning:: Make sure that you have the same user name to all the resources you are submitting and add only one context to the Session


Scheduling ComputeUnits Across Multiple ComputePilots
-----------------------------------------------------
In order to be able to schedule ComputeUnits to multiple ComputePilots, you first need
to select one of the schedulers that support multi-pilot submission when you define
the  :class:`radical.pilot.UnitManager`. In our example we use the Round-Robin
scheduler.

.. code-block:: python
    umgr = rp.UnitManager (session=session,
                           scheduler=rp.SCHEDULER_ROUND_ROBIN)


------------
Preparation
------------

Before running the example, create a config file under your .ssh folder in the following manner:

..code-block:: bash
    host host1.name
        user = username_host1

    host host2.name
        user = username_host2

Download the file ``simple_bot_mult_mach.py`` with the following command:

.. code-block:: bash

    curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/master/docs/simple_bot_mult_res.py


Open the file ``simple_bot_multi_mach.py`` with your favorite editor. The example should 
work right out of the box on your local machine. However, if you want to try it
out with different resources, like remote HPC clusters, look for the sections 
marked: 

.. code-block:: python

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------

and change the code below accordging to the instructions in the comments.

---------
Execution
---------

**This assumes you have installed RADICAL-Pilot either globally or in a 
Python virtualenv. You also need access to a MongoDB server.**

Set the `RADICAL_PILOT_DBURL` environment variable in your shell to the 
MongoDB server you want to use, for example:

.. code-block:: bash
        
        export RADICAL_PILOT_DBURL=mongodb://<user>:<pass>@<mongodb_server>:27017/

If RADICAL-Pilot is installed and the MongoDB URL is set, you should be good
to run your program: 

.. code-block:: bash

    python simple_bot_multi_mach.py

The output should look something like this:

.. code-block:: none

    Initializing Pilot Manager ...
    Submitting Compute Pilots to Pilot Manager ...
    Initializing Unit Manager ...
    Registering Compute Pilots with Unit Manager ...
    Submit Compute Units to Unit Manager ...
    Waiting for CUs to complete ...
    ...
    Waiting for CUs to complete ...
    All CUs completed successfully!
    Closed session, exiting now ...


----------------------
Logging and Debugging
----------------------

Since working with distributed systems is inherently complex and much of the
complexity is hidden within RADICAL-Pilot, it is necessary to do a lot of
internal logging. By default, logging output is disabled, but if something goes
wrong or if you're just curious, you can enable the logging output by setting
the environment variable ``RADICAL_PILOT_VERBOSE`` to a value between CRITICAL
(print only critical messages) and DEBUG (print all messages).  For more details
on logging, see under 'Debugging' in chapter :ref:`chapter_developers`.

Give it a try with the above example:

.. code-block:: bash

  RADICAL_PILOT_VERBOSE=DEBUG python simple_bot.py


The Complete Example
--------------------

.. warning:: Make sure to adjust ... before you attempt to run it.

.. literalinclude:: ../../../examples/docs/simple_bot_mult_res.py

