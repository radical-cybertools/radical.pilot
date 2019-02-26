.. _chapter_tutorial_mpi_tasks:

*******************
MPI tasks
*******************

So far we have run a sequential tasks in a number of configurations.

This example introduces two new concepts: running multi-core MPI tasks
and specifying input data for the task, in this case a simple python MPI script.

------------
Preparation
------------

Download the file ``mpi_tasks.py`` with the following command:

.. code-block:: bash

    curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/master/examples/docs/mpi_tasks.py

Open the file ``mpi_tasks.py`` with your favorite editor. The example might
work right out of the box on your local machine, this depends whether you
have a local MPI installation. However, if you want to try it out with
different resources, like remote HPC clusters, look for the sections marked:

.. code-block:: python

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------

and change the code below according to the instructions in the comments.

This example makes use of an application that we first download to our own
environment and then have staged as input to the MPI tasks.

Download the file ``helloworld_mpi.py`` with the following command:

.. code-block:: bash

    curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/master/examples/helloworld_mpi.py


----------
Execution
----------

** This assumes you have installed RADICAL-Pilot either globally or in a 
Python virtualenv. You also need access to a MongoDB server.**

Set the `RADICAL_PILOT_DBURL` environment variable in your shell to the 
MongoDB server you want to use, for example:

.. code-block:: bash
        
        export RADICAL_PILOT_DBURL=mongodb://<user>:<pass>@<mongodb_server>:27017/

If RADICAL-Pilot is installed and the MongoDB URL is set, you should be good
to run your program: 

.. code-block:: bash

    python mpi_tasks.py

The output should look something like this:

.. code-block:: none

    Initializing Pilot Manager ...
    Submitting Compute Pilot to Pilot Manager ...
    Initializing Unit Manager ...
    Registering Compute Pilot with Unit Manager ...
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
the environment variable ``RADICAL_PILOT_LOG_LVL`` to a value between CRITICAL
(print only critical messages) and DEBUG (print all messages).

Give it a try with the above example:

.. code-block:: bash

  RADICAL_PILOT_LOG_LVL=DEBUG python simple_bot.py
