.. _chapter_tutorial_simple_bot:

*******************
Simple Bag-of-Tasks
*******************

You might be wondering how to create your own RADICAL-Pilot script or how
RADICAL-Pilot can be useful for your needs. Before delving into the remote job
and data submission capabilities that RADICAL-Pilot has, its important to
understand the basics. 


========================
Hands-On Job Submission
========================

The simplest usage of a pilot-job system is to submit multiple identical tasks
(a 'Bag of Tasks') collectively, i.e. as one big job! Such usage arises for example to perform
either a parameter sweep job or a set of ensemble simulation.

We will create an example which submits N jobs using RADICAL-Pilot. The jobs are
all identical, except that they each record their number in their output. This
type of run is very useful if you are running many jobs using the same
executable (but perhaps with different input files).  Rather than submit each job
individually to the queuing system and then wait for every job to become active
and complete, you submit just one container job (called a Pilot) that reserves
the number of cores needed to run all of your jobs. When this pilot becomes
active, your tasks (which are named 'Compute Units' or 'CUs') are pulled by
RADICAL-Pilot from the MongoDB server and executed. 

Download the file ``simple_bot.py`` with the following command:

.. code-block:: bash

    curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/master/examples/tutorial/simple_bot.py

------------------------
How to Edit The Examples
------------------------

Open the file ``simple_bot.py`` with your favorite editor. There is a critical sections that must be
filled in by the user: Line 101 of this file says, "BEGIN REQUIRED CU SETUP."
This section defines the actual tasks to be executed by the pilot.

.. code-block:: python

        # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
        cudesc = radical.pilot.ComputeUnitDescription()
        cudesc.executable  = "/bin/echo"
        cudesc.arguments   = ['I am CU number $CU_NO']
        cudesc.environment = {'CU_NO': i}
        cudesc.cores       = 1
        # -------- END USER DEFINED CU DESCRIPTION --------- #

Let's discuss the above example. We define our executable as "/bin/echo," the
simple UNIX command that writes arguments to standard output. Next, we need to
provide the arguments. In this case, "I am CU number $CU_NO," would correspond
to typing ``/bin/echo 'I am task number $CU_NO'`` on command line.  ``$CU_NO``
is an environment variable, so we will need to provide a value for it, as is
done on the next line: ``{'CU_NO': i}``. Note that this block of code is in
a python for loop, therefore, ``i`` corresponds to what iteration we are on.
This is not a parallel code, echo uses just one core, so we specify ``cores=1``.

-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python simple_bot.py

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
the environment variable ``RADICAL_PILOT_VERBOSE`` to a value between CRITICAL
(print only critical messages) and DEBUG (print all messages).  For lower
lavel log messages, you can use ``SAGA_VERBOSE`` in a similar way.  That will
result in a large amount of output, showing exactly how the remote interactions
are performed.

Give it a try with the above example:

.. code-block:: bash

  RADICAL_PILOT_VERBOSE=DEBUG SAGA_VERBOSE=DEBUG python simple_bot.py

