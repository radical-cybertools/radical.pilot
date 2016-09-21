.. _chapter_tutorial_coupled_tasks:

*************
Coupled Tasks
*************

The script is a simple workflow which submits a set of tasks A and set of tasks B
and waits until they are completed before submiting a set of tasks C. It
demonstrates synchronization mechanisms provided by the Pilot-API. This example
is useful if a task in C has dependencies on some of the output generated
from tasks in A and B.

------------
Preparation
------------

Download the file ``coupled_tasks.py`` with the following command:

.. code-block:: bash

    curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/master/examples/docs/coupled_tasks.py

Open the file ``coupled_tasks.py`` with your favorite editor. The example should 
work right out of the box on your local machine. However, if you want to try it
out with different resources, like remote HPC clusters, look for the sections 
marked: 

.. code-block:: python

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------

and change the code below accordging to the instructions in the comments.

You will need to make the necessary changes to ``coupled_tasks.py`` as you did
in the previous example.  The important difference between this file and the
previous file is that there are three separate "USER DEFINED CU DESCRIPTION"
sections - numbered 1-3. Again, these two sections will not require any
modifications for the purposes of this tutorial. We will not review every
variable again, but instead, review the relationship between the 3 task
descriptions. The three task descriptions are identical except that they each
have a different CU_SET variable assigned - either A, B, or C. 

NOTE that we call each task set the same number of times (i.e. NUMBER_JOBS) in
the tutorial code, but this is not a requirement. It just simplifies the code
for tutorial purposes. It is possible you want to run 16 A, 16 B, and then 32
C using the output from both A and B. 

In this case, the important logic to draw your attention too is around line 140:

.. code-block:: python

        print "Waiting for 'A' and 'B' CUs to complete..."
        umgr.wait_units()
        print "Executing 'C' tasks now..."

In this example, we submit both the A and B tasks to the Pilot, but instead of
running C tasks right away, we call ``wait()`` on the unit manager.  This tells
RADICAL-Pilot to wait for all of the submitted tasks to finish, before continuing in
the code. After all the A and B (submitted tasks) have finished, it then submits
the C tasks. 

----------
Execution
----------

**This assumes you have installed RADICAL-Pilot either globally or in a 
Python virtualenv. You also need access to a MongoDB server.**

Set the `RADICAL_PILOT_DBURL` environment variable in your shell to the 
MongoDB server you want to use, for example:

.. code-block:: bash
        
        export RADICAL_PILOT_DBURL=mongodb://<user>:<pass>@<mongodb_server>:27017/


If RADICAL-Pilot is installed and the MongoDB URL is set, you should be good
to run your program: 

.. code-block:: bash

    python coupled_tasks.py
