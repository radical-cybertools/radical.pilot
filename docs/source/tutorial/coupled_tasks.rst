.. _chapter_tutorial_coupled_tasks:

*************
Coupled Tasks
*************

The script provides a simple workflow which submit a set of tasks(A) and tasks(B)
and wait until they are completed and then submits set of tasks(C). It
demonstrates synchronization mechanisms provided by the Pilot-API. This example
is useful if an executable C has dependencies on some of the output generated
from jobs A and B.

==================
Coupled Tasks Code
==================

Create a new file ``coupled_tasks.py`` and paste the following code:

.. literalinclude:: ../../../examples/tutorial/coupled_tasks.py
	:language: python

------------------------
How to Edit The Examples
------------------------

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

-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python coupled_tasks.py

The output should look something like this (based on NUMBER_JOBS=32, PILOT_SIZE=32):

.. code-block:: none


    Initializing Pilot Manager ...
    Submitting Compute Pilot to Pilot Manager ...
    ...
    All Compute Units completed successfully!
    Closed session, exiting now ...

