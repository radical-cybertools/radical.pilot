.. _chapter_tutorial_chained_tasks:

*************
Chained Tasks
*************

What if you had two different executables to run? What if this second set of
executables had some dependencies on data from A? Can you use one RADICAL-Pilot
to run both jobs? Yes!

The below example submits a set of echo jobs (set A) using RADICAL-Pilot, and
for every successful job (with state ``DONE``), it submits another job (set B)
to the same Pilot-Job.

We can think of this as A is comprised of subjobs {a1,a2,a3}, while B is
comprised of subjobs {b1,b2,b3}. Rather than wait for each subjob {a1},{a2},{a3}
to complete, {b1} can run as soon as {a1} is complete, or {b1} can run as soon
as a slot becomes available – i.e. {a2} could finish before {a1}.

The code below demonstrates this behavior. As soon as there is a slot available
to run a job in B (i.e. a job in A has completed), it executes the job in B.
This keeps the RADICAL-Pilot utilization high. 

==================
Chained Tasks Code
==================

Download the file ``chained_tasks.py`` with the following command:
.. code-block:: bash
    curl -O https://raw.githubusercontent.com/radical-cybertools/radical.pilot/master/examples/tutorial/chained_tasks.py

------------------------
How to Edit The Examples
------------------------

You will need to make the necessary changes to ``chained_tasks.py`` as you
did in the previous example. 

The important difference between this file and the previous file is that there
are two separate "USER DEFINED CU DESCRIPTION" sections - numbered 1 and 2.
Again, these two sections will not require any modifications for the purposes of
this tutorial. We will not review every variable again, but instead, review the
relationship between the 2 CU descriptions.

Go to line 104, "BEGIN USER DEFINED CU DESCRIPTION." This looks a lot like the
description we saw in the previous example. It is also contained in a for loop
from 0 to the NUMBER_JOBS. We are running the same executable, with almost the
same arguments, except that we append an 'A' as an additional TASK_SET variable.
If we look at line 129ff, we see that as soon as a CU in the "A" set reaches the
"Done" state, we start what is defined in "BEGIN USER DEFINED CU B DESCRIPTION"
as a "B" CU. This shows us an important feature of RADICAL-Pilot.  We can call
get_state() on a CU to find out if it is complete or not. The second CU
description is to run the same executable, /bin/echo, and print instead that it
is a B CU, with its CU number.


-------------
Run the Code
-------------

Save the file and execute it **(make sure your virtualenv is activated):**

.. code-block:: bash

    python chained_tasks.py

The output should look something like this:

.. code-block:: none

    Initializing Pilot Manager ...
    Submitting Compute Pilot to Pilot Manager ...
    ...
    All Compute Units completed successfully!
    Closed session, exiting now ...

   
