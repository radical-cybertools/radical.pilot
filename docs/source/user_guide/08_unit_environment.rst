
.. _chapter_user_guide_08:

**********************
Setup Unit Environment
**********************

Different Applications come with different requirements to the runtime
environment.  This section will describe how the shell environment for a CU can
be configured, the next two sections will describe how to 
:ref:`configure CUs to run as MPI application <chapter_user_guide_09>` and how to
:ref:`insert arbitrary setup commands <chapter_user_guide_10>`.

The CU environment is simply configured as a Python dictionary on the unit
description, like this:

.. code-block:: python

    cud = rp.ComputeUnitDescription()

    cud.executable  = '/bin/echo'
    cud.arguments   = ['$RP_UNIT_ID greets $TEST']
    cud.environment = {'TEST' : 'jabberwocky'}

which will make the environment variable `TEST` available during CU execution.
Some other variables, such as the `RP_UNIT_ID` above, are set by RP internally
and are here used for demonstration -- but those should not be relied upon.


Running the Example
-------------------

:download:`08_unit_environment.py <../../../examples/08_unit_environment.py>`.
uses the above blob to run a bag of `echo` commands:

.. image:: 08_unit_environment.png


What's Next?
------------

:ref:`Running MPI applications <chapter_user_guide_09>`, and 
:ref:`providing more generic environment setup <chapter_user_guide_10>`, are the
topics for the next two sections.

