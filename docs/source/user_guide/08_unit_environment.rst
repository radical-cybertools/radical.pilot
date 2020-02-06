
.. _chapter_user_guide_08:

**********************
Setup Unit Environment
**********************

Different applications come with different requirements for the runtime
environment.  This section describes how the shell environment for a unit can
be configured.

The unit environment is defined via a Python dictionary, as part of the unit
description:

.. code-block:: python

    cud = rp.ComputeUnitDescription()

    cud.executable  = '/bin/echo'
    cud.arguments   = ['$RP_UNIT_ID greets $TEST']
    cud.environment = {'TEST' : 'jabberwocky'}


This makes the environment variable `TEST` available during CU execution.
Some other variables, such as the `RP_UNIT_ID` above, are set by RP internally
and are here used for demonstration.

.. -- but those should not be relied upon.


Running the Example
-------------------

:download:`08_unit_environment.py <../../../examples/08_unit_environment.py>`.
uses the code above to run a bag of `echo` commands. Here its output:

.. image:: 08_unit_environment.png


What's Next?
------------

The next section describes how to configure a unit to run
as an :ref:`MPI application <chapter_user_guide_09>`. 

.. and how to insert arbitrary setup commands :ref:`before and after
.. <chapter_user_guide_10>` the execution of a unit.
