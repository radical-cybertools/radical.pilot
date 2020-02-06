

.. _chapter_user_guide_01:

**********************
Obtaining Unit Details
**********************

The :ref:`previous chapter <chapter_user_guide_00>` discussed the basic 
features of RP, how to submit a pilot, and how to submit ComputeUnits to that pilot
for execution.  Here, we show how an application can inspect the details of that 
execution, after the units complete.

You can download the script :download:`01_unit_details.py
<../../../examples/01_unit_details.py>`, which has the following diff to the
basic example:

.. image:: getting_started_00_01.png

Note that we capture the return value of `submit_units()` in line 99,
which is in fact a list of ComputeUnit instances.  We use those instances for
inspection later on, after we waited for their completion.  Inspection is also
available earlier, but may then yield incomplete results.  Note that a unit 
*always* has a state throughout its life span, according to the state model 
discussed in :ref:`chapter_overview`.

The code block below shows how to report information about unit state, exit 
code, and standard output. Later, we will :ref:`see <chapter_user_guide_02>` 
that standard error is handled equivalently):

.. code-block:: python

    report.plain('  * %s: %s, exit: %3s, out: %s\n' \
            % (unit.uid, unit.state[:4], 
                unit.exit_code, unit.stdout.strip()[:35]))

.. note::  The reporting of standard output in this manner is a convenience
    method, and cannot replace proper output file staging: the resulting string
    will be shortened on very long outputs (longer than 1kB by default), and it
    may contain information from RP which are not strictly part of the
    application stdout messages.  The proper staging of output file will be
    discussed in a :ref:`later <chapter_user_guide_06>` example.



Running the Example
-------------------

Running the example results in an output similar to the one shown below:

.. image:: 01_unit_details.png


What's Next?
------------

In the next section (:ref:`chapter_user_guide_02`), we describe how to
defferentiate between failed and successful units. The avid
reader may already have an intuition on how that is done :)

