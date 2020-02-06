

.. _chapter_user_guide_01:

**********************
Obtaining Unit Details
**********************

The :ref:`previous chapter <chapter_user_guide_00>` discussed the basic RP
features, how to submit a pilot, and how to submit ComputeUnits to that pilot
for execution.  We will here show how an application can, after the units
complete, inspect the details of that execution.

You can download the script :download:`01_unit_details.py
<../../../examples/01_unit_details.py>`, which has the following diff to the
basic example:


.. image:: getting_started_00_01.png

You'll notice that we capture the return value of `submit_units()` in line 99,
which is in fact a list of ComputeUnit instances.  We use those instances for
inspection later on, after we waited for completion.  Inspection is also
available earlier, but may then, naturally, yield incomplete results.  A unit
will *always* have a state though, according to the state model discussed in
:ref:`chapter_overview`.


The code block below contains what most applications are interested in: unit
state, exit code, and standard output (we'll see :ref:`later <chapter_user_guide_02>` that stderr is handled equivalently):

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

Running the example will result in an output similar to the one shown below:

.. image:: 01_unit_details.png


What's Next?
------------

The next user guide section (:ref:`chapter_user_guide_02`) will describe how
failed units can be differentiated from successful ones -- although the avid
reader will already have an intuition on how that is done.

