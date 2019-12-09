
.. _chapter_user_guide_04:

**************************
Selecting a Unit Scheduler
**************************

We have seen in the previous examples how the :class:`radical.pilot.UnitManager`
matches submitted units to pilots for execution.  On constructing the unit
manager, it can be configured to use a specific scheduling policy for that.  The
following policies are implemented:

 * `rp.SCHEDULER_ROUND_ROBIN`: alternate units between all available pilot.  This
   policy leads to a static and fair, but not necessarily load-balanced unit
   assignment.  
 * `rp.SCHEDULER_BACKFILLING`: dynamic unit scheduling based on pilot capacity and
   availability.  This is the most intelligent scheduler with good load
   balancing, but it comes with a certain scheduling overhead.

An important element to consider when discussing unit scheduling is pilot
startup time: pilot jobs can potentially sit in batch queues for a long time, or
pass quickly, depending on their size and resource usage, resource policies, etc.  
Any static assignment of units will not be able to take that into account -- and 
the first pilot may have finished all its work before a second pilot even came up.

This is what the backfilling scheduler tries to address: it only schedules units
once the pilot is available, and only as many as a pilot can execute at any
point in time.  As this requires close communication between pilot and
scheduler, that scheduler will incur a runtime overhead for each unit -- so that
is only advisable for heterogeneous workloads and/or pilot setups, and for long
running units.

:download:`04_scheduler_selection.py <../../../examples/04_scheduler_selection.py>`
shows an exemplary scheduling selector, with the following diff to the previous
multi-pilot example:

.. image:: 04_scheduler_selection_a.png

It will select `Round Robin` scheduling for two pilots, and `Backfilling` for
three or more. 


.. Running the Example
.. -------------------

.. We show here the output for 3 pilots, where one is running locally (and thus is
.. likely to come up quickly), and 2 are running exemplarily on `xsede.stampede` and
.. `epsrc.archer`, respectively, where they likely will sit in the queue for a bit.
.. We thus expect the backfilling scheduler to prefer the local pilot
.. (`pilot.0000`).

.. .. image: 04_scheduler_selection_b.png


.. What's Next?
.. ------------

Using multiple pilots is very powerful but it becomes more powerful if you allow
RP to load-balance units between them.  :ref:`chapter_user_guide_04` will show
how to do just that.

