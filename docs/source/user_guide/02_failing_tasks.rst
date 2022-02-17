
.. _chapter_user_guide_02:

********************
Handle Failing Tasks
********************

All applications can fail, often for reasons out of control of the user.
A Task is no different, it can fail as well.  Many non-trivial
application will need to have a way to handle failing tasks -- detecting the
failure is the first and necessary step to do so, and RP makes that part easy:
RP's :ref:`task state model <task_state_model>` defines that a failing task will
immediately go into `FAILED` state, and that state information is available as
`task.state` property.  

The task also has the `task.stderr` property available for further inspection
into causes of the failure -- that will only be available though if the task did
reach the `EXECUTING` state in the first place.


You can download the script :download:`02_failing_tasks.py
<../../../examples/02_failing_tasks.py>`, which demonstrates inspection for
failed tasks.  It has the following diff to the previous example:


.. image:: getting_started_01_02.png

Instead of running an executable we are almost certain will succeed, we now and
then insert an intentional faulty one whose specified executable file does not
exist on the target system.  Upon state inspection, we expect to find a `FAILED`
state for those tasks, and a respective informative `stderr` output:


Running the Example
-------------------

Running the example will result in an output similar to the one shown below:

.. image:: 02_failing_tasks.png

.. note:: You will see red glyphs during the result gathering phase, indicating
    that a failed task has been collected.  The example output above also
    demonstrates an important feature: execution ordering of tasks is *not
    preserved*, that order is independent of the order of submission.  Any task
    dependencies need to be resolved on application level!

What's Next?
------------

The next user guide section (:ref:`chapter_user_guide_03`) will return to the
basic example (i.e., no failing tasks are expected), but will now submit those
tasks to more than one concurrent pilots.

