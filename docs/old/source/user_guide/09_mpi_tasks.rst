
.. _chapter_user_guide_09:

****************
MPI Applications
****************

From a RP perspective, tasks that execute MPI applications are analogous to
non-MPI tasks: they define an executable alongside its arguments and other
related parameters. MPI tasks have two main characteristics: (a) allocating a
specified number of cores; and (b) needing to be started under an MPI regime.
The respective task description entries are shown below:

.. code-block:: python

    cud = rp.TaskDescription()

    cud.executable  = '/bin/echo'
    cud.arguments   = ['-n', '$RP_TASK_ID ']
    cud.cores       = 2
    cud.mpi         = True

This example should result in the task ID echo'ed *twice*, once per MPI rank.

.. note:: Some RP configurations require MPI applications to be linked against
          a specific version of OpenMPI. Please 
          `open a ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_ 
          if you need support with relinking your application.

.. This is the case when using `orte` or `orte_lib` launch methods in the agent.

Running the Example
-------------------

:download:`09_mpi_tasks.py <../../../examples/09_mpi_tasks.py>`.
uses the code above to run a bag of duplicated `echo` commands.

.. note:: This example requires a working MPI installation. Without it, it will fail.

.. image:: 09_mpi_tasks.png


What's Next?
------------

The next section describes how to insert arbitrary setup commands :ref:`before
and after .. <chapter_user_guide_10>` the execution of a task.
