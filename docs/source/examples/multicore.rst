.. _chapter_example_multicore:

************************************************
Executing Multicore / Multithreaded ComputeUnits  
************************************************

Multithreaded Applications
---------------------------

MPI Applications
----------------

To define an MPI ComputeUnit, all you need to do is to set the ``cores`` and the 
``mpi`` arguments in the ComputeUnitDescription.

.. code-block:: python

    pdesc = radical.pilot.ComputeUnitDescription()
    [...]
    pdesc.mpi      = True
    pdesc.cores    = 32

.. literalinclude:: ../../../examples/running_mpi_executables.py

This example uses this simple MPI4Py example as MPI executable
 (requires MPI4Py installed on the remote cluster):

.. literalinclude:: ../../../tests/helloworld_mpi.py
