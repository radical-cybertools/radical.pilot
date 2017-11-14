
.. _chapter_user_guide_09:

****************
MPI Applications
****************

CUs which execute MPI applications are, from an RP application perspective, not
really different from other CUs -- but the application needs to communicate to
RP that the unit will (a) allocate a number of cores, and (b) needs to be
started under an MPI regime.  The respective CU description entries are shown
below:

.. code-block:: python

    cud = rp.ComputeUnitDescription()

    cud.executable  = '/bin/echo'
    cud.arguments   = ['-n', '$RP_UNIT_ID ']
    cud.cores       = 2
    cud.mpi         = True

This example should result in the unit ID echo'ed *twice*, once per MPI rank.

.. note:: Some RP configurations require MPI applications to be linked against
          a specific version of OpenMPI.  This is the case when using `orte` or
          `orte_lib` launch methods in the agent.  Please contact the mailing
          list if you need support with relinking your application.

Running the Example
-------------------

:download:`09_mpi_units.py <../../../examples/09_mpi_units.py>`.
uses the above blob to run a bag of duplicated `echo` commands:

.. image:: 09_mpi_units.png


What's Next?
------------

:ref:`Running MPI applications <chapter_user_guide_09>`, and 
:ref:providing more generic environment setup <chapter_user_guide_10>`, are the
topics for the next two sections.

