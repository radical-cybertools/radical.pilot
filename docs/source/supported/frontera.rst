===============
Frontera (TACC)
===============

Platform user guide
===================

https://frontera-portal.tacc.utexas.edu/user-guide/

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``tacc.frontera`` - ``MPIRUN``
  * ``tacc.frontera_rtx`` - ``SRUN``
  * ``tacc.frontera_prte`` - ``PRTE`` (PRRTE/PMIx)

* Configuration per node (per platform ID)

  * ``tacc.frontera``, ``tacc.frontera_prte``

    * Cascade Lake (CLX) compute nodes (8,368 nodes)

      * 56 CPU cores
      * 192 GB of memory

    * Large memory nodes (16 nodes)

      * 112 CPU cores
      * 2.1 TB of memory (NVDIMM)

  * ``tacc.frontera_rtx`` (90 nodes)

    * 32 CPU cores
    * 4 GPUs (NVIDIA RTX 5000)
    * 128 GB of memory

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment**  with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load python3
   python3 -m venv ve.rp
   source ve.rp/bin/activate

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot


Launching script example
========================

Launching script (e.g., ``rp_launcher.sh``) for the RADICAL-Pilot application
includes setup processes to activate a certain execution environment and
launching command for the application itself.

.. code-block:: bash

   #!/bin/sh

   # - pre run -
   module load python3
   source ve.rp/bin/activate

   export RADICAL_PROFILE=TRUE
   # for debugging purposes
   export RADICAL_LOG_LVL=DEBUG

   # - run -
   python <rp_application>

Execute launching script as ``./rp_launcher.sh`` or run it in the background:

.. code-block:: bash

   nohup ./rp_launcher.sh > OUTPUT 2>&1 </dev/null &
   # check the status of the script running:
   #   jobs -l

=====

.. note::

   If you find any inaccuracy in this description, please, report back to us
   by opening a `ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.

