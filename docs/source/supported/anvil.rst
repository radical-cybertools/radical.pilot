================
Anvil (Purdue)
================

Platform user guide
===================

https://www.rcac.purdue.edu/knowledge/anvil

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``purdue.anvil`` - ``SRUN``

* Configuration per node (per user)

  * ``debug`` queue (2 nodes)

    * 256 CPU cores
    * 257 GB of memory

  * ``gpu-debug`` queue (1 node)

    * 128 CPU cores
    * 2 GPUs
    * 515 GB of memory

  * ``wholenode`` queue (16 nodes)

    * 128 CPU cores
    * 257 GB of memory

  * ``wide`` queue (56 nodes)

    * 128 CPU cores
    * 257 GB of memory

  * ``shared`` queue (1 nodes)

    * 128 CPU cores
    * 257 GB of memory

  * ``highmem`` queue (1 nodes)

    * 128 CPU cores
    * 1031 GB of memory

  * ``gpu`` queue (max 12 GPUs per user)

    * 128 CPU cores
    * 515 GB of memory



Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load anaconda

   python3.9 -m venv ve.rp
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
   module load python
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
