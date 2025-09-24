Luddy AI Research (LAIR) Cluster (Indiana University)
=====================================================

Platform user guide
===================

https://uisapp2.iu.edu/confluence-prd/spaces/SOICKB/pages/871925268/Luddy+AI+Research+LAIR+Cluster

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``iu.lair*`` - ``MPIRUN``

* Configuration per node (per platform ID)

  * ``iu.lair`` (general partition)

    * 64 CPU cores
    * 756 GB of memory

  * ``iu.lair_gpu_l40s`` (L40S GPU nodes, 6 nodes total)

    * 64 CPU cores
    * 8 GPUs (NVIDIA L40S)
    * 756 GB of memory

  * ``iu.lair_gpu_h100`` (H100 GPU node, 1 node total)

    * 128 CPU cores
    * 8 GPUs (NVIDIA H100)
    * 1.5 TB of memory

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True

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
   source ve.rp/bin/activate

   export RADICAL_PROFILE=TRUE
   # for debugging purposes
   export RADICAL_LOG_LVL=DEBUG
   export RADICAL_REPORT=TRUE

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
