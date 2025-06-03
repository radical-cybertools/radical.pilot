==============
Discover (NASA-NCCS)
==============

Platform user guide
===================

https://www.nccs.nasa.gov/systems/discover

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``nccs.discover*`` - ``SRUN``

* Configuration per node (per platform ID)

  * ``nccs.discover`` (676 nodes for Scalable Unit 16)

    * 48 CPU cores
    * 192 GB of memory

  * ``nccs.discover_gpu_a100`` queue (12 nodes for Scalable Unit 16)

    * 48 CPU cores
    * 4 GPUs (NVIDIA A100)
    * 512 GB of memory

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   module load python/GEOSpyD/Min24.1.2-0_py3.12
   python3.12 -m venv ve.rp
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
   module load python/GEOSpyD/Min24.1.2-0_py3.12
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
