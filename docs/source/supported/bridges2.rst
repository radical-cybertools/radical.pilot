=================
Bridges2 (ACCESS)
=================

Platform user guide
===================

https://www.psc.edu/resources/bridges-2/user-guide-2-2/

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``access.bridges2`` - ``MPIRUN``

* Configuration per node (per queue)

  * Regular Memory allocation:

    * ``RM`` or ``RM-512`` queues (50 nodes):

      * 128 CPU cores (1 thread per core)
      * 256 GB or 512 GB of memory respectively

    * ``RM-shared`` queue (50 nodes):

      * 128 CPU cores (1 thread per core)
      * 512 GB of memory

  * Extreme Memory allocation:

    * ``EM`` queue (100 nodes):

      * 96 CPU cores (1 thread per core)
      * 4 TB of memory

  * GPU allocation:

    * ``GPU`` queue (33 nodes):

      * 40 CPU cores (1 thread per core)
      * 8 GPUs (NVIDIA Tesla v100-32 * 32 GB)
      * 8 GPUs Nvidia (V100-16 * 16 GB)
      * 512 GB of memory

    * ``GPU-shared`` queue (1 node):

      * 48 CPU cores (1 thread per core)
      * 16 GPUs (NVIDIA V100-32 * 32 GB)
      * 1.5 TB of memory

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load python
   python3 -m venv ve.rp
   source ve.rp/bin/activate

OR create a **virtual environment** with ``conda``:

.. code-block:: bash

   module load anaconda3
   conda create --name conda.rp
   conda activate conda.rp

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot
   # OR in case of conda environment
   conda install -c conda-forge radical.pilot


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

