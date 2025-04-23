==============
Anvil (Purdue)
==============

Platform user guide
===================

https://www.rcac.purdue.edu/knowledge/anvil

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``purdue.anvil*`` - ``SRUN``, ``MPIRUN``

* Configuration per node (per platform ID)

  * ``purdue.anvil`` (1000 nodes)

    * 128 CPU cores
    * 257 GB of memory

  * ``purdue.anvil_mem`` (32 nodes)

    * 128 CPU cores
    * 1031 GB of memory

  * ``purdue.anvil_gpu`` queue (16 nodes)

    * 128 CPU cores
    * 4 GPUs (NVIDIA A100)
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

OR create a **virtual environment** with ``conda``:

.. code-block:: bash

   module load anaconda
   conda create -y -n ve.rp python=3.9
   conda activate ve.rp
   # OR clone base environment
   #   conda create -y -p $HOME/ve.rp --clone $CONDA_PREFIX
   #   conda activate $HOME/ve.rp


Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot
   
   # Or use conda to install radical.pilot within conda environment
   conda install -c conda-forge radical.pilot 


Launching script example
========================

Launching script (e.g., ``rp_launcher.sh``) for the RADICAL-Pilot application
includes setup processes to activate a certain execution environment and
launching command for the application itself.

.. code-block:: bash

   #!/bin/sh

   # - pre run -
   module load anaconda
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
