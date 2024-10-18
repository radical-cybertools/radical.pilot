============
Delta (NCSA)
============

Platform user guide
===================

https://docs.ncsa.illinois.edu/systems/delta/en/latest/

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``ncsa.delta*`` - ``SRUN``

* Configuration per node (per platform ID)

  * ``ncsa.delta`` (132 nodes)

    * 128 CPU cores, each core has 1 thread
    * 256 GiB of memory

  * ``ncsa.delta_gpu_a40`` (100 nodes)

    * 64 CPU cores, each core has 1 thread
    * 4 GPUs (NVIDIA A40)
    * 256 GiB of memory

  * ``ncsa.delta_gpu_a100_4way`` (100 nodes)

    * 64 CPU cores, each core has 1 thread
    * 4 GPUs (NVIDIA A100)
    * 256 GiB of memory

  * ``ncsa.delta_gpu_a100_8way`` (6 nodes)

    * 128 CPU cores, each core has 1 thread
    * 8 GPUs (NVIDIA A100)
    * 2,048 GiB of memory

  * ``ncsa.delta_gpu_mi100`` (1 node)

    * 128 CPU cores, each core has 1 thread
    * 8 GPUs (AMD MI100)
    * 2,048 GiB of memory

.. note::

   Use the ``accounts`` command to list the accounts available for charging
   (`Local Account Charging <https://docs.ncsa.illinois.edu/systems/delta/en/latest/user_guide/job_accounting.html#local-account-charging>`_).

.. note::

   Use the ``quota`` command to view your use of the file systems and use by
   your projects (`Quota Usage <https://docs.ncsa.illinois.edu/systems/delta/en/latest/user_guide/architecture.html#quota-usage>`_).

Setup execution environment
===========================

Python virtual environment
--------------------------

`Using Python at NCSA Delta <https://docs.ncsa.illinois.edu/systems/delta/en/latest/user_guide/software.html#python>`_

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load python/3.9.18
   python3 -m venv ve.rp
   source ve.rp/bin/activate

OR create a **virtual environment** with ``conda``:

.. code-block:: bash

   # for CPU-only nodes
   module load anaconda3_cpu
   # for GPU nodes
   #   module load anaconda3_gpu    # for CUDA
   #   module load anaconda3_mi100  # for ROCm
   conda create -y -n ve.rp python=3.9
   eval "$(conda shell.posix hook)"
   conda activate ve.rp

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
   module load python/3.9.18
   source ve.rp/bin/activate
   # OR in case of conda environment
   #   module load anaconda3_cpu
   #   eval "$(conda shell.posix hook)"
   #   conda activate ve.rp

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
