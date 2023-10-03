============
Delta (NCSA)
============

Platform user guide
===================

https://wiki.ncsa.illinois.edu/display/DSC/Delta+User+Guide

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
   (`Local Account Charging <https://wiki.ncsa.illinois.edu/display/DSC/Delta+User+Guide#DeltaUserGuide-LocalAccountCharging>`_).

.. note::

   Use the ``quota`` command to view your use of the file systems and use by
   your projects (`Quota Usage <https://wiki.ncsa.illinois.edu/display/DSC/Delta+User+Guide#DeltaUserGuide-quotausage>`_).

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

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot

.. note::

   Polaris does not provide virtual environments with ``conda``.


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
