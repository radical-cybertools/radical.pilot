=============
Rivanna (UVA)
=============

Platform user guide
===================

https://www.rc.virginia.edu/userinfo/rivanna/overview/

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``uva.rivanna`` - ``SRUN``

* Configuration per node

  * CPU-only nodes (520 nodes in total)

    * 16-48 CPU cores
    * 128-1500 GB of memory

  * GPU nodes (47 nodes in total)

    * 28-128 CPU cores
    * 4-10 GPUs (A100, P100, V100, K80, RTX2080Ti, RTX3090)
    * 128-2000 GB of memory

* Available queues

  * ``standard``
  * ``parallel``
  * ``largemem``
  * ``gpu``
  * ``dev``

.. note::

   Rivanna nodes are heterogeneous and have different node configurations.
   Please refer to this `link <https://www.rc.virginia.edu/userinfo/rivanna/overview/#system-details>`_
   for more information about the resources per node.

.. note::

   If you run RADICAL-Pilot in the "interactive" mode
   (``pilot_description.access_schema = 'interactive'``), make sure that you use
   option ``--exclusive`` (`SLURM exclusive <https://slurm.schedmd.com/sbatch.html#OPT_exclusive>`_)
   in your batch script or within a command to start an interactive session.

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

   Rivanna does not provide virtual environments with ``conda``.

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

