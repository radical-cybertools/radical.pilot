=================
Aurora (ALCF/ANL)
=================

**ACCESS IS CURRENTLY ENABLED FOR ESP and ECP TEAMS ONLY**

Platform user guide
===================

https://docs.alcf.anl.gov/aurora/getting-started-on-aurora/

General description
===================

* Resource manager - ``PBSPRO``
* Launch methods (per platform ID)

  * ``anl.aurora`` - ``MPIEXEC``

* Configuration per node (10,624 nodes in total)

  * 2 CPUs with 52 cores each, 2 threads per core (``SMT=2``)
  * 6 GPUs (Intel Data Center Max 1550 Series)
  * 512 GB of memory per CPU

.. note::

   RADICAL-Pilot provides a possibility to manage the ``-l`` option (resource
   selection qualifier) for ``PBSPRO`` and sets the default values in a
   corresponding configuration file. For the cases, when it is needed to have a
   different setup, please, follow these steps:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_anl.json <<EOF
      {
          "aurora": {
              "system_architecture": {"options": ["filesystems=grand:home",
                                                  "place=scatter"]}
          }
      }
      EOF

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load cray-python/3.9.13.1
   python3 -m venv ve.rp
   source ve.rp/bin/activate

OR create a **virtual environment** with ``conda``:

.. code-block:: bash

   module use /soft/modulefiles
   module load frameworks
   conda create -y -n ve.rp python=3.9
   conda activate ve.rp
   # OR clone base environment
   #   conda create -y -p $HOME/ve.rp --clone $CONDA_PREFIX
   #   conda activate $HOME/ve.rp

.. note::

   Using ``conda`` would require to have a different environment setup for
   RADICAL-Pilot while running on a target platform. To have it configured
   correctly, please, follow the steps below:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_anl.json <<EOF
      {
          "aurora": {
              "pre_bootstrap_0" : ["module use /soft/modulefiles",
                                   "module load frameworks"]
          }
      }
      EOF

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot
   # OR in case of conda environment
   conda install -c conda-forge radical.pilot

Launching script example
========================

Launching script (e.g., ``rp_launcher.sh``) for the RADICAL-Pilot application
includes setup processes to activate a certain execution environment and
launching command for the application itself. In this example we use virtual
environment with ``venv``.

.. code-block:: bash

   #!/bin/sh

   # - pre run -
   module load cray-python
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

