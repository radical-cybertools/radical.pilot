==================
Polaris (ALCF/ANL)
==================

Platform user guide
===================

https://docs.alcf.anl.gov/polaris/getting-started

General description
===================

* Resource manager - ``PBSPRO``
* Launch methods (per platform ID)

  * ``anl.polaris`` - ``MPIEXEC``

* Configuration per node (560 nodes in total)

  * 32 CPU cores, each core has 2 threads (``SMT=2``)
  * 4 GPUs (NVIDIA A100)
  * 512 GiB of memory

.. note::

   RADICAL-Pilot provides a possibility to manage the ``-l`` option (resource
   selection qualifier) for ``PBSPRO`` and sets the default values in a
   corresponding configuration file. For the cases, when it is needed to have a
   different setup, please, follow these steps:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_anl.json <<EOF
      {
          "polaris": {
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
   python3 -m venv ve.rp
   source ve.rp/bin/activate

OR create a **virtual environment** with ``conda``:

.. code-block:: bash

   module load conda; conda activate
   conda create -y -n ve.rp python=3.9
   conda activate ve.rp
   # OR clone base environment
   #   conda create -y -p $HOME/ve.rp --clone $CONDA_PREFIX
   #   conda activate $HOME/ve.rp

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
environment with ``conda``.

.. code-block:: bash

   #!/bin/sh

   # - pre run -
   module load conda
   eval "$(conda shell.posix hook)"
   conda activate ve.rp

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

