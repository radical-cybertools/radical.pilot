==================
Summit (OLCF/ORNL)
==================

Platform user guide
===================

https://docs.olcf.ornl.gov/systems/summit_user_guide.html

General description
===================

* Resource manager - ``LSF``
* Launch methods (per platform ID)

  * ``ornl.summit`` - ``MPIRUN``
  * ``ornl.summit_jsrun`` - ``JSRUN``
  * ``ornl.summit_prte`` - ``PRTE`` (PRRTE/PMIx)
  * ``ornl.summit_interactive`` - ``MPIRUN``, ``JSRUN``

* Configuration per node

  * Regular nodes (4,674 nodes)

    * 44 CPU cores (Power9), each core has 4 hardware-threads (``SMT=4``)

      * 2 cores are blocked for users (reserved for system processes)

    * 6 GPUs (NVIDIA Tesla V100)
    * 512 GB of memory

  * "High memory" nodes (54 nodes)

    * Basic configuration is the same as for regular nodes
    * 2 TB of memory

.. note::

   Launch method ``MPIRUN`` is able to see only one hardware-thread per core,
   thus make sure that ``SMT`` level is set to ``1`` with a corresponding
   platform ID either with ``export RADICAL_SMT=1`` (before running the
   application) or follow the steps below:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_ornl.json <<EOF
      {
          "summit": {
              "system_architecture": {"smt": 1}
          }
      }
      EOF

   Launch methods ``JSRUN`` and ``PRTE`` support the following values for the
   ``SMT`` level: ``1``, ``2``, ``4``
   (see `Hardware Threads <https://docs.olcf.ornl.gov/systems/summit_user_guide.html#hardware-threads>`_).

.. note::

   Summit uses the ``-alloc_flags`` option in ``LSF`` to specify nodes
   features (`Allocation-wide Options <https://docs.olcf.ornl.gov/systems/summit_user_guide.html#allocation-wide-options>`_).
   RADICAL-Pilot allows to provide such features within a corresponding
   configuration file. For example, follow the next steps to enable
   Multi-Process Service (`MPS <https://docs.olcf.ornl.gov/systems/summit_user_guide.html#mps>`_):

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_ornl.json <<EOF
      {
          "summit_jsrun": {
              "system_architecture": {"options": ["gpumps"]}
          }
      }
      EOF

.. note::

   Changes in the ``"system_architecture"`` parameters can be combined.

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load python/3.8-anaconda3
   python3 -m venv ve.rp
   source ve.rp/bin/activate

OR create a **virtual environment** with ``conda``:

.. code-block:: bash

   module load python/3.8-anaconda3
   conda create -y -n ve.rp python=3.9
   eval "$(conda shell.posix hook)"
   conda activate ve.rp

OR clone a ``conda`` **virtual environment** from the base environment:

.. code-block:: bash

   module load python/3.8-anaconda3
   eval "$(conda shell.posix hook)"
   conda create -y -p $HOME/ve.rp --clone $CONDA_PREFIX
   conda activate $HOME/ve.rp

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
   module load python/3.8-anaconda3
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

