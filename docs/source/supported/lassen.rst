=============
Lassen (LLNL)
=============

Platform user guide
===================

https://hpc.llnl.gov/documentation/tutorials/using-lc-s-sierra-systems

General description
===================

* Resource manager - ``LSF``
* Launch methods (per platform ID)

  * ``llnl.lassen`` - ``JSRUN``

* Configuration per node (788 nodes in total)

  * 44 CPU cores (Power9), each core has 4 hardware-threads (``SMT=4``)

    * 4 cores are blocked for users (reserved for system processes)

  * 4 GPUs (NVIDIA Tesla V100)
  * 256 GB of memory

.. note::

   Changing the number of hardware-threads per core available for an
   application could be done either with ``export RADICAL_SMT=1`` (before
   running the application) or by following the steps below:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_llnl.json <<EOF
      {
          "lassen": {
              "system_architecture": {"smt": 1}
          }
      }
      EOF

.. note::

   Lassen uses the ``-alloc_flags`` option in ``LSF`` to specify nodes
   features. RADICAL-Pilot allows to provide such features within a
   corresponding configuration file. Allowed features are: ``atsdisable``,
   ``autonumaoff``, ``cpublink``, ``ipisolate``. For example, follow the next
   steps to add some of that features:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_llnl.json <<EOF
      {
          "lassen": {
              "system_architecture": {"options": ["autonumaoff", "cpublink"]}
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
   module load python/3.8.2
   python3 -m venv ve.rp
   source ve.rp/bin/activate
   pip install -U pip setuptools wheel

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot

.. note::

   Lassen does not provide virtual environments with ``conda``.

Launching script example
========================

Launching script (e.g., ``rp_launcher.sh``) for the RADICAL-Pilot application
includes setup processes to activate a certain execution environment and
launching command for the application itself.

.. code-block:: bash

   #!/bin/sh

   # - pre run -
   module load python/3.8.2
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

