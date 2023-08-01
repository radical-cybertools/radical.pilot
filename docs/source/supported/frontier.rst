====================
Frontier (OLCF/ORNL)
====================

Platform user guide
===================

https://docs.olcf.ornl.gov/systems/frontier_user_guide.html

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``ornl.frontier`` - ``SRUN``

* Configuration per node (9,408 nodes in total)

  * 64 CPU cores, each core has 2 threads (``SMT=2``)
  * 8 GPUs (AMD MI250X)
  * 512 GiB of memory

.. note::

   Frontier uses the ``--constraint`` option in ``SLURM`` to specify nodes
   features (`SLURM constraint <https://slurm.schedmd.com/sbatch.html#OPT_constraint>`_).
   RADICAL-Pilot allows to provide such features within a corresponding
   configuration file. For example, follow the following steps to set ``NVMe``
   constraint (`NVMe Usage <https://docs.olcf.ornl.gov/systems/frontier_user_guide.html#nvme-usage>`_):

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_ornl.json <<EOF
      {
          "frontier": {
              "system_architecture": {"options": ["nvme"]}
          }
      }
      EOF

.. note::

   RADICAL-Pilot follows the default setting of Frontier SLURM core
   specialization, which reserves one core from each L3 cache region, leaving
   56 allocatable cores out of the available 64.

   If you need to change the core specialization to use
   all 64 cores (i.e., constraining all system processes to core 0),
   then follow these steps:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_ornl.json <<EOF
      {
         "frontier": {
            "system_architecture" : {"blocked_cores" : []}
         }
      }
      EOF

   If you need to change only the SMT level (``=1``), but keep the default
   setting (8 cores for system processes), then follow these steps:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_ornl.json <<EOF
      {
         "frontier": {
            "system_architecture" : {"smt"           : 1,
                                     "blocked_cores" : [0, 8, 16, 24, 32, 40, 48, 56]}
         }
      }
      EOF

.. note::

   Changes in the ``"system_architecture"`` parameters can be combined.

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment**  with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load cray-python
   python3 -m venv ve.rp
   source ve.rp/bin/activate

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot
   
.. note::

   Frontier does not provide virtual environments with ``conda``.

MongoDB
-------

OLCF provides a MongoDB service via 
`Slate <https://docs.olcf.ornl.gov/services_and_applications/slate/index.html>`_,
an infrastructure built on Kubernetes and OpenShift. Please ask the RADICAL team for a
corresponding MongoDB URI by opening a 
`ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.

RADICAL-Pilot will connect to the MongoDB instance using the provided URI.

.. code-block:: bash

   export RADICAL_PILOT_DBURL="<mongodb_uri>"

Launching script example
========================

Launching script (e.g., ``rp_launcher.sh``) for the RADICAL-Pilot application
includes setup processes to activate a certain execution environment and
launching command for the application itself.

.. code-block:: bash

   #!/bin/sh

   # - pre run -
   module load cray-python
   source ve.rp/bin/activate

   export RADICAL_PILOT_DBURL="mongodb://localhost:27017/"
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

