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

  * 64 CPU cores

    * Each core has 2 threads (``SMT=2``)

  * 8 GPUs (AMD MI250X)
  * 512 GiB of memory

.. note::

   RADICAL-Pilot provides a possibility to manage the ``--constraint`` option
   (resource selection qualifier) for ``SLURM`` and sets the default values in
   a corresponding configuration file. For the cases, when it is needed to have
   a different setup, please, follow these steps:

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
   56 allocatable cores.

   If it is needed to change the core specialization and to be able to use
   all 64 cores (in this case it constrains all system processes to core 0),
   then follow the next steps:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_ornl.json <<EOF
      {
         "frontier": {
            "system_architecture" : {"blocked_cores" : []}
         }
      }
      EOF

   If it is needed to change SMT level only (``=1``), but keeping the default
   setting (8 cores for system processes), then follow the next steps:

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

   Changes in the ``"system_architecture"`` parameters can be combined.

Setup execution environment
===========================

Python virtual environment
--------------------------

**virtual environment with** ``venv`` (virtual environment with ``conda`` is
not provided by the system)

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load cray-python
   python3 -m venv ve.rp
   source ve.rp/bin/activate

Install RADICAL-Pilot after activating a corresponding virtual environment.

MongoDB
-------

MongoDB service is provided by OLCF within its infrastructure by
`Slate <https://docs.olcf.ornl.gov/services_and_applications/slate/index.html>`_,
which is built on Kubernetes and OpenShift. Please ask the RADICAL team for a
corresponding MongoDB URL.

RADICAL-Pilot will connect to the MongoDB instance using the provided URL.

.. code-block:: bash

   export RADICAL_PILOT_DBURL="<provided_mongodb_url>"

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
   with a `ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.

