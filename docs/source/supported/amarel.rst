================
Amarel (Rutgers)
================

Platform user guide
===================

https://sites.google.com/view/cluster-user-guide

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``rutgers.amarel`` - ``SRUN``

* Configuration per node (per queue)

  * ``main`` queue (388 nodes)

    * 12-56 CPU cores
    * 64-256 GB of memory

  * ``gpu`` queue (38 nodes)

    * 12-52 CPU cores
    * 2-8 GPUs
    * 64-1500 GB of memory

  * ``mem`` queue (7 nodes)

    * 28-56 CPU cores
    * 512-1500 GB of memory

* Other available queues

  * ``nonpre``

    * jobs will not be preempted by higher-priority or owner jobs

  * ``graphical``

    * specialized partition for jobs submitted by the OnDemand system

  * ``cmain``

    * "main" partition for the Amarel resources located in Camden


.. note::
   In order to be able to access Amarel cluster, you must be connected to
   Rutgers Virtual Private Network (VPN) with a valid Rutgers ``netid``.
   

.. note::

   Amarel uses the ``--constraint`` option in ``SLURM`` to specify nodes
   features (`SLURM constraint <https://slurm.schedmd.com/sbatch.html#OPT_constraint>`_).
   RADICAL-Pilot allows to provide such features within a corresponding
   configuration file. For example, if you want to select nodes with "skylake"
   and "oarc" features (see the list of `Available compute hardware <https://sites.google.com/view/cluster-user-guide#h.kyrykrouyxxz>`_),
   please follow the next steps:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_rutgers.json <<EOF
      {
          "amarel": {
              "system_architecture": {"options": ["skylake", "oarc"]}
          }
      }
      EOF

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment with** ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load python
   # OR
   #   module use /projects/community/modulefiles
   #   module load python/3.9.6-gc563
   python3.9 -m venv ve.rp
   source ve.rp/bin/activate

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot

MongoDB
-------

MongoDB service is **not** provided by Amarel cluster, thus, you have to use
either your running instance of MongoDB service or contact the RADICAL team by
opening a `ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.

RADICAL-Pilot will connect to the MongoDB instance using a corresponding URI:

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
   module load python
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
