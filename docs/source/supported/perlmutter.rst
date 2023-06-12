==================
Perlmutter (NERSC)
==================

Platform user guide
===================

https://docs.nersc.gov/systems/perlmutter/

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``nersc.perlmutter*`` - ``SRUN``

* Configuration per node (per platform ID)

  * ``nersc.perlmutter`` (3,072 nodes)

    * 128 CPU cores, each core has 2 threads (``SMT=2``)
    * 512 GiB of memory

  * ``nersc.perlmutter_gpu`` (1,792 nodes in total)

    * 64 CPU cores, each core has 2 threads (``SMT=2``)
    * 4 GPUs (NVIDIA A100)

      * 1,536 nodes with 40 GiB of HBM per GPU
      * 256 nodes with 80 GiB of HBM per GPU

    * 256 GiB of memory

.. note::

   Perlmutter uses the ``--constraint`` option in ``SLURM`` to specify nodes
   features (`SLURM constraint <https://slurm.schedmd.com/sbatch.html#OPT_constraint>`_).
   RADICAL-Pilot allows to provide such features within a corresponding
   configuration file. For example, Perlmutter allows to request to run on up
   to 256 GPU nodes, which have 80 GiB of GPU-attached memory instead of 40 GiB
   (`Specify a constraint during resource allocation <https://docs.nersc.gov/systems/perlmutter/running-jobs/#specify-a-constraint-during-resource-allocation>`_),
   thus the corresponding configuration should be updated as following:

   .. code-block:: bash

      mkdir -p ~/.radical/pilot/configs
      cat > ~/.radical/pilot/configs/resource_nersc.json <<EOF
      {
          "perlmutter_gpu": {
              "system_architecture": {"options": ["gpu", "hbm80g"]}
          }
      }
      EOF

Setup execution environment
===========================

Python virtual environment
--------------------------

`Using Python at NERSC <https://docs.nersc.gov/development/languages/python/nersc-python/>`_

Create a **virtual environment** with ``venv``:

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load python
   python3 -m venv ve.rp
   source ve.rp/bin/activate

OR create a **virtual environment** with ``conda``:

.. code-block:: bash

   module load python
   conda create -y -n ve.rp python=3.9
   conda activate ve.rp

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   pip install radical.pilot
   # OR in case of conda environment
   conda install -c conda-forge radical.pilot

MongoDB
-------

NERSC provides `database services <https://docs.nersc.gov/services/databases/>`_,
including MongoDB. You need to fill out a form to request a database instance -
https://docs.nersc.gov/services/databases/#requesting-a-database.

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

