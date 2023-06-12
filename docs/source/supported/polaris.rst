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

MongoDB
-------

Local installation
^^^^^^^^^^^^^^^^^^

If MongoDB was already setup and initialized then just run its instance
(see `Run MongoDB instance <#run-mongodb-instance>`_ subsection).

.. code-block:: bash

   cd $HOME
   wget https://downloads.mongodb.com/linux/mongodb-linux-x86_64-enterprise-suse15-4.4.0.tgz
   tar -zxf mongodb-linux-x86_64-enterprise-suse15-4.4.0.tgz
   mv mongodb-linux-x86_64-enterprise-suse15-4.4.0 mongo
   mkdir -p mongo/data mongo/etc mongo/var/log mongo/var/run
   touch mongo/var/log/mongodb.log

Config setup
^^^^^^^^^^^^

Description of the MongoDB setup is provided in this
`user guide <https://docs.alcf.anl.gov/theta/data-science-workflows/mongo-db/>`_,
which is the same for all ALCF platforms.

.. code-block:: bash

   cat > mongo/etc/mongodb.polaris.conf <<EOF

   processManagement:
     fork: true
     pidFilePath: $HOME/mongo/var/run/mongod.pid

   storage:
     dbPath: $HOME/mongo/data

   systemLog:
     destination: file
     path: $HOME/mongo/var/log/mongodb.log
     logAppend: true

   net:
     bindIp: 0.0.0.0
     port: 54937
   EOF

*"Each server instance of MongoDB should have a unique port number, and this
should be changed to a sensible number"*, then assigned port is
``54937``, which is a random number.

Run MongoDB instance
^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   # launch the server
   $HOME/mongo/bin/mongod -f $HOME/mongo/etc/mongodb.polaris.conf
   # shutdown the server
   $HOME/mongo/bin/mongod -f $HOME/mongo/etc/mongodb.polaris.conf --shutdown

.. warning::

   The instance of MongoDB runs on a login node. Please, make sure to terminate
   it after every run.

MongoDB initialization
^^^^^^^^^^^^^^^^^^^^^^

Initialization of the MongoDB instance should be done **ONLY** once, thus if a
corresponding instance is already running, then it means that this step was
completed.

.. code-block:: bash

   $HOME/mongo/bin/mongo --host `hostname -f` --port 54937
    > use rct_db
    > db.createUser({user: "rct", pwd: "jdWeRT634k", roles: ["readWrite"]})
    > exit

RADICAL-Pilot will connect to the MongoDB instance using the following URI.

.. code-block:: bash

   export RADICAL_PILOT_DBURL="mongodb://rct:jdWeRT634k@`hostname -f`:54937/rct_db"

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

   $HOME/mongo/bin/mongod -f $HOME/mongo/etc/mongodb.polaris.conf

   export RADICAL_PILOT_DBURL="mongodb://rct:jdWeRT634k@`hostname -f`:54937/rct_db"
   export RADICAL_PROFILE=TRUE
   # for debugging purposes
   export RADICAL_LOG_LVL=DEBUG

   # - run -
   python <rp_application>

   # - post run -
   $HOME/mongo/bin/mongod -f $HOME/mongo/etc/mongodb.polaris.conf --shutdown

Execute launching script as ``./rp_launcher.sh`` or run it in the background:

.. code-block:: bash

   nohup ./rp_launcher.sh > OUTPUT 2>&1 </dev/null &
   # check the status of the script running:
   #   jobs -l

=====

.. note::

   If you find any inaccuracy in this description, please, report back to us
   by opening a `ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.

