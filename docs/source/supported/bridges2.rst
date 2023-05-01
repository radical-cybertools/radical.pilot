====================
Bridges2 (ACCESS)
====================

Platform user guide
===================

https://www.psc.edu/resources/bridges-2/user-guide-2-2/

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``access.bridges2*`` - ``MPIRUN``

* Configuration per node (per platform ID)

  * ``access.bridges2.RM`` (50 nodes)

    * 128 CPU cores (1 thread per core)
    * 256GB GiB of memory

  * ``access.bridges2.RM-512`` (50 nodes)

    * 128 CPU cores (1 thread per core)
    * 512 GiB of memory

  * ``access.bridges2.EM`` (100 nodes)

    * 96 CPU cores (1 thread per core)
    * 4 TB of memory

  * ``access.bridges2.v100-32`` (24 nodes)

    * 128 CPU cores (1 thread per core)
    * 8 GPUs (Tesla v100-32) or 
    * 512 GiB of memory

  * ``access.bridges2.v100-16`` (9 node)

    * 128 CPU cores (1 thread per core)
    * 8 GPUs (Tesla v100-16)
    * 192 GiB of memory

Setup execution environment
===========================

Python virtual environment
--------------------------

**virtual environment with** ``venv`` (virtual environment with ``conda`` is
not provided by the system)

.. code-block:: bash

   export PYTHONNOUSERSITE=True
   module load python
   python3 -m venv ve.rp
   source ve.rp/bin/activate

Install RADICAL-Pilot after activating a corresponding virtual environment.

MongoDB
-------

MongoDB service is **not** provided by Bridges2, thus, you have to use either your
running instance of MongoDB service or contact the RADICAL team for a support.

RADICAL-Pilot will connect to the MongoDB instance using a corresponding URL.

.. code-block:: bash

   export RADICAL_PILOT_DBURL="<mongodb_url>"

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
   with a `ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.
