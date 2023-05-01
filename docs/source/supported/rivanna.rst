====================
Rivanna (UVA)
====================

Platform user guide
===================

https://www.rc.virginia.edu/userinfo/user-guide/

General description
===================

* Resource manager - ``SLURM``
* Launch methods (per platform ID)

  * ``uva.rivanna*`` - ``MPIRUN``

* Configuration per node (per platform ID)

  * ``uva.standard`` (1 node)

    * 40 CPU cores (1 thread per core)
    * 375GB GiB of memory

  * ``uva.parallel`` (25 nodes)

    * 40 CPU cores (1 thread per core)
    * 375GB GiB of memory

  * ``uva.largemem`` (1 node)

    * 10 CPU cores (1 thread per core)
    * 975 GiB of memory

  * ``uva.dev`` (2 nodes)

    * 4 CPU cores (1 thread per core)
    * 36 GiB of memory

  * ``uva.gpu`` (4 nodes)

    * 10 CPU cores (1 thread per core)
    * 16 GPUs NVIDIA(RTX2080Ti, RTX3090, K80, P100, V100, and A100)
    * 375 GiB of memory

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

MongoDB service is **not** provided by NCSA, thus, you have to use either your
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
