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

  * ``uva.rivanna`` - ``SRUN``

* Configuration per node (per queue):

  * ``standard`` (1 node)

  * ``parallel`` (25 nodes)

  * ``largemem`` (1 node)

  * ``dev`` (2 nodes)

  * ``gpu`` (4 nodes)


.. note::
   Rivanna nodes are heterogeneous and have different node configurations.
   Please refer to this `link <https://www.rc.virginia.edu/userinfo/rivanna/overview/#system-details>`_
   for more information about the resources per node.


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

MongoDB service is **not** provided by UVA, thus, you have to use either your
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
   by opening a `ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.

