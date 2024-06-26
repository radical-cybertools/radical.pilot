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

.. note::

   `Binding MPI ranks to GPUs <https://docs.alcf.anl.gov/polaris/running-jobs/#binding-mpi-ranks-to-gpus>`_:
   If you want to control GPUs assignment per task, then the following code
   snippet provides an example of setting ``CUDA_VISIBLE_DEVICES`` for each MPI
   rank on Polaris:

   .. code-block:: python

      import radical.pilot as rp

      td = rp.TaskDescription()
      td.pre_exec.append('export CUDA_VISIBLE_DEVICES=$((3 - $PMI_LOCAL_RANK % 4))')
      td.gpu_type = ''  # reset GPU type, thus RP will not set "CUDA_VISIBLE_DEVICES"

Setup execution environment
===========================

Python virtual environment
--------------------------

Create a **virtual environment** with ``conda``:

.. code-block:: bash

   module use /soft/modulefiles; module load conda
   conda create -y -n ve.rp python=3.9
   conda activate ve.rp
   # OR clone base environment
   #   conda activate base
   #   conda create -y -p $HOME/ve.rp --clone $CONDA_PREFIX
   #   conda activate $HOME/ve.rp

Install RADICAL-Pilot after activating a corresponding virtual environment:

.. code-block:: bash

   conda install -c conda-forge radical.pilot

Launching script example
========================

Launching script (e.g., ``rp_launcher.sh``) for the RADICAL-Pilot application
includes setup processes to activate a certain execution environment and
launching command for the application itself.

.. code-block:: bash

   #!/bin/sh

   # - pre run -
   module use /soft/modulefiles; module load conda
   eval "$(conda shell.posix hook)"
   conda activate ve.rp

   export RADICAL_PROFILE=TRUE
   # for debugging purposes
   export RADICAL_LOG_LVL=DEBUG
   export RADICAL_REPORT=TRUE

   # - run -
   python <rp_application>

Execute launching script as ``./rp_launcher.sh`` or run it in the background:

.. code-block:: bash

   nohup ./rp_launcher.sh > OUTPUT 2>&1 </dev/null &
   # check the status of the script running:
   #   jobs -l

**Monitoring page:** https://status.alcf.anl.gov/#/polaris

=====

.. note::

   If you find any inaccuracy in this description, please, report back to us
   by opening a `ticket <https://github.com/radical-cybertools/radical.pilot/issues>`_.

