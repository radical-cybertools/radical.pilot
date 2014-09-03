
.. _chapter_machconf:

***********************
Resource Configurations
***********************

Preconfigured Resources
=======================

We maintain a growing set of ready to use resource configuration files.

.. include:: ./resources.rst  

Introduction
============

In order to keep RADICAL-Pilot applications free from clutter and 
machine-specific parameters and constants, RADICAL-Pilot uses 
resource configuration files.  Those are installed along with RADICAL-Pilot.

A resource configuration uses the JSON format and has the following layout::

    {
        "futuregrid.india": {
            ...
        },

        "futuregrid.sierra": {
            ...
        }
    }

In the example above, `futuregrid.india` and `futuregrid.sierra` are the
**resource keys**. Resource keys are referenced in
:class:`radical.pilot.ComputePilotDescription` to create a
:class:`radical.pilot.ComputePilot` for a given machine::

    pd = radical.pilot.ComputePilotDescription()
    pd.resource = "futuregrid.india"  # Key defined in futuregrid.json
    pd.cores    = 16
    pilot_india = pm.submit_pilots(pd)


Writing a Custom Resource Configuration File
============================================

If you want to use RADICAL-Pilot with a resource that is not in any of the provided 
configuration files, you can write your own.

A configuration file has to be valid JSON. The structure is as follows:

.. code-block:: python

    {
        "RESOURCE_KEY_NAME": {
            "remote_job_manager_endpoint" : "slurm+ssh://stampede.tacc.utexas.edu",
            "remote_filesystem_endpoint"  : "sftp://stampede.tacc.utexas.edu/",
            "local_job_manager_endpoint"  : "slurm://localhost",
            "local_filesystem_endpoint"   : "file://localhost/",
            "default_queue"               : "normal",
            "lrms"                        : "SLURM",
            "task_launch_method"          : "SSH",
            "mpi_launch_method"           : "IBRUN",
            "python_interpreter"          : "/opt/apps/python/epd/7.3.2/bin/python",
            "pre_bootstrap"               : ["module purge", "module load TACC", "module load cluster", "module load Linux", "module load mvapich2", "module load python/2.7.3-epd-7.3.2"],
            "valid_roots"                 : ["/home1", "/scratch", "/work"],
            "pilot_agent"                 : "radical-pilot-agent-multicore.py"
        },
        "ANOTHER_KEY_NAME": ...
    }

`RESOURCE_KEY_NAME` is the string which is used as value for 
`ComputePilotDescription.resource` to reference this entry. They have to be 
unique. 

All fields are mandatory, unless indicated otherwise below.

* `remote_job_manager_endpoint` : access url for pilot submission (interpreted by SAGA)
* `remote_filesystem_endpoint`  : access url for file staging (interpreted by SAGA)
* `local_job_manager_endpoint`  : as above when running on the target host
* `local_filesystem_endpoint`   : as above when running on the target host
* `default_queue`               : queue to use for pilot submission (optional)
* `lrms`                        : type of job management system (`LOADL`, `LSF`, `PBSPRO`, `SGE`, `SLURM`, `TORQUE`, `FORK`)
* `task_launch_method`          : type of compute node access (required for non-MPI units: `SSH`,`APRUN` or `LOCAL`)
* `mpi_launch_method`           : type of MPI support (required for MPI units: `MPIRUN`, `MPIEXEC`, `APRUN`, `IBRUN` or `POE`)
* `python_interpreter`          : path to python (optional)
* `pre_bootstrap`               : list of commands to execute for initialization (optional)
* `valid_roots`                 : list of shared filesystem roots (optional).  Pilot sandboxes must lie under these roots.
* `pilot_agent`                 : type of pilot agent to use

