
.. _chapter_machconf:

************************************
Using Local and Remote HPC Resources
************************************

Introduction
============

The real advantage of using RADICAL-Pilot becomes visible when it is used 
on large HPC clusters. RADICAL-Pilot allows you to launch a ComputePilot 
allocating a large number of cores and then use it to run many ComputeUnits
with small core-counts. This is not only a very nice abstraction to separate
resource allocation / management from resource usage, but also circumvents 
very effectively HPC cluster queue policies and waiting times which can 
significantly reduce the total time to completion (TTC) of your application. 

If you want to use a remote HPC resource, in this example a cluster named
"archer", you define it in the ComputePilotDescription like this:

.. code-block:: python

    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource   = "archer.ac.uk"
    pdesc.project    = "e1234"
    pdesc.runtime    = 60
    pdesc.cores      = 128

Using a ``resource`` key other than "localhost" implicitly tells RADICAL-Pilot
that it is dealing with a remote resource. RADICAL-Pilot is using the SSH 
(and SFTP) protocols to communicate with remote resources. The next section,
:ref:`ssh_config` provides some details about SSH set-up. 
:ref:`preconfigured_resources` list the resource keys that are already defined 
and ready to use in RADICAL-Pilot.

In some cases you may want to use an HPC resource locally instead of remotely.
For example, you might want to run your application from one of the cluster
head-nodes. In this case you can tell RADICAL-Pilot not to use SSH but to
access the queuing system and file systems locally by simply adding **:local**
to the end of the resource key:

.. note::
    .. code-block:: python

        pdesc = radical.pilot.ComputePilotDescription()
        pdesc.resource   = "archer.ac.uk:local"


.. _ssh_config:

Configuring SSH Access
======================

If you can manually SSH into the target resource, RADICAL-Pilot can do the same.
While RADICAl-Pilot supports username / password authentication, it is 
highly-advisable to set-up password-less ssh keys for the resource you want to
use. If you are not familiar with this, check out 
`THIS LINK <http://www.debian-administration.org/articles/152>`_. 

All SSH-specific informations, like remote usernames and passwords are set in
a  ``Context`` object. For example, if you want to tell RADICAL-Pilot your 
user-id on the remote resource, use the following construct:

.. code-block:: python

    session = radical.pilot.Session(database_url=DBURL)

    c = radical.pilot.Context('ssh')
    c.user_id = "tg802352"
    session.add_context(c)

.. note::
    **Tip:** You can create an empty file called `.hushlogin` in your home 
    directory to turn of the system messages you see on your screen at every
    login. This can help if you encounter random connection problems with 
    RADICAL-Pilot. 

.. _preconfigured_resources:

Pre-Configured Resources
========================

Resource configurations are a set of dictionaries that hide specific details 
of a remote resource (queuing-, file-system- and environment-details) from the 
user. A user allocates a pre-configured resource like this:

.. code-block:: python

    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource   = "archer.ac.uk"
    pdesc.project    = "e1234"
    pdesc.runtime    = 60
    pdesc.cores      = 128

We maintain a growing set of resource configuration files:

.. include:: ./resources.rst  


Writing a Custom Resource Configuration File
============================================

If you want to use RADICAL-Pilot with a resource that is not in any of the provided 
configuration files, you can write your own.

A configuration file has to be valid JSON. The structure is as follows:

.. code-block:: python

    # filename: lrz.json
    {
        "supermuc": 
        {
            "description"                 : "The SuperMUC petascale HPC cluster at LRZ.",
            "notes"                       : "Access only from registered IP addresses.",
            "schemas"                     : ["gsissh", "ssh"],
            "ssh"                         :
            {
                "job_manager_endpoint"    : "loadl+ssh://supermuc.lrz.de/",
                "filesystem_endpoint"     : "sftp://supermuc.lrz.de/"
            },
            "gsissh"                      :
            {
                "job_manager_endpoint"    : "loadl+gsissh://supermuc.lrz.de:2222/",
                "filesystem_endpoint"     : "gsisftp://supermuc.lrz.de:2222/"
            },
            "default_queue"               : "test",
            "lrms"                        : "LOADL",
            "task_launch_method"          : "SSH",
            "mpi_launch_method"           : "MPIEXEC",
            "forward_tunnel_endpoint"     : "login03",
            "global_virtenv"              : "/home/hpc/pr87be/di29sut/pilotve",
            "pre_bootstrap"               : ["source /etc/profile",
                                             "source /etc/profile.d/modules.sh",
                                             "module load python/2.7.6",
                                             "module unload mpi.ibm", "module load mpi.intel",
                                             "source /home/hpc/pr87be/di29sut/pilotve/bin/activate"
                                            ],
            "valid_roots"                 : ["/home", "/gpfs/work", "/gpfs/scratch"],
            "pilot_agent"                 : "radical-pilot-agent-multicore.py"
        },
        "ANOTHER_KEY_NAME": 
        {
            ...
        }
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
* `valid_roots`                 : list of shared file system roots (optional).  Pilot sandboxes must lie under these roots.
* `pilot_agent`                 : type of pilot agent to use

