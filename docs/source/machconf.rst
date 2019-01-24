
.. _chapter_machconf:

************************************
Using Local and Remote HPC Resources
************************************

Introduction
============

RADICAL-Pilot allows you to launch a ComputePilot allocating a large number of
cores on a remote HPC cluster. The ComputePilot is then used to run multiple
ComputeUnits with small core-counts. This separates resource allocation and
management from resource usage, and avoids HPC cluster queue policies and
waiting times which can significantly reduce the total time to completion of
your application.

If you want to use a remote HPC resource (in this example a cluster named
"Archer", located at EPSRC, UK) you have to define it in the
ComputePilotDescription like this:

.. code-block:: python

    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource = "epsrc.archer"
    pdesc.project  = "e1234"
    pdesc.runtime  = 60
    pdesc.cores    = 128

Using a ``resource`` key other than "local.localhost" implicitly tells
RADICAL-Pilot that it is targeting a remote resource. RADICAL-Pilot is using the
SSH/GSISSH (and SFTP/GSISFTP) protocols to communicate with remote resources.
The next section, :ref:`ssh_config` provides some details about SSH set-up.
:ref:`preconfigured_resources` lists the resource keys that are already defined
and ready to be used in RADICAL-Pilot.


.. _ssh_config:

Configuring SSH Access
======================

If you can manually SSH into the target resource, RADICAL-Pilot can do the same.
While RADICAl-Pilot supports username and password authentication, it is
highly-advisable to set-up password-less ssh keys for the resource you want to
use. If you are not familiar with how to setup password-less ssh keys, check out
this `link <http://www.debian-administration.org/articles/152>`_.

All SSH-specific informations, like remote usernames, passwords, and keyfiles,
are set in a  ``Context`` object. For example, if you want to tell RADICAL-Pilot
your user-id on the remote resource, use the following construct:

.. code-block:: python

    session = radical.pilot.Session()

    c = radical.pilot.Context('ssh')
    c.user_id = "tg802352"
    session.add_context(c)

.. note::
    **Tip:** You can create an empty file called `.hushlogin` in your home
    directory to turn off the system messages you see on your screen at every
    login. This can help if you encounter random connection problems with
    RADICAL-Pilot.

.. _preconfigured_resources:

Pre-Configured Resources
========================

Resource configurations are a set of key/value dictionaries with details of a
remote resource like queuing-, file-system-, and environment-. Once a configuration file is available for a given resource, a user chooses that
pre-configured resource in her code like this:

.. code-block:: python

    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource   = "epsrc.archer"
    pdesc.project    = "e1234"
    pdesc.runtime    = 60
    pdesc.cores      = 128
    pdesc.queue      = "large"

The RADICAL-Pilot developer team maintains a growing set of resource
configuration files. Several of the settings included there can be overridden in
the ``ComputePilotDescription`` object. For example, the snipped above replaces
the default queue ``standard`` with the queue ``large``. For a list of supported
configurations, see :ref:`chapter_resources` - those resource files live under
``radical/pilot/configs/``.


Writing a Custom Resource Configuration File
============================================

If you want to use RADICAL-Pilot with a resource that is not in any of the
provided resource configuration files, you can write your own, and drop it in
``$HOME/.radical/pilot/configs/<your_resource_configuration_file_name>.json``.

.. note::
    Be advised that you may need specific knowledge about the target resource to
    do so.  Also, while RADICAL-Pilot can handle very different types of systems
    and batch system, it may run into trouble on specific configurations or
    software versions we did not encounter before.  If you run into trouble
    using a resource not in our list of officially supported ones, please drop
    us a note on the RADICAL-Pilot users `mailing list
    <https://groups.google.com/d/forum/radical-pilot-users>`_.

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
            "pre_bootstrap_0"             : ["source /etc/profile",
                                             "source /etc/profile.d/modules.sh",
                                             "module load python/2.7.6",
                                             "module unload mpi.ibm", "module load mpi.intel",
                                             "source /home/hpc/pr87be/di29sut/pilotve/bin/activate"
                                            ],
            "valid_roots"                 : ["/home", "/gpfs/work", "/gpfs/scratch"],
            "agent_type"                  : "multicore",
            "agent_scheduler"             : "CONTINUOUS",
            "agent_spawner"               : "POPEN",
            "pilot_agent"                 : "radical-pilot-agent-multicore.py",
            "pilot_dist"                  : "default"
        },
        "ANOTHER_KEY_NAME":
        {
            ...
        }
    }


The name of your file (here ``lrz.json``) together with the name of the resource
(``supermuc``) form the resource key which is used in the
`class:ComputePilotDescription` resource attribute (``lrz.supermuc``).

All fields are mandatory, unless indicated otherwise below.

* ``description``: a human readable description of the resource.
* ``notes``: information needed to form valid pilot descriptions, such as what parameter are required, etc.
* ``schemas``: allowed values for the ``access_schema`` parameter of the pilot description.  The first schema in the list is used by default.  For each schema, a subsection is needed which specifies ``job_manager_endpoint`` and ``filesystem_endpoint``.
* ``job_manager_endpoint``: access url for pilot submission (interpreted by SAGA).
* ``filesystem_endpoint``: access url for file staging (interpreted by SAGA).
* ``default_queue``: queue to use for pilot submission (optional).
* ``lrms``: type of job management system. Valid values are: ``LOADL``, ``LSF``, ``PBSPRO``, ``SGE``, ``SLURM``, ``TORQUE``, ``FORK``.
* ``task_launch_method``: type of compute node access, required for non-MPI units. Valid values are: ``SSH``,``APRUN`` or ``LOCAL``.
* ``mpi_launch_method``: type of MPI support, required for MPI units. Valid values are: ``MPIRUN``, ``MPIEXEC``, ``APRUN``, ``IBRUN`` or ``POE``.
* ``python_interpreter``: path to python (optional).
* ``python_dist``: `anaconda` or `default`, ie. not `anaconda` (mandatory).
* ``pre_bootstrap_0``: list of commands to execute for initialization of main agent (optional).
* ``pre_bootstrap_1``: list of commands to execute for initialization of sub-agent (optional).
* ``valid_roots``: list of shared file system roots (optional). Note: pilot sandboxes must lie under these roots.
* ``pilot_agent``: type of pilot agent to use. Currently: ``radical-pilot-agent-multicore.py``.
* ``forward_tunnel_endpoint``: name of the host which can be used to create ssh tunnels from the compute nodes to the outside world (optional).

Several configuration files are part of the RADICAL-Pilot installation, and live
under ``radical/pilot/configs/``.


