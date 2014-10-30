
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
"archer", located at EPSRC, UK, you define it in the ComputePilotDescription
like this:

.. code-block:: python

    pdesc = radical.pilot.ComputePilotDescription()
    pdesc.resource = "epsrc.archer"
    pdesc.project  = "e1234"
    pdesc.runtime  = 60
    pdesc.cores    = 128

Using a ``resource`` key other than "local.localhost" implicitly tells RADICAL-Pilot
that it is dealing with a remote resource. RADICAL-Pilot is using the SSH/GSISSH
(and SFTP/GSISFTP) protocols to communicate with remote resources. The next section,
:ref:`ssh_config` provides some details about SSH set-up. 
:ref:`preconfigured_resources` list the resource keys that are already defined 
and ready to use in RADICAL-Pilot.


.. _ssh_config:

Configuring SSH Access
======================

If you can manually SSH into the target resource, RADICAL-Pilot can do the same.
While RADICAl-Pilot supports username / password authentication, it is 
highly-advisable to set-up password-less ssh keys for the resource you want to
use. If you are not familiar with this, check out this 
`link <http://www.debian-administration.org/articles/152>`_. 

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
    pdesc.resource   = "epsrc.archer"
    pdesc.project    = "e1234"
    pdesc.runtime    = 60
    pdesc.cores      = 128
    pdesc.queue      = "large"

We maintain a growing set of resource configuration files.  Several of the
settings included there can be adapted in the `PilotDescription` (for example,
the snipped above replaces the default queue `standard` with the queue `large`).
For a list of supported configurations, see :ref:`chapter_resources` - those
resource files live under `radical/pilot/configs/`.


Writing a Custom Resource Configuration File
============================================

If you want to use RADICAL-Pilot with a resource that is not in any of the
provided configuration files, you can write your own, and drop it in
`$HOME/.radical/pilot/configs/<your_site>.json`.

.. note::
    Be advised that you may need system admin level knowledge for the target
    cluster to do so.  Also, while RADICAL-Pilot can handle very different types
    of systems and batch system, it may run into trouble on specific
    configurationsor versions we did not encounter before.  If you run into
    trouble using a cluster not in our list of officially supported ones, please
    drop us a note on the users 
    `mailing list <https://groups.google.com/d/forum/radical-pilot-users>`_.

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


The name of your file (here `lrz.json`) together with the name of the resource
(`supermuc`) form the resource key which is used in the
`class:ComputePilotDescription` resource attribute (`lrz.supermuc`).

All fields are mandatory, unless indicated otherwise below.

* `description`                 : a human readable description of the resource
* `notes`                       : information needed to form valid pilot descriptions, such as which parameter are required, etc. 
* `schemas`                     : allowed values for the `access_schema` parameter of the pilot description.  The first schema in the list is used by default.  For each schema, a subsection is needed which specifies `job_manager_endpoint` and `filesystem_endpoint`.
* `job_manager_endpoint`        : access url for pilot submission (interpreted by SAGA)
* `filesystem_endpoint`         : access url for file staging (interpreted by SAGA)
* `default_queue`               : queue to use for pilot submission (optional)
* `lrms`                        : type of job management system (`LOADL`, `LSF`, `PBSPRO`, `SGE`, `SLURM`, `TORQUE`, `FORK`)
* `task_launch_method`          : type of compute node access (required for non-MPI units: `SSH`,`APRUN` or `LOCAL`)
* `mpi_launch_method`           : type of MPI support (required for MPI units: `MPIRUN`, `MPIEXEC`, `APRUN`, `IBRUN` or `POE`)
* `python_interpreter`          : path to python (optional)
* `pre_bootstrap`               : list of commands to execute for initialization (optional)
* `valid_roots`                 : list of shared file system roots (optional).  Pilot sandboxes must lie under these roots.
* `pilot_agent`                 : type of pilot agent to use (`radical-pilot-agent-multicore.py`)
* `forward_tunnel_endpoint`     : name of host which can be used to create ssh tunnels from the compute nodes to the outside world (optional)

Several configuration files are part of the RADICAL-Pilot installation, and live
under `radical/pilot/configs/`.  


Customizing Resource Configurations Programatically
===================================================

The set of resource configurations available to the RADICAL-Pilot session is
accessible programatically.  The example below changes the `default_queue` for
the `epsrc.archer` resource.

.. code-block:: python

    import radical.pilot as rp
    
    # create a new session, and get the respective resource config instance
    session = rp.Session()
    cfg = session.get_resource_config('epsrc.archer')
    print "Default queue of archer is: %s" % cfg['default_queue']

    # create a new config based on the old one, and set a different queue
    new_cfg = rp.ResourceConfig(cfg)
    new_cfg.default_queue = 'quick'

    # now add the entry back.  As we did not change the config name, this will
    # replace the original configuration.  A completely new configuration would
    # need a unique name.
    session.add_resource_config(new_cfg)

    # verify that the changes are in place: retrieve the config again and print
    # the queue
    check_cfg = session.get_resource_config('epsrc.archer')
    print "Default queue of archer after change is: %s." % s['default_queue']


