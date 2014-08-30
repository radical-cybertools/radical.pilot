
.. _chapter_machconf:

****************************
Resource Configuration Files
****************************

Introduction
============

In order to keep RADICAL-Pilot applications free from clutter and 
machine-specific parameters and constants, RADICAL-Pilot uses 
resource configuration files.

Machine configuration files can be passed to a :class:`radical.pilot.PilotManager` 
instance::

    FGCONF = 'https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json'
    
    s = radical.pilot.Session(database_url=DBURL)
    pm = radical.pilot.PilotManager(session=s, resource_configurations=FGCONF)

Multiple configuration files can be passed as a list::

    FGCONF = 'https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json'
    XSCONF = 'https://raw.github.com/radical-cybertools/radical.pilot/master/configs/xsede.json'

    s = radical.pilot.Session(database_url=DBURL)
    pm = radical.pilot.PilotManager(session=s, resource_configurations=[FGCONF, XSCONF])

Resource configuration file URLs can either be `https(s)://` URLs to point to 
a remote location, or `file://localhost` URLs to point to a local file. ::

    REMOTE_CONF = 'https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json'
    LOCAL_CONF  = 'file://localhost/home/project/cfgs/mycustomconfig.json'

A resource configuration uses the JSON format and has the following layout::

    {
        "futuregrid.INDIA": {
            ...
        },

        "futuregrid.SIERRA": {
            ...
        }
    }

In the example above, `futuregrid.INDIA` and `futuregrid.SIERRA` are the
**resource keys**. Resource keys are referenced in
:class:`radical.pilot.ComputePilotDescription` to create a
:class:`radical.pilot.ComputePilot` for a given machine::

    pd = radical.pilot.ComputePilotDescription()
    pd.resource = "futuregrid.INDIA"  # Key defined in futuregrid.json
    pd.cores = 16

    pilot_india = pm.submit_pilots(pd)


Available Resource Configuration Files
======================================

We maintain a set of ready to use resource configuration files:

FutureGrid
----------

* Homepage: `http://www.futuregrid.org <http://www.futuregrid.org>`_
* Resource file URL: `https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json <https://raw.github.com/radical-cybertools/radical.pilot/master/configs/futuregrid.json>`_

XSEDE
-----

* Homepage: `http://www.xsede.org <http://www.xsede.org>`_
* Resource file URL: `https://raw.github.com/radical-cybertools/radical.pilot/master/configs/xsede.json <https://raw.github.com/radical-cybertools/radical.pilot/master/configs/xsede.json>`_

Writing a Custom Resource Configuration File
============================================

If you want to use RADICAL-Pilot with a resource that is not in any of the provided 
configuration files, you can write your own.

A configuration file has to be valid JSON. The structure is as follows:

.. code-block:: python

    {
        "RESOURCE_KEY_NAME": {
            "URL"                : "slurm+ssh://stampede.tacc.utexas.edu",
            "filesystem"         : "sftp://stampede.tacc.utexas.edu/",
            "default_queue"      : "normal",
            "python_interpreter" : "/opt/apps/python/epd/7.3.2/bin/python",
            "pre_bootstrap"      : ["module purge", "module load TACC", "module load cluster", "module load python/2.7.3-epd-7.3.2"],
            "task_launch_mode"   : "SSH",
            "valid_roots"        : ["/home1", "/scratch", "/work"]
        },
        "ANOTHER_KEY_NAME": ...
    }

`RESOURCE_KEY_NAME` is the string which is used as value for 
`ComputePilotDescription.resource` to reference this entry. They have to be 
unique. 

All fields are mandatory:

* `URL`: The URL of the cluster queueing manager. This can be one of `pbs+ssh://`, `sge+ssh://`, `slurm+ssh://`.
* `filesystem`: An SFTP URL that points to the remote cluster's root filesystem. 
* `default_queue`: The default cluster queue to use if not defined in :class:`radical.pilot.ComputePilotDescription` 
* `python_interpreter`: The path to a valid Python interpreter (**>= 2.6**) on the remote cluster.
* `pre_bootstrap`: A list of commands to execute before RADICAL-Pilot agent startup.
* `task_launch_mode`: The RADICAL-Pilot agent task launch method. This can be either "SSH" (only single-core tasks are supported) or "MPI" (mpi-style tasks are supported).
* `valid_roots`: A list of valid directory prefixes for shared filesystem mounts. A user can define the agent working directory via the `PilotDescription.sandbox` parameter. This is checked against the list of `valid_roots` to ensure the user doesn't provide an invalid path.

