.. _chapter_example_remote_and_hpc_pilots:

************************************
Launching Remote / HPC ComputePilots  
************************************

**This chapter describes how to use SAGA-Pilot to execute ComputeUnits 
on ComputePilots running on one or more distributed HPC 
clusters.**

As a pilot-job system, SAGA-Pilot aims to provide a programmable resource
overlay that allows a user to efficiently execute their workloads (tasks)
transparently across one or more distributed resources.

SAGA-Pilot has been developed with HPC (High-Performance Computing) clusters
as the primary type of  distributed resources in mind. Currently SAGA-Pilot
supports HPC clusters running the following queuing systems:

* PBS / PBS Pro
* LSF
* SLURM
* Sun Grid Engine 
* IBM LoadLeveler

.. note:: SAGA-Pilot also provides limited support for Grid-style resources 
          based on HTCondor. For more information checkout 
          :ref:`chapter_example_condor_grids`.

Authentication and Security Contexts
------------------------------------

SAGA-Pilot's remote capabilities are built to a large extend on top of SSH and
SFTP. ComputePilot agents are transferred on-the-fly via SFTP and launched via
SSH on the remote clusters. Once a ComputePilot agent has been started, the 
rest of the communication between SAGA-Pilot and the agent happens through
MongoDB (see diagram below).

.. code-block:: text

    +--------------------------------------+
    |              SAGA-Pilot              |
    +--------------------------------------+
          ^                      |
          | <MDB>                | <SSH/SFTP>
          v                      |
     (~~~~~~~~~)           +-----|---------+
     (         )           |  HPC|Cluster  |
     ( MongoDB )           |-----v---------|
     (         )   <MDB>   | +~~~~~~~+     |
     (_________)<----------->| Agent |     |
                           | +~~~~~~~+     |
                           +---------------+

In order to allow SAGA-Pilot to launch ComputePilot agents on a remote  host
via SSH, you need to provided it with the right credentials. This  is done via
the :class:`sagapilot.Context` class.

.. note:: In order for Context to work, you need to be able to manually
          SSH into the target host, i.e., you need to have either a username
          and password or a public / private key set for the host. The 
          most practical way is to set up password-less public-key authentication
          on the remote host. More about password-less keys can be found 
          `HERE <http://www.debian-administration.org/articles/152>`_.

Assuming that you have password-less public-key authentication set up for 
a remote host, the most common way to use Context is to set the 
user name you use on the remote host:

.. code-block:: python

      session = sagapilot.Session(database_url=DBURL)

      c = sagapilot.Context('ssh')
      c.user_id = "tg802352"
      session.add_context(c)

Once you have added a credential to a session, it is available to all
PilotManagers that are created withing this session.

Launching an HPC ComputePilot
-----------------------------

You can describe a :class:`sagapilot.ComputePilot` via a :class:`sagapilot.ComputePilotDescription` to the PilotManager:

.. code-block:: python

    pdesc = sagapilot.ComputePilotDescription()
    pdesc.resource  = "stampede.tacc.utexas.edu"
    pdesc.runtime   = 15
    pdesc.cores     = 32 

    pilot = pmgr.submit_pilots(pdesc)


Launching Multiple ComputePilots
--------------------------------

Scheduling ComputeUnits Across Multiple ComputePilots
-----------------------------------------------------



The Complete Example
--------------------

.. warning:: Make sure to adjust ... before you attempt to run it.

.. literalinclude:: ../../../examples/getting_started_remote.py
