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

Authentication and SSH Credentials 
----------------------------------

ssh creds...


Launching an HPC ComputePilot
-----------------------------

In order to launch a :class:`sagapilot.ComputePilot` on a remote HPC cluster,
SAGA-Pilot needs to know a couple of things about it, like for example how to
access it, what the shared file systems are and so on. In SAGA-Pilot this is
done via **resource configuration files**. Resource configuration files are
simple JSON dictionaries. We provide ready-to-use resource configuration files
for `XSEDE <https://raw.github.com/saga-project/saga-
pilot/master/configs/xsede.json>`_ and `FutureGrid <https://raw.github.com
/saga-project/saga-pilot/master/configs/futuregrid.json>`_, but you can easily
write one for your own (set of) clusters. How you do this is explained in
:ref:`chapter_machconf`.

To add a resource configuration file to SAGA-Pilot, you pass the additional
(optional) ``resource_configurations`` parameter to the
:class:`sagapilot.PilotManager` constructor. It can either point to a string
or a list of strings containing the URL(s) of one or more resource
configuration files. The following types of URLs are supported:

  * ``file://localhost/...`` An absolute path to a local configuration file. 
  * ``http://host/...`` A remote configuration file accessible via HTTP.
  * ``https://host/...`` A remote configuration file accessible via HTTPS.

So if for example you want to launch a ComputePilot on `stampede <https://www.tacc.utexas.edu/stampede/>`_, one of the XSEDE clusters, you can use the `xsede.json <https://raw.github.com/saga-project/saga-pilot/master/configs/xsede.json>`_ configuration file directly from our repository:

.. code-block:: python

    pmgr = sagapilot.PilotManager(session=session,
        resource_configurations="https://raw.github.com/saga-project/saga-pilot/master/configs/xsede.json")

Once you have attached one or more resource configuration files to a
PilotManager, you can use any of the **resource keys** from those files to
describe a :class:`sagapilot.ComputePilot` via a :class:`sagapilot.ComputePilotDescription` to the PilotManager:

.. code-block:: python

    pdesc = sagapilot.ComputePilotDescription()
    pdesc.resource  = "stampede.tacc.utexas.edu"
    pdesc.sandbox   = "/home1/00988/tg802352/sagapilot.sandbox"
    pdesc.runtime   = 15
    pdesc.cores     = 32 

    pilot = pmgr.submit_pilots(pdesc)

.. note:: In order to launch ComputePilot on an HPC cluster, 
          ``ComputePilotDescription.sandbox`` needs to point to a directory 
          on a **shared file system**, otherwise the ComputePilot agent won't 
          start properly.

Launching Multiple ComputePilots
--------------------------------

Scheduling ComputeUnits Across Multiple ComputePilots
-----------------------------------------------------



The Complete Example
--------------------

A fully workin example looks something like this.

.. warning:: Make sure to adjust ... before you attempt to run it.

.. literalinclude:: ../../../examples/remote_hpc_pilots.py
