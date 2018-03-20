
# Agent Bootstrapping

## `bootstrap_0.sh`

`bootstrapper_0.sh` is the original workload when the pilot agent gets placed on
the target resource.  It is usually placed by the batch system on the first of
the allocated nodes.  In cases where multiple pilots get submitted in one
request, several `bootstrap_0.sh` instances may get started simultaneously - but
this document is only concerned with individual instances, and assumes that any
separation wrt. environment and available resources is take care of *before*
instantiation.

The purpose of stage 0 is to prepare the Shell and Python environment for the
later bootstrapping stages.  Specifically, the stage will 

  - creation of the pilot sandbox under `$PWD`;
  - if needed, create a tunnel for MongoDB access;
  - perform resource specific environment settings to ensure the availability
    of a functional Python (`module load` commands etc);
  - create a virtualenv, or ensure if a static VE exists;
  - activate that virtualenv;
  - install all python module dependencies;
  - install the RCT stack (into a tree outside of the VE)
  - create `bootstrap_2,sh` on the fly
  - start the next bootstrapper stage (`bootstrapper_1.py`) in Python land.


## `bootstrap_1.py`

The second stage enters Python, and will 
  
  - pull pilot creation and cancellation requests from MongoDB;
  - invoke LRMS specific code parts of RP to set up a partition for each pilot
    reqest (pilots fail immediately otherwise);
  - run `bootstrap_2.sh` for each pilot to get it enroled on its partition.


## `bootstrap_2.sh`

This has been created on the fly by `bootstrap_0.sh` to make sure that all
agents and sub-agents can use the exact environment settings as created and
defined by `bootstrap_0.sh`, w/o needing to go through the whole process again,
on the same node, or on other node.  More specifically, `bootstrap_2.sh` will
launch the `radical-pilot-agent` python script on the first node of the agent's
partition.


## `radical-pilot-agent` (`bootstrap_3`)

The `radical-pilot-agent` script and its only method `bootstrap_3` manage the
lifetime of `Agent_0` (and later `Agent_n`) class instances.


## `Agent_0`

Up to this point we do not actively place any element of the bootstrapping
chain, and the elements thus live whereever the batch system happens to
originally place them.  This changes now.

The `Agent_0` class constructor will now inspect the job environment, and will
determine whats compute nodes are available.  This will respect the partitioning
information from `bootstrap_1.py`.  Based on that information, and on the agent
configuration file (`agent_0.cfg`), the class will then instantiate all
communication bridges and several RP agent components.  But more importantly,
the config file will also contain information about the placement of the
sub-agents, which will live on the compute nodes and will instantiate the
remaining agent components on those nodes.

Once the sub-agent placement decisions have been made, `Agent_0` will write the
configuration files for those sub-agents, and then use the `agent_launch_method`
and `agent_spawner` (as defined in its config file) to execute `bootstrap_2.sh
agent_$n` on the target node.  Using `bootstrap_2.sh` will ensure that the
sub-agents find the same environment as `Agent_0`.  Using the launch and spawner
methods of RP avoids special code pathes for agent and unit execution - which
are ultimately the same thing.

## `Agent_n`

The sub-agents will connect to the communication bridges (their addresses have
been stored in the sub-agent config files by `Agent_0`), and will report
successful startup and component instantiation.  Once all sub-agents report for
duty, the bootstrapping process is complete, and the agent(s) start operation by
pulling units from the database.


