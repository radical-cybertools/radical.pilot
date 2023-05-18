
# Pilot Structure

Pilots are defined via the RP API, and are shaped (in space, times and
capabilities) according to application requirements.  The pilot is structured in
3 layers: pilot job, pilot partition, and pilot agent.

The pilot job is a placeholder job which gets submitted to a target resource
(cluster), usually via the target resource's batch system.  The pilot job has
two purposes: (i) to acquire sufficient resources and capabilities from the
target resource, and (ii) to host the bootstrapping chain - described below.
One of the bootstrapping stages (`bootstrap_1.py`) partitions the resources,
acquired by the pilot job, to one or more partitions.  On each partition, the
bootstrapper places one pilot agent which manages that partition and executes
tasks on its resources.

### Implementation:

A pilot agent consists of different types of components which can be distributed
over different compute nodes, and which communicate with each other over
a network of ZMQ channels.  The agent can be configured to create multiple
instances of each component type, for scaling and reliability purposes.


### Multiplicity:

    * 1 application          : 1..n pilot jobs
    * 1 pilot job            : 1..m pilot partitions
    * 1 pilot partition      : 1    pilot agent
    * 1 pilot agent          : k    pilot agent components
    * 1 pilot agent component: 1..i pilot agent component instances


## Pilot Job Specification

The pilot job needs to have sufficient resources to hold the pilot partitions
required by the application.  RP additionally needs to know where and how to
submit the pilot job.  These information are passed via the pilot description
which additionally references a list of partition descriptions, obviously
describing the partitions to be created in the pilot job.

The pilot description specifies:

    * resource target
    * resource access mechanism (schema, project)
    * runtime environment settings (sandbox, STDOUT/STDERR)
    * runtime (optional)

Other parameters needed for the pilot job submission are stored in the resource
configuration files.  Resource requirements are derived from the partition
specifications (CPUs, GPUs, Memory, ...).


## Pilot Partitions

A pilot partition represents a subset of the resources acquired by a pilot job.
One or more partitions can co-exist in a pilot job - but it is guaranteed that
any resources managed by one partition are not concurrently used by any other
partition.  A pilot partition thus represents an abstracted view of a resource
partition to the pilot agent.

Partition lifetimes are independent of pilot lifetimes - but a pilot will always
start with a pre-defined set of partitions which manage the pilot's resources.


# Pilot Bootstrapping

## Stage 0: `bootstrap_0.sh`

`bootstrapper_0.sh` is the original workload of the pilot job as it is placed on
the target resource.  It is usually started by the batch system on the first of
the allocated nodes.  In cases where multiple pilots get submitted in one
request, several `bootstrap_0.sh` instances may get started simultaneously - but
this document is only concerned with individual instances, and assumes that any
separation wrt.  environment and available resources is taken care of *before*
instantiation.

The purpose of stage 0 is to prepare the Shell and Python environment for later
bootstrapping stages.  Specifically, the stage will:

  - create the pilot sandbox under `$PWD`;
  - create a tunnel for MongoDB access (if needed);
  - perform resource specific environment settings to ensure the availability of
    a functional Python (`module load` commands etc);
  - create a virtualenv, or ensure a VE exists and is usable;
  - activate that virtualenv;
  - install all python module dependencies;
  - install the RCT stack (into a tree outside of the VE)
  - create `bootstrap_2.sh` on the fly
  - start the next bootstrapping stage (`bootstrapper_1.py`) in Python land.


## Stage 1: `bootstrap_1.py`

The second stage enters Python, and will:

  - pull pilot partition creation and termination requests from MongoDB;
  - invoke RM specific code parts of RP to set up the requested partitions;
  - run `bootstrap_2.sh` for each partition to get the partition's pilot agent
    started.


## Stage 2: `bootstrap_2.sh`

This script has been created on the fly by stage 0 to make sure that all pilot
agents and sub-agents use the exact same environment settings as created and
defined by stage 0, w/o the need to redo the necessary setup steps.  More
specifically, `bootstrap_2.sh` will launch the `radical-pilot-agent` Python
script on the partition.  It will already use the configured agent launch
methods (`agent_launch_method` and `agent_spawner`) for placing and starting the
agents.


## Stage 3: `radical-pilot-agent` (`bootstrap_3`)

Up to this point we do not actively place any element of the bootstrapping
chain, and the elements thus live wherever the batch system happens to
originally place them.  This changes now: depending on the agent configuration,
this script will land on a specific node which is part of the partition the
agent is supposed to manage (but it can also live on a MOM-node or other special
nodes on certain architectures).

The `radical-pilot-agent` Python script and its only method (`bootstrap_3()`)
manage the lifetime of the `Agent_0` class instance, which *is* the agent
responsible for managing the partition it got started on.


## Stage 4: `Agent_0`

The `Agent_0` class constructor will now inspect its environment, and will
determine what resources are available.  This will respect the partitioning
information from `bootstrap_1.py`.  Based on that information and on the agent
configuration file (`agent_0.cfg`), the class will then instantiate all
communication bridges and several RP agent components.  The configuration file
also contains information about any sub-agents to be placed on the compute
nodes.

Once the respective sub-agent placement decisions have been made, `Agent_0` will
write configuration files for those sub-agents, and then again use the
`agent_launch_method` and `agent_spawner` (as defined in its configuration file)
to execute `bootstrap_2.sh agent_$n` on the target node.  Using `bootstrap_2.sh`
will ensure that the sub-agents find the same environment as `Agent_0`.  Using
the launch and spawner methods of RP avoids special code paths for agent and
task execution - which are ultimately the same thing.


## `Agent_n`

The `Agent_n` sub-agents, which host additional agent component instances, are
bootstrapped by the same mechanism as `Agent_0`: `bootstrap_2.sh`,
`radical-pilot-agent`, `bootstrap_3()`, `Agent_n` instance.  The only difference
is the limited configuration file, and the disability of `Agent_n` to spawn
further sub-agents.

The sub-agent components will connect to the communication bridges (their
addresses have been stored in the sub-agent configuration files by `Agent_0`),
and will report successful startup and component instantiation.  Once all
sub-agents report for duty, the bootstrapping process is complete, and the agent
start operation by pulling tasks from the database.

---

