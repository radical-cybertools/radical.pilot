
# Configuration Settings

RP is configurable in many different ways, for two main reasons:

  * RP is used for *research* in distributed computing;
  * RP targets a wide variety of resources and use cases with a
    single code base.


There exist multiple dimensions of configurability, using different means:

  * *resource configuration files* contain settings which adapt RP to different
    resource types;
  * *agent, session, tmgr and pmgr configuration files* contain settings which
    tune RP towards certain use cases and experiments
  * *environment variables* control RP's logging and profiling behavior

The lines between those configuration classes are not strict though: certain
agent configurations are required on specific resources; some rarely used
logging- and profiling preferences are set in config files instead of
environment variables; some lower lever configurations (radical.saga and
radical.utils layer) have their own set of configuration options, which can
overlap RP configuration.

But either way, the above in general holds, and this document describes the
respective configuration settings in this scheme.


NOTE: it is difficult to keep detailed documentation in sync with a dynamically
      and quickly evolving code base.  Please open a ticket or contact the
      mailing list if you have specific questions about RP configuration
      options!


## Resource Configuration

The purpose of RP's resource configuration files is to ensure that RP correctly
interfaces to the resource's software and OS stack, such as it's batch system,
access mechanisms, network settings, filesystem sharing and retention policies,
etc.  As such, the configurations capture relevant system configuration choices
where those cannot be automatically be detected.  In fact, we often forego
opportunities of automated detection in favor of explicit configuration, at
least at the current stage of development.

Resource configuration files are, as all configuration files really, located
under `src/radical/pilot/configs/`, and are named `resource_xyz.json`, where
`xyz` usually denotes a site name which hosts the respective resource(s).

FIXME: ...


## Agent Configuration

The RP agent is arguably the most configurable element in RP, as it is the main
target for the resource configuration settings listed above, but also offers
a wide range of resource independent configuration settings, which tune its
behavior and performance in various ways.

First, there is a set of configuration options which affects how the different
agent components communicate which each other, and with the RP database: pulling
options, timeouts, buffer sizes, artificial barriers (for experiments) etc. are
part of those options.

Secondly, the agent components can be placed relatively freely on the target
resource: all components can live on the same node, or spread different nodes;
multiple instance for each component can exist; communication bridges can be
configured and placed freely; etc.  There exist some resource specific
constraints though such as:

  * the agent topology influences the number of nodes available for the
    workload;
  * internal and external network connectivity constraints the placement of some
    components;
  * some components (such as the agent scheduler and communication bridges)
    cannot be made redundant;
  * the agent instance is placed by the batch system, and thus resource dependent
    and not configurable.

In general, an agent config is structured like this:

```
{
  'option 1' : 'value 1',
  'option 2' : 'value 2',
  'agents'   : [
    {
      'target'     : 'local',
      'mode'       : 'shared',
      'partition'  : '50%',
      'bridges'    : [ {'type'       : 'queue 1` } ,
                       {'type'       : 'pubsub 2`} ,
                       {'type'       : 'pubsub 3`} ],
      'components' : [ {'type'       : 'component 1`, 'count' : '2'},
                       {'type'       : 'component 2`, 'count' : '2'} ],
       'agents'    : [ {'target'     : 'node',
                        'mode'       : 'shared',
                        'components' : [{'type' : 'component 3`, 'count': '8'}]}]
    },
    {
      'target'     : 'local',
      'mode'       : 'shared',
      'partition'  : '50%',
      'bridges'    : [ {'type'       : 'queue 1` } ,
                       {'type'       : 'pubsub 2`} ,
                       {'type'       : 'pubsub 3`} ],
      'components' : [ {'type'       : 'component 1`, 'count' : '2'},
                       {'type'       : 'component 2`, 'count' : '2'},
                       {'type'       : 'component 3`, 'count': '8'}]
    },
  ]
}
```

This example is to be interpreted as follows:

  * `option 1` and `option 2` apply to all agents, bridges and components.
  * `target` is set to `local` when the respective agent should reside on the
    same node as the parent agent; is set to `node` when the respective agent is
    to be started on an otherwise unoccupied node
  * `mode` is set to `shared`, if the agent's node can also be used to execute
    tasks; is set to `reserved` if the node is exclusively used for the
    agent, and not shared with the workload.
  * `partition` allows to run multiple independent agent instances in the same
    pilot job, each managing a subset (partition) of the available nodes.
  * `agents`: a set of sub-agents to be spawned by the current agent instance.
    Each agent can spawn communication bridges, components, and further
    sub-agents.
  * `bridges`: a set of communication bridges, which is used for communication
    between all elements within the respective top-level agent
  * `components`: a set of components which manage the actual workload.


Limitations:

  * `partition` is currently not used;
  * only one top-level agent is currently allowed;
  * the hierarchy of agents can only be two levels deep;
  * only one bridge of each type can exist per top-level agent;
  * bridges must live in a top-level agent.


