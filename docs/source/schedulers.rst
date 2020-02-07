
.. _chapter_schedulers:

**************
Unit Scheduler
**************

.. toctree::

Introduction
------------

The `class:radical.pilot.UnitManager` dispatches compute units to available
pilots for execution.  It does so according to some scheduling algorithm,
which can be selected when instantiating the manager.  Momentarily we support
two scheduling algorithms: 'Round-Robin' and 'Backfilling'.  New schedulers
can be added to radical.pilot. Please Open an issue in the RADICAL-Pilot github 
`issue tracker <https://github.com/radical-cybertools/radical.pilot/issues>`_
for support.

Note that radical.pilot enacts a second scheduling step: once a pilot agent
takes ownership of units which the unit manager scheduler assigned to it, the
agent scheduler will place the units on the set of resources (cores) that
agent is managing.  The agent scheduler can be configured via agent and
resource configuration files (see :ref:`chapter_resources`).

Round-Robin Scheduler (``SCHEDULER_ROUND_ROBIN``)
-------------------------------------------------

The Round-Robin scheduler will fairly distributed arriving compute units over
the set of known pilots, independent of unit state, expected workload, pilot
state or pilot lifetime.  As such, it is a fairly simplistic, but also a very
fast scheduler, which does not impose any additional communication round trips
between the unit manager and pilot agents.


Backfilling Scheduler (``SCHEDULER_BACKFILLING``)
-------------------------------------------------

The backfilling scheduler does a better job at actual load balancing, but at
the cost of additional communication round trips.  It depends on the actual
application workload if that load balancing is beneficial or not.

Backfilling is most beneficial for large numbers of pilots and for relatively
long running units (where the CU runtime is significantly longer than the
communication round trip time between unit manager and pilot agent).

In general we recommend to *not* use backfilling for:
  - a single pilot;
  - large numbers of short-running CUs.

The backfilling scheduler (BF) will only dispatch units to pilot agents once
the pilot agent is in 'RUNNING' state.  The units will thus get executed even
if one of the pilots never reaches that state: the load will be distributed
between pilots which become 'ACTIVE'.

The BF will only dispatch as many units to an agent which the agent can, in
principle, execute concurrently.  No units will be waiting in the agent's own
scheduler queue.  The BF will react on unit termination events, and will then
backfill (!) the agent with any remaining units.  The agent will remain
under-utilized during that communication.

In order to minimize agent under-utilization, the user can set the environment
variable `RADICAL_PILOT_BF_OVERSUBSCRIPTION`, which specifies (in percent)
with how many units the BF can overload the pilot agent, without waiting for
unit termination notices.  This mechanism effectively hides the communication
latencies, as long as unit runtimes are significantly larger than the
communication delays.  The default over subscription value is '0%', i.e., no
over subscription.


