
RP uses Queues to communicate entities like pilots and units, and uses Pubsub
channels to communicate events like state upudates or failures.  Those Queues
and Pubsub channels are implemented in ZMQ.  This directory contains the
conceptual code snippets which document how exactly ZMQ is used in the code.
Those code snippets are actually interoperable with the RP implementation in
`src/radical/pilot/utils/{queue,pubsub}.py`, and can be used as endpoints for
testing etc.

For both communication types, we provide three different entities, two for
producers and consumers of messages, and one `bridge` type for load balancing
and message distribution.  The types are:

  Queue : PUT, BRIDGE, GET
  Pubsub: PUB, BRIDGE, SUB

RP modules usually instantiate only one pubsub bridge, which manages multiple
channels (channels are identified by a `topic` string - ZMQ rules for topic
string interpretations apply).

Queues are established by instantiating one bridge instance per queue.

The bridges need to be started first - they create a small control file which
contains the input and output addresses to be used by message producers and
consumers in order to use those bridges (i.e. in order to join those
communication channels).

