
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


Pubsub Tools::

   Bridge:
        pubsub/pubsub.py         <channel>
        bin/radical-pilot-bridge <channel> pubsub

    Producer:
        pubsub/pub.py            <channel> <topic_1> ...
        bin/radical-pilot-pub    <channel> <topic_1> ...

    Consumer:
        pubsub/sub.py            <channel> <topic_1> ...
        bin/radical-pilot-sub    <channel> <topic_1> ...


Queue Tools::

   Bridge:
        queue/queue.py           <name>
        bin/radical-pilot-bridge <name> queue

    Producer:
        pubsub/put.py            <name>
        bin/radical-pilot-put    <name>

    Consumer:
        pubsub/get.py            <name>
        bin/radical-pilot-get    <name>

Pubsub Scenario:

    cd  pubsub
    ./pubsub.py states
    ./pub.py    states unit
    ./pub.py    states pilot
    ./sub.py    states unit pilot
    ./sub.py    states unit pilot


Queue Scenario:

    cd  queue
    ./queue.py  execution
    ./put.py    execution
    ./put.py    execution
    ./get.py    execution
    ./get.py    execution
    ./get.py    execution


