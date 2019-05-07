#!/usr/bin/env python

import sys
import time
import radical.pilot.utils as rpu


def test_pubsub():

    # FIXME: test disabled
    return


    b  = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'agent_state_pubsub', rpu.PUBSUB_BRIDGE)

    s1 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'agent_state_pubsub', rpu.PUBSUB_SUB)
    s1.subscribe('state')

    s2 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'agent_state_pubsub', rpu.PUBSUB_SUB)
    s2.subscribe('state')

    p1 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'agent_state_pubsub', rpu.PUBSUB_PUB)
    p2 = rpu.Pubsub.create(rpu.PUBSUB_ZMQ, 'agent_state_pubsub', rpu.PUBSUB_PUB)

    time.sleep (1)

    N = 500
    print "n   : %d" % N

    start = time.time()
    for i in range(N):
        p1.put('state', {'id' : "p1_%05d" % i})
        p2.put('state', {'id' : "p2_%05d" % i})
    stop = time.time()
    print "sent: %4.2f (%8.1f)" % (stop - start, 2 * N / (stop - start))

    start = time.time()
    for i in range(2 * N):
        msg_1 = s1.get()
        msg_2 = s2.get()
      # print "<= s1 %s" % msg_1
      # print "<= s2 %s" % msg_2
    stop = time.time()
    print "recv: %4.2f (%8.1f)" % (stop - start, 2 * N / (stop - start))

    b.close()

