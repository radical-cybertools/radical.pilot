#!/usr/bin/env python3

import sys
import time

import radical.utils as ru


# ------------------------------------------------------------------------------
#
def test_agent_bridge(url=None):

    if not url:
        return

    bridge = ru.zmq.Client(url=url)

    sid = 'foo'

    try:
        print(bridge.request('client_register',   {'sid': sid}))
        print(bridge.request('client_lookup',     {'sid': sid}))
        time.sleep(3)
        print(bridge.request('client_heartbeat',  {'sid': sid}))
        time.sleep(3)
        print(bridge.request('client_heartbeat',  {'sid': sid}))
        time.sleep(3)
        print(bridge.request('client_heartbeat',  {'sid': sid}))
        time.sleep(3)
        print(bridge.request('client_heartbeat',  {'sid': sid}))
        time.sleep(3)
        print(bridge.request('client_heartbeat',  {'sid': sid}))
        print(bridge.request('client_lookup',     {'sid': sid}))
        print(bridge.request('client_unregister', {'sid': sid}))
        print(bridge.request('client_lookup',     {'sid': sid}))

    finally:
        bridge.close()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    url = None
    if len(sys.argv) > 1:
        url = sys.argv[1]

    test_agent_bridge(url)


# ------------------------------------------------------------------------------

