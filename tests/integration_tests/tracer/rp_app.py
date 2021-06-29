#!/usr/bin/env python3

import time

import radical.pilot as rp
import radical.utils as ru


def start_hb(uid):

        bcfg = ru.Config(cfg={'channel'  : 'heartbeat',
                              'type'     : 'pubsub',
                              'uid'      : uid,
                              'stall_hwm': 1,
                              'bulk_size': 0,
                              'path'     : '.'})
        hb_bridge = ru.zmq.PubSub(bcfg)
        hb_bridge.start()

        cfg = {'pub': str(hb_bridge.addr_pub),
               'sub': str(hb_bridge.addr_sub),
               'interval': 200,
               'timeout' : 900}
        ru.write_json('heartbeat_pubsub.cfg', cfg)

        # runs a HB monitor on that channel
        ru.Heartbeat(uid=uid, timeout=900, interval=300)

        return cfg


if __name__ == '__main__':

    # start heartbeat pubsub (embedded)
    uid = 'hb'
    hb_cfg = start_hb(uid)

    # start control pubsub
    control_pubsub_cfg = ru.read_json('./control_pubsub.json')
    control_pubsub_cfg['heartbeat'] = hb_cfg
    ru.write_json('./control_pubsub.json', control_pubsub_cfg)
    ru.sh_callout_bg('../bin/radical-pilot-bridge control_pubsub.json')

    # start state pubsub
    state_pubsub_cfg = ru.read_json('./state_pubsub.json')
    state_pubsub_cfg['heartbeat'] = hb_cfg
    ru.write_json('./state_pubsub.json', state_pubsub_cfg)
    ru.sh_callout_bg('../bin/radical-pilot-bridge state_pubsub.json')

    # start tracer queue
    tracer_queue_cfg = ru.read_json('./tracer_queue.json')
    tracer_queue_cfg['heartbeat'] = hb_cfg
    ru.write_json('./tracer_queue.json', tracer_queue_cfg)
    ru.sh_callout_bg('../bin/radical-pilot-bridge tracer_queue.json')

    # start tracer component
    tracer_cfg = ru.read_json('./tracer.json')
    tracer_cfg['heartbeat'] = hb_cfg
    ru.write_json('./tracer.json', tracer_cfg)
    ru.sh_callout_bg('../bin/radical-pilot-component tracer.json')

    # let things settle
    time.sleep(1)

    # create profiler, point to tracer queue
    tgt_cfg = ru.read_json('tracer_queue.cfg')
    prof = ru.Profiler('radical.pilot', target=tgt_cfg['put'])

    # create profile traces
    start = time.time()
    for i in range(1024 * 1024):
        prof.prof('event_%d' % i)
    stop = time.time()
    print('ttp: %.2f' % (stop - start))

    # let tracer component work for a while
    time.sleep(10)

