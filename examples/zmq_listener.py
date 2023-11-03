#!/usr/bin/env python3

import os
import threading     as mt

import radical.utils as ru
import radical.pilot as rp


TRACER = 'tracer_pubsub'

# ------------------------------------------------------------------------------
#
class RPListener(rp.utils.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        sid      = os.environ.get('RP_SESSION_ID')
        reg_addr = os.environ.get('RP_REGISTRY_ADDRESS')
        session  = rp.Session(uid=sid, _role=rp.Session._DEFAULT,
                                       _reg_addr=reg_addr)

        super().__init__(session.cfg, session)

        self.register_subscriber(TRACER, self._tracer_cb)

        self.start()

    # --------------------------------------------------------------------------
    #
    def _tracer_cb(self, topic, msg):
        print('=== TRACER2: %s: %s' % (topic, msg))


# ------------------------------------------------------------------------------
#
class ZMQListener(object):

    def __init__(self):

        reg_addr   = os.environ['RP_REGISTRY_ADDRESS']
        self._reg  = ru.zmq.RegistryClient(url=reg_addr)
        tracer_cfg = self._reg['bridges.%s' % TRACER]

        self._sub = ru.zmq.Subscriber(TRACER, tracer_cfg['addr_sub'],
                                      topic=TRACER, cb=self._tracer_cb)


    # listen for tracer events
    def _tracer_cb(self, topic, msg):
        print('=== TRACER1: %s: %s' % (topic, msg))


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    print('==================== start ==================')

    zmq_listener = ZMQListener()
    rp_listener  = RPListener()

    # report successful startup to RP
    tid = os.environ['RP_TASK_ID']
    out, err, ret = ru.sh_callout('radical-pilot-service-signal %s' % tid)
    print('=== signal: %s - %s - %s ' % (out, err, ret))

    rp_listener.wait()

    print('==================== end ====================')


# ------------------------------------------------------------------------------


