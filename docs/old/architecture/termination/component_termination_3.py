#!/usr/bin/env python

import pprint

import radical.utils       as ru
import radical.pilot       as rp
import radical.pilot.utils as rpu

class TestComponent(rpu.Component):

    def __init__(self, cfg, session):
        rpu.Component.__init__(self, cfg, session)
        print self._uid

    @classmethod
    def create(cls, cfg, session):
        return cls(cfg, session)

    def initialize_child(self):
        self.register_input ('TEST_IN_PENDING',  'test_in_queue', self.work)
        self.register_output('TEST_OUT_PENDING', 'test_out_queue')
        self.register_subscriber('test_control_pubsub', self._control_cb)

    def _control_cb(self, topic, msg):
        if msg.get('cmd') == 'stop':
            self.stop()

    def work(self, things):
        for thing in things:
            thing['state'] = 'TEST_OUT_PENDING'
        self._advance(things)
            


# ------------------------------------------------------------------------------

cfg = {
    "heartbeat_interval" : 10,
    "heartbeat_timeout"  : 300,
    "bridges" : {
        "log_pubsub"     : {"log_level" : "debug",
                            "stall_hwm" : 1,
                            "bulk_size" : 0},
        "state_pubsub"   : {"log_level" : "debug",
                            "stall_hwm" : 1,
                            "bulk_size" : 0},
        "control_pubsub" : {"log_level" : "debug",
                            "stall_hwm" : 1,
                            "bulk_size" : 0}
    },
    "components" : {
        "UpdateWorker"  : 1, 
        "TestComponent" : 2
    }
}


s = rp.Session(cfg=cfg)
pprint.pprint(s.ctrl_cfg)
s.close()

