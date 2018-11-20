
import os
import sys
import time

import radical.utils           as ru
import radical.pilot           as rp
import radical.pilot.utils     as rpu
import radical.pilot.constants as rpc


# ------------------------------------------------------------------------------
#
class CompB(rpu.Component):

    def __init__(self, session):

        self._uid     = 'comp_b'
        self._msg     = None
        self._session = session

        rpu.Component.__init__(self, session._cfg, self._session)


    def initialize_child(self):

        self.register_subscriber(rpc.CONTROL_PUBSUB, self.control_cb_1)

    def finalize_child(self):

        print "got  %s" % self._msg


    def control_cb_1(self, topic, msg):
        self._msg = msg


# ------------------------------------------------------------------------------
#
class CompA(rpu.Component):

    def __init__(self, session):

        self._idx     = 0
        self._uid     = 'comp_a'
        self._msg     = None
        self._session = session

        rpu.Component.__init__(self, self._session._cfg, self._session)


    def initialize_child(self):
        self.register_timed_cb(self.idle_cb, timer=0.2)
#       self.register_publisher(self.idle_cb, timeout=2.0)


    def finalize_child(self):
        print 'sent  %s' % self._msg


    def idle_cb(self):
        msg = {'cmd' : 'count', 
               'arg' : {'idx' : self._idx}}
        self.publish(rpc.CONTROL_PUBSUB, msg)
        self._idx += 1
        self._msg  = msg


# ------------------------------------------------------------------------------
#
def test():

    s = None
    try:
        cfg = ru.read_json("%s/session.json" % os.path.dirname(__file__))
        dh  = ru.DebugHelper()
        s   = rp.Session(cfg=cfg)

        ca1 = CompA(s)
        cb1 = CompB(s)
        cb2 = CompB(s)

        ca1.start()
        cb1.start()
        cb2.start()

      # s._controller.add_things([ca1, cb1, cb2])

        time.sleep(3)


    finally:
        if s:
            print 'close'
            s.close()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test()


# ------------------------------------------------------------------------------

