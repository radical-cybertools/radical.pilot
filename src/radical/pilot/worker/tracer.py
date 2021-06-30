
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import time

import threading     as mt

import radical.utils as ru

from ..   import utils     as rpu
from ..   import constants as rpc


# ------------------------------------------------------------------------------
#
class Tracer(rpu.Worker):
    '''
    An tracer collects trace events from the tracer queue and forwards them to
    some storage backend.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        rpu.Worker.__init__(self, cfg, session)

        self._handle = None
        self._count  = 0

        self._stats_thread = mt.Thread(target=self._stats)
        self._stats_thread.daemon = True
        self._stats_thread.start()


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        if self._handle:
            self._handle.flush()
            self._handle.close()
            self._handle = None


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        cfg = ru.read_json('tracer_queue.cfg')
        self._getter = ru.zmq.Getter(rpc.TRACER_QUEUE, cfg['get'],
                                      cb=self._work)

        # FIXME: storage backend should be pluggable
        self._handle = open('./traces.prof', 'a')

        # announce to the world (via the `CONTROL_PUBSUB`) that the tracer can
        # be used
        self.register_publisher(rpc.CONTROL_PUBSUB)
        self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'config_tracer',
                                          'arg': {'target': cfg['put']}})


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg, session):

        return cls(cfg, session)


    # --------------------------------------------------------------------------
    #
    def _stats(self):

        with open('./tracer.stats', 'w') as fout:
            while True:
                fout.write('%10.1f  %10d\n' % (time.time(), self._count))
                time.sleep(0.1)


    # --------------------------------------------------------------------------
    #
    def _work(self, traces):

      # import pprint

        try:
            for trace in ru.as_list(traces):
              # self._log.debug('===> %s', pprint.pformat(trace))
                ts, event, comp, thread, uid, state, msg = trace
                self._handle.write('%.7f,%s,%s,%s,%s,%s,%s\n' %
                               (ts, event, comp, thread, uid, state, msg))
                self._count += 1
        except:
            self._log.exception('oops')


# ------------------------------------------------------------------------------

