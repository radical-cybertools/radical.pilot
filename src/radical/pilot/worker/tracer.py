
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import sys
import time

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

      # self.register_input(states=None, input=rpc.TRACER_QUEUE,
      #                     worker=self._work)
        cfg = ru.read_json('tracer_queue.cfg')
        self._getter = ru.zmq.Getter(rpc.TRACER_QUEUE, cfg['get'],
                                      cb=self._work)

        # FIXME: storage backend should be pluggable
        self._handle = open('./traces.prof', 'a')


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg, session):

        return cls(cfg, session)


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
              # self._handle.flush()
        except:
            self._log.exception('oops')


# ------------------------------------------------------------------------------

