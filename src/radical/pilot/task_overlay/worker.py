
import sys
import time
import threading         as mt

import radical.utils     as ru

from .. import Session
from .. import utils     as rpu
from .. import constants as rpc


# ------------------------------------------------------------------------------
#
class Worker(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, info=None):

        if isinstance(cfg, str): cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : cfg = ru.Config(cfg=cfg)

        self._uid     = cfg.uid
        self._info    = info
        self._term    = mt.Event()
        self._methods = dict()
        self._info    = ru.Config(cfg=cfg.get('info', {}))
        self._session = Session(cfg=cfg, _primary=False)

        rpu.Component.__init__(self, cfg, self._session)

        # connect to master
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # connect to the request / response ZMQ queues
        self._req_get = ru.zmq.Getter('to_req', self._info.req_addr_get,
                                                cb=self._request_cb)
        self._res_put = ru.zmq.Putter('to_res', self._info.res_addr_put)

        # the worker can return custom information which will be made available
        # to the master.  This can be used to communicate, for example, worker
        # specific communication endpoints.

        # `info` is a placeholder for any additional meta data communicated to
        # the worker
        self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                          'arg': {'uid' : self._uid,
                                                  'info': self._info}})

    def register_method(self, name, method):

        assert(name not in self._methods)

        self._methods[name] = method


    # --------------------------------------------------------------------------
    #
    def _request_cb(self, msg):
        '''
        grep call type from msg, check if such a method is registered, and
        invoke it.
        '''
        self._log.debug('requested %s', msg)
        call   = msg['call']
        method = self._methods.get(call)
        res    = {'req': msg['uid'],
                  'err': None,
                  'res': None}

        if not method:
            res['err'] = 'no such call %s' % msg['call']

        else:
            args   = msg.get('args',   [])
            kwargs = msg.get('kwargs', {})
            try:
                res['res'] = method(*args, **kwargs)
            except Exception as e:
                self._log.exception('method call failed')
                res['err'] = str(e)

        self._res_put.put(res)


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        self._log.debug('got control msg: %s: %s', topic, msg)

        if msg['cmd'] == 'worker_terminate':
            if msg['arg']['uid'] == self._uid:
                self._term.set()
                self.stop()
                sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def run(self):

        while not self._term.is_set():
            time.sleep(1)


# ------------------------------------------------------------------------------

