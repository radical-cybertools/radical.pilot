
import os
import sys
import time

import radical.utils     as ru

from .. import constants as rpc


# ------------------------------------------------------------------------------
#
class Worker(object):
    '''
    Implement the Raptor protocol for dispatching multiple Tasks on persistent
    resources.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, register=True, session=None):

        self._session = session

        if cfg is None:
            cfg = dict()

        if isinstance(cfg, str): self._cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : self._cfg = ru.Config(cfg=cfg)

        self._uid  = os.environ['RP_TASK_ID']
        self._log  = ru.Logger(name=self._uid,   ns='radical.pilot.worker',
                               level='DEBUG', targets=['.'], path=os.getcwd())
        self._prof = ru.Profiler(name=self._uid, ns='radical.pilot.worker')

        if register:

            ppath    = os.environ['RP_PILOT_SANDBOX']
            ctrl_cfg = ru.read_json('%s/%s.cfg' % (ppath, rpc.CONTROL_PUBSUB))

            self._control_sub = ru.zmq.Subscriber(rpc.CONTROL_PUBSUB,
                                                  url=ctrl_cfg['sub'],
                                                  log=self._log,
                                                  prof=self._prof,
                                                  cb=self._control_cb)

            self._ctrl_pub = ru.zmq.Publisher(rpc.CONTROL_PUBSUB,
                                              url=ctrl_cfg['pub'],
                                              log=self._log,
                                              prof=self._prof)
            # let ZMQ settle
            time.sleep(1)

            # `info` is a placeholder for any additional meta data
            self._ctrl_pub.put(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                                    'arg': {'uid' : self._uid,
                                                            'info': {}}})

          # # FIXME: we never unregister on termination
          # self._ctrl_pub.put(rpc.CONTROL_PUBSUB, {'cmd': 'worker_unregister',
          #                                         'arg': {'uid' : self._uid}})


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def run(fpath, cname, cfg):

        # load worker class from fname if that is a valid string
        wclass = None

        # Create the worker class and run it's work loop.
        if fpath:
            wclass = ru.load_class(fpath, cname, Worker)

        else:
            # import all known workers into the local name space so that
            # `get_type` has a chance to find them

            from .worker_default import DefaultWorker  # pylint: disable=W0611 # noqa
            from .worker_mpi     import MPIWorker      # pylint: disable=W0611 # noqa

            wclass = ru.get_type(cname)

        if not wclass:
            raise RuntimeError('no worker [%s] [%s]' % (cname, fpath))

        worker = wclass(cfg)
        worker.start()
        worker.join()


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        if msg['cmd'] == 'terminate':
            self.stop()
            self.join()
            sys.exit()

        elif msg['cmd'] == 'worker_terminate':
            self._log.debug('worker_terminate signal')
            if msg['arg']['uid'] == self._uid:
                self.stop()
                self.join()
                sys.exit()


    # --------------------------------------------------------------------------
    #
    def get_master(self):
        '''
        The worker can submit tasks back to the master - this method will
        return a small shim class to provide that capability
        '''

        # ----------------------------------------------------------------------
        class Master(object):

            def __init__(self, addr):
                self._task_service_ep = ru.zmq.Client(url=addr)

            def run_task(self, td):
                return self._task_service_ep.request('run_task', td)
        # ----------------------------------------------------------------------

        return Master(self._cfg.ts_addr)


    # --------------------------------------------------------------------------
    #
    def start(self):

        raise NotImplementedError('`start()` must be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def stop(self):

        raise NotImplementedError('`run()` must be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def join(self):

        raise NotImplementedError('`join()` must be implemented by child class')


# ------------------------------------------------------------------------------

