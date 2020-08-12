
import os
import copy
import time

import threading         as mt

import radical.utils     as ru

from .. import Session, ComputeUnitDescription
from .. import utils     as rpu
from .. import states    as rps
from .. import constants as rpc

from .request import Request


# ------------------------------------------------------------------------------
#
class Master(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, backend='zmq'):

        self._backend = backend  # FIXME: use

        self._lock     = ru.Lock('master')
        self._workers  = dict()     # wid: worker
        self._requests = dict()     # bookkeeping of submitted requests
        self._lock     = mt.Lock()  # lock the request dist on updates

        cfg.sid        = os.environ['RP_SESSION_ID']
        cfg.base       = os.environ['RP_PILOT_SANDBOX']
        cfg.path       = os.environ['RP_PILOT_SANDBOX']
        self._session  = Session(cfg=cfg, uid=cfg.sid, _primary=False)
        cfg            = self._get_config(cfg)

        rpu.Component.__init__(self, cfg, self._session)

        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)


        # set up RU ZMQ Queues for request distribution and result collection
        req_cfg = ru.Config(cfg={'channel'    : '%s.to_req' % self._uid,
                                 'type'       : 'queue',
                                 'uid'        : self._uid + '.req',
                                 'path'       : os.getcwd(),
                                 'stall_hwm'  : 0,
                                 'bulk_size'  : 56})

        res_cfg = ru.Config(cfg={'channel'    : '%s.to_res' % self._uid,
                                 'type'       : 'queue',
                                 'uid'        : self._uid + '.res',
                                 'path'       : os.getcwd(),
                                 'stall_hwm'  : 0,
                                 'bulk_size'  : 56})

        self._req_queue = ru.zmq.Queue(req_cfg)
        self._res_queue = ru.zmq.Queue(res_cfg)

        self._req_queue.start()
        self._res_queue.start()

        self._req_addr_put = str(self._req_queue.addr_put)
        self._req_addr_get = str(self._req_queue.addr_get)

        self._res_addr_put = str(self._res_queue.addr_put)
        self._res_addr_get = str(self._res_queue.addr_get)

        # this master will put requests onto the request queue, and will get
        # responses from the response queue.  Note that the responses will be
        # delivered via an async callback (`self._result_cb`).
        self._req_put = ru.zmq.Putter('%s.to_req' % self._uid,
                                      self._req_addr_put)
        self._res_get = ru.zmq.Getter('%s.to_res' % self._uid,
                                      self._res_addr_get,
                                      cb=self._result_cb)

        # for the workers it is the opposite: they will get requests from the
        # request queue, and will send responses to the response queue.
        self._info = {'req_addr_get': self._req_addr_get,
                      'res_addr_put': self._res_addr_put}


        # make sure the channels are up before allowing to submit requests
        time.sleep(1)

        # connect to the local agent
        self._log.debug('startup complete')


    # --------------------------------------------------------------------------
    #
    def _get_config(self, cfg=None):
        '''
        derive a worker base configuration from the control pubsub configuration
        '''

        # FIXME: this uses insider knowledge on the config location and
        #        structure.  It would be better if agent.0 creates the worker
        #        base config from scratch on startup.

        pwd = os.getcwd()
        ru.dict_merge(cfg, ru.read_json('%s/../control_pubsub.json' % pwd))

        del(cfg['channel'])
        del(cfg['cmgr'])

        cfg['log_lvl'] = 'debug'
        cfg['kind']    = 'master'
        cfg['base']    = pwd
        cfg['uid']     = ru.generate_id('master.%(item_counter)06d',
                                        ru.ID_CUSTOM,
                                        ns=self._session.uid)

        return ru.Config(cfg=cfg)


    # --------------------------------------------------------------------------
    #
    @property
    def workers(self):
        return self._workers


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'worker_register':

            uid  = arg['uid']
            info = arg['info']

            self._log.debug('register %s', uid)

            with self._lock:
                self._workers[uid]['info']  = info
                self._workers[uid]['state'] = 'ACTIVE'
                self._log.debug('info: %s', info)


        elif cmd == 'worker_unregister':

            uid = arg['uid']
            self._log.debug('unregister %s', uid)

            with self._lock:
                self._workers[uid]['state'] = 'DONE'


    # --------------------------------------------------------------------------
    #
    def submit(self, descr, count, cores, gpus):
        '''
        submit n workers, and pass the queue info as configuration file.
        Do *not* wait for them to come up
        '''

        # each worker gets the specified number of cores and gpus.  All
        # resources need to be located on the same node.
        descr['cpu_processes']   = 1
        descr['cpu_threads']     = cores
        descr['cpu_thread_type'] = 'POSIX'
        descr['gpu_processses']  = gpus


        tasks = list()
        for _ in range(count):

            # write config file for that worker
            cfg          = copy.deepcopy(self._cfg)
            cfg['info']  = self._info
            uid          = ru.generate_id('worker')
            sbox         = '%s/%s'      % (cfg['base'], uid)
            fname        = '%s/%s.json' % (sbox, uid)

            cfg['kind']  = 'worker'
            cfg['uid']   = uid
            cfg['base']  = sbox
            cfg['cores'] = cores
            cfg['gpus']  = gpus

            ru.rec_makedir(sbox)
            ru.write_json(cfg, fname)

            # grab default settings via CUD construction
            descr_complete = ComputeUnitDescription(descr).as_dict()

            # create task dict
            task = dict()
            task['description']       = copy.deepcopy(descr_complete)
            task['state']             = rps.AGENT_STAGING_INPUT_PENDING
            task['type']              = 'unit'
            task['uid']               = uid
            task['unit_sandbox_path'] = sbox
            task['unit_sandbox']      = 'file://localhost/' + sbox
            task['pilot_sandbox']     = cfg.base
            task['session_sandbox']   = cfg.base + '/../'
            task['resource_sandbox']  = cfg.base + '/../../'

            task['description']['arguments'] += [fname]

            tasks.append(task)
            self._workers[uid] = task

            self._log.debug('submit %s', uid)

        # insert the task
        self.advance(tasks, publish=False, push=True)


    # --------------------------------------------------------------------------
    #
    def wait(self, count=None, uids=None):
        '''
        wait for `n` workers, *or* for workers with given UID, *or* for all
        workers to become available, then return.
        '''

        if count:
            self._log.debug('wait for %d workers', count)
            while True:
                with self._lock:
                    states = [w['state'] for w in self._workers.values()]
                n = states.count('ACTIVE')
                self._log.debug('states [%d]: %s', n,
                                {k:states.count(k) for k in set(states)})
                if n >= count:
                    self._log.debug('wait ok')
                    return
                time.sleep(1)

        elif uids:
            self._log.debug('wait for workers: %s', uids)
            while True:
                with self._lock:
                    states = [self._workers[uid]['state'] for uid in uids]
                n = states.count('ACTIVE')
                self._log.debug('states [%d]: %s', n, states)
                if n == len(uids):
                    self._log.debug('wait ok')
                    return
                time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def create_work_items(self):
        '''
        This method MUST be implemented by the child class.  It is expected to
        return a list of work items.  When calling `run()`, the master will
        distribute those work items to the workers and collect the results.
        Run will finish once all work items are collected.

        NOTE: work items are expected to be serializable dictionaries.
        '''

        raise NotImplementedError('method be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def run(self):

        # get work from the overloading implementation
        self.create_work_items()

        # wait for the submitted requests to complete
        while True:

            # count completed items
            with self._lock:
                states = [req.state for req in self._requests.values()]

            completed = [s for s in states if s in ['DONE', 'FAILED']]

          # self._log.debug('%d =?= %d', len(completed), len(states))
            if len(completed) == len(states):
                break

            # FIXME: this should be replaced by an async state check.  Maybe
            #        subscrive to state updates on the update pubsub?
            time.sleep(5.0)


    # --------------------------------------------------------------------------
    #
    def request(self, req):
        '''
        submit a work request (function call spec) to the request queue
        '''

        # create request and add to bookkeeping dict.  That response object will
        # be updated once a response for the respective request UID arrives.
        req = Request(req=req)
        with self._lock:
            self._requests[req.uid] = req

        # push the request message (here and dictionary) onto the request queue
        self._req_put.put(req.as_dict())
      # self._log.debug('requested %s', req.uid)

        # return the request to the master script for inspection etc.
        return req


    # --------------------------------------------------------------------------
    #
    def result_cb(self, request):
        '''
        this method can be overloaded by the deriving class
        '''

        pass


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, msg):

      # self._log.debug('master _result_cb: %s', msg)

        # update result and error information for the corresponding request UID
        uid = msg['req']
        out = msg['out']
        err = msg['err']
        ret = msg['ret']

        req = self._requests[uid]
        req.set_result(out, err, ret)

        try:
            new_items = ru.as_list(self.result_cb([req]))
            for item in new_items:
                self.request(item)
        except:
            self._log.exception('result callback failed')


    # --------------------------------------------------------------------------
    #
    def terminate(self):
        '''
        terminate all workers
        '''

        for uid in self._workers:
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_terminate',
                                              'arg': {'uid': uid}})


# ------------------------------------------------------------------------------

