
import os
import sys
import copy
import time

import threading         as mt

import radical.utils     as ru

from .. import Session, TaskDescription
from .. import utils     as rpu
from .. import states    as rps
from .. import constants as rpc

from .request import Request


def out(msg):
    sys.stdout.write('%s\n' % msg)
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
class Master(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, backend='zmq'):

        self._backend = backend  # FIXME: use

        self._lock     = ru.Lock('master')
        self._workers  = dict()      # wid: worker
        self._requests = dict()      # bookkeeping of submitted requests
        self._lock     = mt.Lock()   # lock the request dist on updates
        self._term     = mt.Event()  # termination signal
        self._thread   = None        # run loop

        cfg.sid        = os.environ['RP_SESSION_ID']
        cfg.base       = os.environ['RP_PILOT_SANDBOX']
        cfg.path       = os.environ['RP_PILOT_SANDBOX']
        self._session  = Session(cfg=cfg, uid=cfg.sid, _primary=False)
        cfg            = self._get_config(cfg)

        rpu.Component.__init__(self, cfg, self._session)

        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        self.register_subscriber(rpc.STATE_PUBSUB,   self._state_cb)
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
    def _state_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'update':

            for thing in ru.as_list(arg):

                uid   = thing['uid']
                state = thing['state']

                if uid in self._workers:
                    if state == rps.AGENT_STAGING_OUTPUT:
                        with self._lock:
                            self._workers[uid]['state'] = 'DONE'

        return True


    # --------------------------------------------------------------------------
    #
    def submit(self, descr, count, cores, gpus):
        '''
        submit n workers, and pass the queue info as configuration file.
        Do *not* wait for them to come up
        '''

        # each worker gets the specified number of cores and gpus.  All
        # resources need to be located on the same node.
        descr['cpu_processes']    = count
        descr['cpu_process_type'] = 'MPI'
        descr['cpu_threads']      = cores
        descr['cpu_thread_type']  = 'POSIX'
        descr['gpu_processses']   = gpus

        # write config file for all worker ranks.  The worker will live in the
        # master sandbox
        # NOTE: the uid generated here is for the worker MPI task, not for the
        #       worker processes (ranks)
        cfg          = copy.deepcopy(self._cfg)
        cfg['info']  = self._info
        uid          = ru.generate_id('worker.%(item_counter)06d',
                                    ru.ID_CUSTOM,
                                    ns=self._session.uid)
        sbox         = os.getcwd()
        fname        = '%s/%s.json' % (sbox, uid)

        cfg['kind']  = 'worker'
        cfg['uid']   = uid
        cfg['base']  = sbox
        cfg['cores'] = cores
        cfg['gpus']  = gpus

        ru.rec_makedir(sbox)
        ru.write_json(cfg, fname)

        # grab default settings via TD construction
        descr_complete = TaskDescription(descr).as_dict()

        # create task dict
        td = copy.deepcopy(descr_complete)
        td['arguments'] += [fname]

        task = dict()
        task['description']       = td
        task['state']             = rps.AGENT_STAGING_INPUT_PENDING
        task['type']              = 'task'
        task['umgr']              = 'umgr.0000'  # FIXME
        task['pilot']             = os.environ['RP_PILOT_ID']
        task['uid']               = uid
        task['task_sandbox_path'] = sbox
        task['task_sandbox']      = 'file://localhost/' + sbox
        task['pilot_sandbox']     = cfg.base
        task['session_sandbox']   = cfg.base + '/../'
        task['resource_sandbox']  = cfg.base + '/../../'
        task['resources']         = {'cpu': td['cpu_processes'] *
                                            td.get('cpu_threads', 1),
                                     'gpu': td['gpu_processes']}

        # NOTE: the order of insert / state update relies on that order
        # being maintained through the component's message push, the update
        # worker's message receive up to the insertion order into the update
        # worker's DB bulk op.
        self._log.debug('insert %s', uid)
        self.publish(rpc.STATE_PUBSUB, {'cmd': 'insert', 'arg': task})

        self._log.debug('submit %s', uid)
        self.advance(task, publish=True, push=True)

        with self._lock:
            self._workers[uid] = dict()
            self._workers[uid]['state'] = 'NEW'


    # --------------------------------------------------------------------------
    #
    def wait(self, count=None, uids=None):
        '''
        wait for `n` workers, *or* for workers with given UID, *or* for all
        workers to become available, then return.
        '''

        if not count and not uids:
            uids = list(self._workers.keys())

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
    def start(self):

        self._thread = mt.Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):

        self._term.set()
        if self._thread:
            self._thread.join()

        rpu.Component.stop(self, timeout=timeout)

        # FIXME: this *should* get triggered by the base class
        self.terminate()


    # --------------------------------------------------------------------------
    #
    def alive(self):

        if not self._thread or self._term.is_set():
            return False
        return True


    # --------------------------------------------------------------------------
    #
    def join(self):

        if self._thread:
            self._thread.join()


    # --------------------------------------------------------------------------
    #
    def _run(self):

        # get work from the overloading implementation
        try:
            self.create_work_items()
        except Exception:
            self._log.exception('failed to create work')
            self._term.set()

        # wait for the submitted requests to complete
        while not self._term.is_set():

            # count completed items
            with self._lock:
                states = [req.state for req in self._requests.values()]

            completed = [s for s in states if s in ['DONE', 'FAILED']]

            self._log.debug('%d =?= %d', len(completed), len(states))
            if len(completed) == len(states):
                break

            # FIXME: this should be replaced by an async state check.  Maybe
            #        subscrive to state updates on the update pubsub?
            time.sleep(1.0)

        self._log.debug('=== master term')


    # --------------------------------------------------------------------------
    #
    def request(self, reqs):
        '''
        submit a list of work request (function call spec) to the request queue
        '''

        reqs  = ru.as_list(reqs)
        dicts = list()
        objs  = list()

        # create request and add to bookkeeping dict.  That response object will
        # be updated once a response for the respective request UID arrives.
        with self._lock:
            for req in reqs:
                request = Request(req=req)
                self._requests[request.uid] = request
                dicts.append(request.as_dict())
                objs.append(request)

        # push the request message (as dictionary) onto the request queue
        self._log.debug('=== put %d: [%s]', len(dicts),
                         [r['uid'] for r in dicts])
        self._req_put.put(dicts)

        # return the request to the master script for inspection etc.
        return objs


    # --------------------------------------------------------------------------
    #
    def result_cb(self, requests):
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

        self._term.set()
        for uid in self._workers:
            self._log.debug('=== master %s sends term to %s', self._uid, uid)
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_terminate',
                                              'arg': {'uid': uid}})

        # wait for workers to terminate
        uids = self._workers.keys()
        while True:
            states = [self._workers[uid]['state'] for uid in uids]
            if set(states) == {'DONE'}:
                break
            self._log.debug('=== states: %s', states)
            time.sleep(1)

        self._log.debug('=== all workers terminated')


# ------------------------------------------------------------------------------

