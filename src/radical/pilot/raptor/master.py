
import copy
import json
import os
from re import L
import sys
import time

from typing import Dict, Union

import threading         as mt

import radical.utils     as ru

from .. import utils     as rpu
from .. import states    as rps
from .. import constants as rpc

from .. import Session, Task, TaskDescription

from .request import Request


def out(msg):
    sys.stdout.write('%s\n' % msg)
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
class Master(rpu.Component):

    # FIXME: instead of the input and output queue, use the default component
    #        facilities

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, backend='zmq'):

        self._backend  = backend     # FIXME: use

        self._uid      = os.environ['RP_TASK_ID']
        self._name     = os.environ['RP_TASK_NAME']

        self._workers  = dict()      # wid: worker
        self._tasks    = dict()      # bookkeeping of submitted requests
        self._lock     = mt.Lock()   # lock the request dist on updates
        self._term     = mt.Event()  # termination signal
        self._thread   = None        # run loop

        if not cfg:
            cfg = ru.Config(cfg={})

        cfg.sid        = os.environ['RP_SESSION_ID']
        cfg.base       = os.environ['RP_PILOT_SANDBOX']
        cfg.path       = os.environ['RP_PILOT_SANDBOX']
        self._session  = Session(cfg=cfg, uid=cfg.sid, _primary=False)
        cfg            = self._get_config(cfg)

        rpu.Component.__init__(self, cfg, self._session)

        self.register_publisher(rpc.STATE_PUBSUB)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        self.register_subscriber(rpc.STATE_PUBSUB,   self._state_cb)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)

        # send new worker tasks and agent input staging / agent scheduler
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)

        # set up zmq queues between the agent scheduler and this master so that
        # we can receive new requests from RP tasks
        qname = '%s_input_queue' % self._uid
        input_cfg = ru.Config(cfg={'channel'   : qname,
                                   'type'      : 'queue',
                                   'uid'       : '%s_input' % self._uid,
                                   'path'      : os.getcwd(),
                                   'stall_hwm' : 0,
                                   'bulk_size' : 56})

        self._input_queue = ru.zmq.Queue(input_cfg)
        self._input_queue.start()

        # send completed request tasks to agent output staging / tmgr
        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        # set up zmq queues between this master and all workers for request
        # distribution and result collection
        req_cfg = ru.Config(cfg={'channel'   : 'request',
                                 'type'      : 'queue',
                                 'uid'       : self._uid + '.req',
                                 'path'      : os.getcwd(),
                                 'stall_hwm' : 0,
                                 'bulk_size' : 1024})

        res_cfg = ru.Config(cfg={'channel'   : 'result',
                                 'type'      : 'queue',
                                 'uid'       : self._uid + '.res',
                                 'path'      : os.getcwd(),
                                 'stall_hwm' : 0,
                                 'bulk_size' : 1024})

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
        self._req_put = ru.zmq.Putter('request', self._req_addr_put)
        self._res_get = ru.zmq.Getter('result',  self._res_addr_get,
                                      cb=self._result_cb)

        # for the workers it is the opposite: they will get requests from the
        # request queue, and will send responses to the response queue.
        self._info = {'req_addr_get': self._req_addr_get,
                      'res_addr_put': self._res_addr_put}


        # make sure the channels are up before allowing to submit requests
        time.sleep(1)

        # set up zmq queues between the agent scheduler and this master so that
        # we can receive new requests from RP tasks
        qname = '%s_input_queue' % self._uid
        input_cfg = ru.Config(cfg={'channel'   : qname,
                                   'type'      : 'queue',
                                   'uid'       : '%s_input' % self._uid,
                                   'path'      : os.getcwd(),
                                   'stall_hwm' : 0,
                                   'bulk_size' : 56})


        # begin to receive tasks in that queue
        self._input_getter = ru.zmq.Getter(qname,
                                      self._input_queue.addr_get,
                                      cb=self._request_cb)

        # and register that input queue with the scheduler
        self._log.debug('registered raptor queue')
        self.publish(rpc.CONTROL_PUBSUB,
                      {'cmd': 'register_raptor_queue',
                       'arg': {'name' : self._uid,
                               'queue': qname,
                               'addr' : str(self._input_queue.addr_put)}})

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

        if cfg and 'path' in cfg:
            del(cfg['path'])

        ru.dict_merge(cfg, ru.read_json('%s/../control_pubsub.json' % pwd))

        del(cfg['channel'])
        del(cfg['cmgr'])

        cfg['log_lvl'] = 'debug'
        cfg['kind']    = 'master'
        cfg['base']    = pwd

        cfg = ru.Config(cfg=cfg)
        cfg['uid'] = self._uid

        return cfg


    # --------------------------------------------------------------------------
    #
    @property
    def workers(self):
        return self._workers


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):

        return tasks


    # --------------------------------------------------------------------------
    #
    def result_cb(self, req):

        pass


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
                if 'uid' not in self._workers:
                    return
                self._workers[uid]['info']  = info
                self._workers[uid]['state'] = 'ACTIVE'

            self._log.debug('info: %s', info)


        elif cmd == 'worker_unregister':

            uid = arg['uid']
            self._log.debug('unregister %s', uid)

            with self._lock:
                if 'uid' not in self._workers:
                    return
                self._workers[uid]['status'] = 'DONE'


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
    def submit_workers(self, descr: Dict[str, Union[str, int]],
                             count: int) -> None:
        '''
        Submit`count` workers per given `descr`, and pass the queue info as
        configuration file.  Do *not* wait for the workers to come up - they are
        expected to register via the control channel.

        The `descr` dict is expected to support the following keys:

          - named_env       : environment to use (same as master usually)
          - ranks           : number of MPI ranks per worker
          - cores_per_rank  : int, number of cores per worker rank
          - gpus_per_rank   : int, number of gpus per worker rank
          - worker_class    : str, type name of worker class to execute
          - worker_file     : str, optional if an RP worker class is used

        Note that only one rank (presumably rank 0) should register with the
        master - the worker ranks are expected to syncronize their ranks as
        needed.
        '''
        with self._lock:

            tasks    = list()
            base     = descr['worker_class'].split('.')[-1].lower()

            cfg          = copy.deepcopy(self._cfg)
            cfg['descr'] = descr
            cfg['info']  = self._info

            for i in range(count):

                uid        = '%s.%04d' % (base, i)
                cfg['uid'] = uid

                fname = './%s.json' % uid
                ru.write_json(cfg, fname)

                td = dict()
                td['named_env']        = descr.get('named_env')
                td['cpu_processes']    = descr['cpu_processes']
                td['cpu_process_type'] = 'MPI'
                td['cpu_thread_type']  = 'POSIX'
                td['cpu_threads']      = descr.get('cpu_threads', 1)
                td['gpu_processes']    = descr.get('gpu_processes', 0)

                # this master is obviously running in a suitable python3 env,
                # so we expect that the same env is also suitable for the worker
                td['executable'] = 'python3'
                td['arguments']  = [
                        '-c',
                        'import radical.pilot as rp; '
                        'rp.raptor.Worker.run("%s", "%s", "%s")'
                            % (descr.get('worker_file', ''),
                               descr.get('worker_class', 'DefaultWorker'),
                               fname)]



                # all workers run in the same sandbox as the master
                sbox = os.getcwd()
                task = dict()

                task['description']       = TaskDescription(td).as_dict()
                task['state']             = rps.AGENT_STAGING_INPUT_PENDING
                task['status']            = 'NEW'
                task['type']              = 'task'
                task['uid']               = uid
                task['task_sandbox_path'] = sbox
                task['task_sandbox']      = 'file://localhost/' + sbox
                task['pilot_sandbox']     = os.environ['RP_PILOT_SANDBOX']
                task['session_sandbox']   = os.environ['RP_SESSION_SANDBOX']
                task['resource_sandbox']  = os.environ['RP_RESOURCE_SANDBOX']
                task['pilot']             = os.environ['RP_PILOT_ID']
              # task['tmgr']              = 'tmgr.0000'  # FIXME
                task['resources']         = {'cpu': td['cpu_processes'] *
                                                    td.get('cpu_threads', 1),
                                             'gpu': td['gpu_processes']}
                tasks.append(task)

                # NOTE: the order of insert / state update relies on that order
                #       being maintained through the component's message push,
                #       the update worker's message receive up to the insertion
                #       order into the update worker's DB bulk op.
                self._log.debug('insert %s', uid)
                self.publish(rpc.STATE_PUBSUB, {'cmd': 'insert', 'arg': task})

                self._workers[uid] = dict()
                self._workers[uid]['state'] = 'NEW'

            self.advance(tasks, publish=True, push=True)


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
                stats = {'NEW'    : 0,
                         'ACTIVE' : 0,
                         'DONE'   : 0,
                         'FAILED' : 0}

                with self._lock:
                    for w in self._workers.values():
                        stats[w['status']] += 1

                self._log.debug('stats: %s', stats)
                n = stats['ACTIVE'] + stats['DONE'] + stats['FAILED']
                if n >= count:
                    self._log.debug('wait ok')
                    return
                time.sleep(10)

        elif uids:
            self._log.debug('wait for workers: %s', uids)
            while True:
                with self._lock:
                    stats = [self._workers[uid]['status'] for uid in uids]
                n = stats['ACTIVE'] + stats['DONE'] + stats['FAILED']
                self._log.debug('stats [%d]: %s', n, stats)
                if n == len(uids):
                    self._log.debug('wait ok')
                    return
                time.sleep(1)


    # --------------------------------------------------------------------------
    #
    def start(self):

        self._thread = mt.Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()


    # --------------------------------------------------------------------------
    #
    def stop(self, timeout=None):

        self._log.debug('set term from stop: %s', ru.get_stacktrace())
        self._term.set()

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

        # FIXME: only now subscribe to request input channels

        # wait for the submitted requests to complete
        while not self._term.is_set():

            self._log.debug('still alive')

            time.sleep(1.0)


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, tasks):
        '''
        submit a list of tasks to the task queue
        '''

        # `tasks` can be task instances, task dicts, task description instances,
        # or task description dicts...
        task_dicts = list()
        for thing in ru.as_list(tasks):
            try:
                if isinstance(thing, Task):
                    task_dicts.append(thing.as_dict())
                elif isinstance(thing, TaskDescription):
                    task = Task(self, thing)
                    task_dicts.append(task.as_dict())
                elif isinstance(thing, dict):
                    if 'description' in thing:
                        task_dicts.append(thing)
                    else:
                        task = Task(self, TaskDescription(thing))
                        task_dicts.append(task.as_dict())
            except:
                import pprint
                self._log.exception('=== %s\n', pprint.pformat(td))


        # push the request message (as dictionary) onto the request queue
        self._req_put.put(task_dicts)


    # --------------------------------------------------------------------------
    #
    def _request_cb(self, tasks):

        tasks = ru.as_list(tasks)

        try:
            filtered = self.request_cb(tasks)
            if filtered:
                self.submit_tasks(filtered)

        except:
            self._log.exception('request cb failed')
            # FIXME: fail the request


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, tasks):

        for task in ru.as_list(tasks):

            try:
                self.result_cb(task)

            except:
                self._log.exception('result callback failed')

            ret = task['exit_code']
            if ret == 0: task['target_state'] = rps.DONE
            else       : task['target_state'] = rps.FAILED

            self.advance(task, rps.AGENT_STAGING_OUTPUT_PENDING,
                               publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def terminate(self):
        '''
        terminate all workers
        '''

        # unregister input queue
        self.publish(rpc.CONTROL_PUBSUB, {
                    'cmd': 'unregister_raptor_queue',
                    'arg': {'name' : self._uid,
                            'queue': self._input_queue.channel}})

        self._log.debug('set term from terminate')
        self._term.set()
        for uid in self._workers:
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_terminate',
                                              'arg': {'uid': uid}})

        # wait for workers to terminate
        uids = self._workers.keys()
        while True:
            states = [self._workers[uid]['state'] for uid in uids]
            if set(states) == {'DONE'}:
                break
            self._log.debug('term states: %s', states)
            time.sleep(1)

        self._log.debug('all workers terminated')


# ------------------------------------------------------------------------------

