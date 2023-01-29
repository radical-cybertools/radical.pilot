
import os
import sys
import copy
import time


from collections import defaultdict
from typing      import Dict, Union

import threading         as mt

import radical.utils     as ru

from .. import utils     as rpu
from .. import states    as rps
from .. import constants as rpc

from .. import Session, Task, TaskDescription, TASK_EXECUTABLE

from ..task_description import RAPTOR_WORKER


def out(msg):
    sys.stdout.write('%s\n' % msg)
    sys.stdout.flush()


# ------------------------------------------------------------------------------
#
class Master(rpu.Component):

    NEW    = 'NEW'
    ACTIVE = 'ACTIVE'
    DONE   = 'DONE'


    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None):

        self._uid        = os.environ['RP_TASK_ID']
        self._sid        = os.environ['RP_SESSION_ID']
        self._name       = os.environ['RP_TASK_NAME']
        self._sbox       = os.environ['RP_TASK_SANDBOX']
        self._psbox      = os.environ['RP_PILOT_SANDBOX']

        self._workers    = dict()      # wid: worker
        self._tasks      = dict()      # bookkeeping of submitted requests
        self._term       = mt.Event()  # termination signal
        self._thread     = None        # run loop

        self._hb_freq    = 10          # check worker heartbetas every n seconds
        self._hb_timeout = 15          # consider worker dead after 15 seconds

        cfg              = self._get_config(cfg)
        self._session    = Session(cfg=cfg, uid=cfg.sid, _primary=False)

        rpu.Component.__init__(self, cfg, self._session)

        self.register_publisher(rpc.STATE_PUBSUB,   self._psbox)
        self.register_publisher(rpc.CONTROL_PUBSUB, self._psbox)

        self.register_subscriber(rpc.STATE_PUBSUB,   self._state_cb,   self._psbox)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb, self._psbox)

        # send new worker tasks and agent input staging / agent scheduler
        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE, self._psbox)

        # set up zmq queues between the agent scheduler and this master so that
        # we can receive new requests from RP tasks
        qname = '%s.input_queue' % self._uid
        input_cfg = ru.Config(cfg={'channel'   : qname,
                                   'type'      : 'queue',
                                   'uid'       : '%s_input' % self._uid,
                                   'path'      : self._sbox,
                                   'stall_hwm' : 0,
                                   'bulk_size' : 1})

        self._input_queue = ru.zmq.Queue(input_cfg)
        self._input_queue.start()

        # send completed request tasks to agent output staging / tmgr
        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE, self._psbox)

        # set up zmq queues between this master and all workers for request
        # distribution and result collection
        req_cfg = ru.Config(cfg={'channel'   : 'raptor_tasks',
                                 'type'      : 'queue',
                                 'uid'       : self._uid + '.req',
                                 'path'      : self._sbox,
                                 'stall_hwm' : 0,
                                 'bulk_size' : 1})

        res_cfg = ru.Config(cfg={'channel'   : 'raptor_results',
                                 'type'      : 'queue',
                                 'uid'       : self._uid + '.res',
                                 'path'      : self._sbox,
                                 'stall_hwm' : 0,
                                 'bulk_size' : 1})

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
        self._req_put = ru.zmq.Putter('raptor_tasks',    self._req_addr_put)
        self._res_get = ru.zmq.Getter('raptor_results',  self._res_addr_get,
                                                         cb=self._result_cb)

        # for the workers it is the opposite: they will get requests from the
        # request queue, and will send responses to the response queue.
        self._info = {'req_addr_get': self._req_addr_get,
                      'res_addr_put': self._res_addr_put}

        # make sure the channels are up before allowing to submit requests
        time.sleep(1)

        # begin to receive tasks in that queue
        ru.zmq.Getter(qname, self._input_queue.addr_get, cb=self._request_cb)

        # and register that input queue with the scheduler
        self._log.debug('registered raptor queue')
        self.publish(rpc.CONTROL_PUBSUB,
                      {'cmd': 'register_raptor_queue',
                       'arg': {'name' : self._uid,
                               'queue': qname,
                               'addr' : str(self._input_queue.addr_put)}})

        # also create a ZMQ server endpoint for the workers to
        # send task execution requests back to the master
        self._task_service = ru.zmq.Server()
        self._task_service.register_request('run_task', self._run_task)
        self._task_service.start()
        self._task_service_data = dict()   # task.uid : [mt.Event, task]

        # all comm channels are set up - begin to work
        self._log.debug('startup complete')


    # --------------------------------------------------------------------------
    #
    def _get_config(self, cfg=None):
        '''
        derive a worker base configuration from the control pubsub configuration
        '''

        # FIXME: use registry for comm EP info exchange, not cfg files

        if cfg is None:
            cfg = dict()

        if cfg and 'path' in cfg:
            del cfg['path']

        ru.dict_merge(cfg, ru.read_json('%s/control_pubsub.json' % self._psbox))

        del cfg['channel']
        del cfg['cmgr']

        cfg['log_lvl'] = 'warn'
        cfg['kind']    = 'master'
        cfg['sid']     = self._sid
        cfg['base']    = self._sbox
        cfg['path']    = self._sbox

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
    def _control_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        if cmd == 'worker_register':

            uid = arg['uid']

            self._log.debug('register %s', uid)

            if uid not in self._workers:
                return
            self._workers[uid]['status'] = self.ACTIVE


        elif cmd == 'worker_rank_heartbeat':

            uid  = arg['uid']
            rank = arg['rank']

            self._log.debug('=== recv rank heartbeat %s:%s', uid, rank)

            if uid not in self._workers:
                return

            self._workers[uid]['heartbeats'][rank] = time.time()


        elif cmd == 'worker_unregister':

            uid = arg['uid']
            self._log.debug('unregister %s', uid)

            if uid not in self._workers:
                return

            self._workers[uid]['status'] = self.DONE


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, topic, msg):

        cmd = msg['cmd']
        arg = msg['arg']

        # state update for tasks created by raptor
        if cmd == 'raptor_state_update':

            tasks = ru.as_list(arg)

            for task in tasks:
                uid   = task['uid']
                state = task['state']

                if uid in self._task_service_data:
                    # update task info and signal task service thread
                    self._log.debug('unlock 2 %s', uid)
                    self._task_service_data[uid][1] = task
                    self._task_service_data[uid][0].set()


        # general task state updates -- check if our workers are affected
        elif cmd == 'update':

            for thing in ru.as_list(arg):

                uid   = thing['uid']
                state = thing['state']

                if uid in self._workers:

                    if state == rps.AGENT_STAGING_OUTPUT:
                        self._workers[uid]['status'] = self.DONE
                        self._log.info('worker %s final: %s', uid, state)

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
        tasks = list()
        base  = self._uid

        worker_file  = descr.pop('worker_file', '')
        worker_class = descr.pop('worker_class', 'DefaultWorker')

        td = TaskDescription(descr)
        td.mode       = RAPTOR_WORKER
        td.executable = 'radical-pilot-raptor-worker'

        # ensure that defaults and backward compatibility kick in
        td.verify()

        cfg                   = copy.deepcopy(self._cfg)
        cfg['info']           = self._info
        cfg['ts_addr']        = self._task_service.addr
        cfg['cores_per_rank'] = td.cores_per_rank
        cfg['gpus_per_rank']  = td.gpus_per_rank
        cfg['ranks']          = td.ranks

        for i in range(count):

            uid        = '%s.worker.%04d' % (base, i)
            cfg['uid'] = uid

            now   = time.time()
            fname = './%s.json' % uid
            ru.write_json(cfg, fname)

            # this master is obviously running in a suitable python3 env,
            # so we expect that the same env is also suitable for the worker
            # NOTE: shell escaping is a bit tricky here - careful on change!
            td.arguments  = [worker_file, worker_class, fname]

            # all workers run in the same sandbox as the master
            task = dict()

            task['origin']            = 'raptor'
            task['description']       = td.as_dict()
            task['state']             = rps.AGENT_STAGING_INPUT_PENDING
            task['status']            = self.NEW
            task['type']              = 'task'
            task['uid']               = uid
            task['task_sandbox_path'] = self._sbox
            task['task_sandbox']      = 'file://localhost/' + self._sbox
            task['pilot_sandbox']     = os.environ['RP_PILOT_SANDBOX']
            task['session_sandbox']   = os.environ['RP_SESSION_SANDBOX']
            task['resource_sandbox']  = os.environ['RP_RESOURCE_SANDBOX']
            task['pilot']             = os.environ['RP_PILOT_ID']
            task['resources']         = {'cpu': td.ranks * td.cores_per_rank,
                                         'gpu': td.ranks * td.gpus_per_rank}
            tasks.append(task)

            # NOTE: the order of insert / state update relies on that order
            #       being maintained through the component's message push,
            #       the update worker's message receive up to the insertion
            #       order into the update worker's DB bulk op.
            self._log.debug('insert %s', uid)
            self.publish(rpc.STATE_PUBSUB, {'cmd': 'insert', 'arg': task})

            self._workers[uid] = {
                    'uid'        : uid,
                    'status'     : self.NEW,
                    'heartbeats' : {r: now for r in range(td.ranks)},
                    'description': task['description']
            }

        self.advance(tasks, publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def wait(self, count=None, uids=None):
        '''
        wait for `n` workers, *or* for workers with given UID, *or* for all
        workers to become available, then return.
        '''

        if uids:
            uids = [uid for uid in uids if uid in self._workers]
        else:
            uids = list(self._workers.keys())

        if not count or count > len(uids):
            count = len(uids)

        self._log.debug('wait for %d workers: %s', count, uids)


        while True:

            stats = defaultdict(int)
            for uid in uids:
                stats[self._workers[uid]['status']] += 1

            n_done = stats[self.DONE]
            self._log.debug('stats [%d]: %s', n_done, stats)

            if n_done >= count:
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
    def stop(self):

        self._log.debug('set term from stop: %s', ru.get_stacktrace())
        self._term.set()

        rpu.Component.stop(self)

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

        # wait for the submitted requests to complete
        while True:

            if self._term.is_set():
                break

            self._log.debug('still alive')

            # check worker heartbeats
            now  = time.time()
            lost = set()
            for uid in self._workers:
                for rank, hb in self._workers[uid]['heartbeats'].items():
                    if hb < now - self._hb_timeout:
                        self._log.warn('lost rank %d on worker %s', rank, uid)
                        lost.add(uid)

            time.sleep(self._hb_freq)

            for uid in lost:
                self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_terminate',
                                                  'arg': {'uid': uid}})
                self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'cancel_tasks',
                                                  'arg': {'uids': [uid]}})
        self._log.debug('terminate run loop')


    # --------------------------------------------------------------------------
    #
    def _run_task(self, td):
        '''
        accept a single task request for execution, execute it and wait for it's
        completion before returning the call.

        Note: this call is running in a separate thread created by an ZMQ
              Server instance and will thus not block the master's progress.
        '''

        # we get a dict but want a proper `TaskDescription` instance
        td = TaskDescription(td)

        # Create a new task ID for the submitted task (we do not allow
        # user-specified IDs in this case as we can't check uniqueness with the
        # client tmgr).  Then submit that task and wait for the `result_cb` to
        # report completion of the task

        tid   = '%s.%s' % (self.uid, ru.generate_id('subtask'))
        event = mt.Event()

        td['uid'] = tid

        self._task_service_data[tid] = [event, td]
        self.submit_tasks([td])

        # wait for the result cb to pick up the td again
        event.wait()

        # update td info and remove data
        task = self._task_service_data[tid][1]
        del self._task_service_data[tid]

        # the task is completed and we can return it to the caller
        return task


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, tasks):
        '''
        submit a list of tasks to the task queue
        We expect to get either `TaskDescription` instances which will then get
        converted into task dictionaries and pushed out, or we get task
        dictionaries which are used as is.
        '''

        raptor_tasks     = list()
        executable_tasks = list()
        for task in ru.as_list(tasks):

            if isinstance(task, TaskDescription):
                # convert to task dict
                task = Task(self, task, origin='raptor').as_dict()

            assert 'description' in task

            mode = task['description'].get('mode', TASK_EXECUTABLE)
            if mode == TASK_EXECUTABLE:
                executable_tasks.append(task)
            else:
                raptor_tasks.append(task)

        self._submit_executable_tasks(executable_tasks)
        self._submit_raptor_tasks(raptor_tasks)


    # --------------------------------------------------------------------------
    #
    def _submit_raptor_tasks(self, tasks):

        if tasks:

            self.advance(tasks, state=rps.AGENT_SCHEDULING,
                                publish=True, push=False)

            self._req_put.put(tasks)


    # --------------------------------------------------------------------------
    #
    def _submit_executable_tasks(self, tasks):
        '''
        Submit tasks per given task description to the agent this
        master is running in.
        '''

        if not tasks:
            return

        for task in tasks:

            td = task['description']

            sbox = '%s/%s' % (self._sbox, td['uid'])

          # task['uid']               = td.get('uid')
            task['state']             = rps.AGENT_STAGING_INPUT_PENDING
            task['task_sandbox_path'] = sbox
            task['task_sandbox']      = 'file://localhost/' + sbox
            task['pilot_sandbox']     = os.environ['RP_PILOT_SANDBOX']
            task['session_sandbox']   = os.environ['RP_SESSION_SANDBOX']
            task['resource_sandbox']  = os.environ['RP_RESOURCE_SANDBOX']
            task['pilot']             = os.environ['RP_PILOT_ID']
            task['resources']         = {'cpu': td['ranks'] *
                                                td.get('cores_per_rank', 1),
                                         'gpu': td['ranks'] *
                                                td.get('gpus_per_rank', 0)}

            # NOTE: the order of insert / state update relies on that order
            #       being maintained through the component's message push,
            #       the update worker's message receive up to the insertion
            #       order into the update worker's DB bulk op.
            self._log.debug('insert %s', td['uid'])
            self.publish(rpc.STATE_PUBSUB, {'cmd': 'insert', 'arg': task})

        self.advance(tasks, state=rps.AGENT_STAGING_INPUT_PENDING,
                            publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def _request_cb(self, tasks):

        tasks = ru.as_list(tasks)

        self._log.debug('request_cb: %d', len(tasks))

        try:
            filtered = self.request_cb(tasks)
            if filtered:
                for task in filtered:
                    self._log.debug('REQ cb: %s', task['uid'])
                self.submit_tasks(filtered)

        except:
            self._log.exception('request cb failed')
            # FIXME: fail tasks


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):

        # FIXME: document task format
        return tasks


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, tasks):

        tasks = ru.as_list(tasks)

        try:
            self.result_cb(tasks)

        except:
            self._log.exception('result callback failed')

        for task in tasks:

            # check if the task was submited via the task_service EP
            tid = task['uid']

            if tid in self._task_service_data:
                # update task info and signal task service thread
                self._log.debug('unlock 1 %s', tid)
                self._task_service_data[tid][1] = task
                self._task_service_data[tid][0].set()

            ret = int(task.get('exit_code', -1))

            if ret == 0: task['target_state'] = rps.DONE
            else       : task['target_state'] = rps.FAILED

        self.advance(tasks, rps.AGENT_STAGING_OUTPUT_PENDING,
                            publish=True, push=True)


    # --------------------------------------------------------------------------
    #
    def result_cb(self, tasks):

        # FIXME: document task format

        pass


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
        # FIXME: TS
      # self.wait()

        self._log.debug('all workers terminated')


# ------------------------------------------------------------------------------

