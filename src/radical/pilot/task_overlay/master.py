
import os
import copy
import time

import radical.utils     as ru

from .. import Session, ComputeUnitDescription
from .. import utils     as rpu
from .. import states    as rps
from .. import constants as rpc


# ------------------------------------------------------------------------------
#
class Master(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, backend='zmq'):

        self._backend = backend  # FIXME: use

        self._lock    = ru.Lock('master')
        self._workers = dict()  # wid: worker

        cfg     = self._get_config()
        session = Session(cfg=cfg, _primary=False)

        rpu.Component.__init__(self, cfg, session)

        self.register_output(rps.AGENT_STAGING_INPUT_PENDING,
                             rpc.AGENT_STAGING_INPUT_QUEUE)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)

        # connect to the local agent
        self._log.debug('startup complete')


    # --------------------------------------------------------------------------
    #
    def _get_config(self):
        '''
        derive a worker base configuration from the control pubsub configuration
        '''

        # FIXME: this uses insider knowledge on the config location and
        #        structure.  It would be better if agent.0 creates the worker
        #        base config from scratch on startup.

        cfg = ru.read_json('../control_pubsub.json')

        del(cfg['channel'])
        del(cfg['cmgr'])

        cfg['log_lvl'] = 'debug'
        cfg['kind'] = 'master'
        cfg['base'] = os.getcwd()
        cfg['uid']  = ru.generate_id('master')

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

        self._log.debug('control: %s: %s', cmd, arg)

        if cmd == 'worker_register':

            uid  = arg['uid']
            info = arg['info']

            with self._lock:
                self._workers[uid]['info']  = info
                self._workers[uid]['state'] = 'ACTIVE'
                self._log.debug('info: %s', info)

        elif cmd == 'worker_unregister':

            uid = arg['uid']

            with self._lock:
                self._workers[uid]['state'] = 'DONE'


    # --------------------------------------------------------------------------
    #
    def submit(self, info, descr, count=1):
        '''
        submit n workers, do *not* wait for them to come up
        '''

        tasks = list()
        for i in range(count):

            # write config file for that worker
            cfg   = copy.deepcopy(self._cfg)
            cfg['info'] = info
            uid   = ru.generate_id('worker')
            sbox  = '%s/%s'      % (cfg['base'], uid)
            fname = '%s/%s.json' % (sbox, uid)

            cfg['kind'] = 'worker'
            cfg['uid']  = uid
            cfg['base'] = sbox

            ru.rec_makedir(sbox)
            ru.write_json(cfg, fname)

            # grab default settings via CUD construction
            descr = ComputeUnitDescription(descr).as_dict()

            # create task dict
            task = dict()
            task['description']       = copy.deepcopy(descr)
            task['state']             = rps.AGENT_STAGING_INPUT_PENDING
            task['type']              = 'unit'
            task['uid']               = uid
            task['unit_sandbox_path'] = sbox

            task['description']['arguments'] = [fname]

            tasks.append(task)
            self._workers[uid] = task

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
                self._log.debug('states [%d]: %s', n, {k:states.count(k) for k in set(states)})
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
    def terminate(self):
        '''
        terminate all workers
        '''

        for uid in self._workers:
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                              'arg': {'uid': uid}})


# ------------------------------------------------------------------------------

