
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import time
import queue

from collections import defaultdict
from functools   import partial

import threading       as mt
import multiprocessing as mp

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Flux(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    class Partition(object):

        def __init__(self):

            self.uid      = None
            self.uri      = None
            self.service  = None
            self.helper   = None

    class Event(ru.TypedDict):
        _schema = {'name'     : str,
                   'timestamp': float}


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, session, prof):

        self._partitions  = list()
        self._idmap       = dict()             # flux_id -> task_id
        self._part_map    = dict()             # task_id -> part_id
        self._events      = defaultdict(list)  # flux_id -> [events]
        self._events_lock = mt.Lock()
        self._in_queues   = list()
        self._out_queues  = list()

        super().__init__(name, lm_cfg, rm_info, session, prof)


    # --------------------------------------------------------------------------
    #
    @property
    def partitions(self):
        return self._partitions


    def can_launch(self, task):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launch_cmds(self, task, exec_path):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_launcher_env(self):
        raise RuntimeError('method cannot be used on Flux LM')


    def get_rank_cmd(self):
        return 'export RP_RANK=$FLUX_TASK_RANK\n'


    def get_env_blacklist(self):
        '''
        extend the base blacklist by FLUX related environment variables
        '''

        ret = super().get_env_blacklist()

        ret.extend([
                    'FLUX_*',
                    'MPICH_*',
                    'PMI_*',
                    'PMIX_*',
                    # Slurm included since we launch Flux using Srun LM
                    'SLURM_*'
                   ])

        return ret

    def get_env_preserved(self):
        ret = super().get_env_preserved()
        ret.extend([
            'LD_LIBRARY_PATH'
        ])
        return ret


    # --------------------------------------------------------------------------
    #
    def _terminate(self):

        for part in self._partitions:

            self._log.debug('stop partition %s [%s]', part.uid, part.uri)

            try   : part.service.stop()
            except: pass

            for fh in part.helper:
                try   : fh.stop()
                except: pass

        self._partitions = list()


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        # just make sure we can load flux

        fm = ru.flux.FluxModule()
        if fm.exc:
            raise RuntimeError('flux import failed: %s' % fm.exc)

        self._log.debug('flux core   : %s', fm.core)
        self._log.debug('flux job    : %s', fm.job)
        self._log.debug('flux exe    : %s', fm.exe)
        self._log.debug('flux version: %s', fm.version)

        lm_info = {'env'       : env,
                   'env_sh'    : env_sh}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        pass


    # --------------------------------------------------------------------------
    #
    def start_flux(self, event_cb):

        self._event_cb = event_cb

        # start one flux instance per partition.  Flux instances are hosted in
        # partition threads which are fed tasks through queues.
        self._prof.prof('flux_start')

        # first partition nodelist into, well, partitions
        partitions = ru.partition(self._rm_info.node_list,
                                  self._rm_info.n_partitions)

        # run one partition thread per partition
        for part_id, nodes in enumerate(partitions):

            self._log.debug('starting flux partition %d on %d nodes',
                            part_id, len(nodes))

            q_in  = mp.Queue()
            q_out = mp.Queue()
            part_proc = mp.Process(target=self._part_thread,
                                     args=(part_id, nodes, q_in, q_out),
                                     name='rp.flux.part.%d' % part_id)
            part_proc.start()
            # FIXME: add process watcher

            self._in_queues.append(q_in)
            self._out_queues.append(q_out)

        for q_out in self._out_queues:

            # wait for all partition threads to finish starting
            start = time.time()
            while True:
                now = time.time()
                if now - start > 600:
                    raise RuntimeError('flux partition did not start')

                try:
                    val = q_out.get(timeout=1)
                except queue.Empty:
                    continue

                if val is not True:
                    raise RuntimeError('flux partition startup failed')

                break

            self._log.debug('flux partition started')

        # start watcher thread
        watcher = mt.Thread(target=self._queue_watcher)
        watcher.daemon = True
        watcher.start()

        self._prof.prof('flux_start_ok')


    # --------------------------------------------------------------------------
    #
    def _part_thread(self, part_id, nodes, q_in, q_out):

        threads_per_node = self._rm_info.cores_per_node  # == hw threads
        gpus_per_node    = self._rm_info.gpus_per_node

        self._log.info('starting partition thread %d on %d nodes',
                       part_id, len(nodes))

        part = self.Partition()

        self._log.debug('flux partition %d starting', part_id)

        # FIXME: this is a hack for frontier and will only work for slurm
        #        resources.  If Flux is to be used more widely, we need to
        #        pull the launch command from the agent's resource manager.
        launcher = None
        srun     = ru.which('srun')
        mpiexec  = ru.which('mpiexec')
        nodelist = ','.join([node['name'] for node in nodes])
        if srun:
            launcher = 'srun --nodes %d --nodelist %s --ntasks-per-node 1 ' \
                       '--cpus-per-task=%d --gpus-per-task=%d ' \
                       '--export=ALL' \
                       % (len(nodes), nodelist, threads_per_node, gpus_per_node)

        elif mpiexec:
            launcher = 'mpiexec -n %d --host %s' % (len(nodes), nodelist)

        self._log.debug('flux launcher: %s', launcher)

        part.service = ru.FluxService(launcher=launcher)
        part.service.start()

        if not part.service.ready(timeout=600):
            raise RuntimeError('flux service did not start')

        part.uri = part.service.r_uri

        self._log.debug('flux service %s: %s', part.service.uid, part.uri)

        part.helper = ru.FluxHelper(uri=part.uri)
        part.helper.start()

        partial_cb = partial(self._job_event_cb, q_out)
        part.helper.register_cb(partial_cb)

        self._log.debug('flux helper: %s', part.helper.uid)

        # signal startup
        q_out.put(True)


        # we now wait for tasks to submit to our flux service
        while True:

            try:
                msg = q_in.get(timeout=1)

            except queue.Empty:
                continue

            cmd = msg[0]

            if cmd == 'cancel':
                tid = msg[1]
                self._log.debug('cancel task %s on partition %d', tid, part_id)
                part.helper.cancel(tid)
                continue

            assert cmd == 'submit', cmd

            try:
                tasks = msg[1]
                specs = list()

                for tid in tasks:

                    self._prof.prof('part_0', uid=tid)
                    specs.append(tasks[tid])

                for tid in tasks:
                    self._prof.prof('submit_0', uid=tid)

                fids = part.helper.submit(specs)
                for fid, tid in zip(fids, tasks.keys()):
                    self._prof.prof('submit_1', uid=tid)
                  # self._log.debug('push flux job id: %s -> %s', tid, fid)
                    q_out.put(['job_id', (tid, fid)])

                self._log.debug('%s: submitted %d tasks', part.uid, len(tasks))

            except Exception:
                self._log.exception('LM flux submit failed')
                for tid in tasks:
                    self._event_cb(tid, self.Event(name='lm_failed',
                                                   timestamp=time.time()))



    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, parts):

        for part_id, specs in parts.items():

            q_in = self._in_queues[part_id]
            q_in.put(['submit', specs])

            for tid in specs:
                self._log.debug('register %s on part %d', tid, part_id)
                self._part_map[tid] = part_id


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):

        tid     = task['uid']
        part_id = self._part_map.get(tid)

        if part_id is None:
            self._log.error('cancel: no part for task %s', tid)
            return

        self._log.debug('cancel task %s on partition %d', tid, part_id)

        q_in = self._in_queues[part_id]
        q_in.put(['cancel', tid])


    # --------------------------------------------------------------------------
    #
    def _queue_watcher(self):

        while True:

            busy = False
            for q_out in self._out_queues:

                if q_out.empty():
                    continue

                busy = True
                cmd, event = q_out.get()

              # self._log.debug('=== pull event: %s: %s', cmd, event)

                if cmd == 'job_id':
                    task_id, flux_id = event
                    self._job_id_handler(task_id, flux_id)

                elif cmd == 'event':
                    flux_id, event = event
                    self._job_event_handler(flux_id, event)

                else:
                    self._log.error('unknown flux event: %s', cmd)

            if not busy:
                time.sleep(0.1)


    # --------------------------------------------------------------------------
    #
    def _job_id_handler(self, task_id, flux_id):

        with self._events_lock:

            self._log.debug('flux job id: %s -> %s', task_id, flux_id)

            self._idmap[flux_id] = task_id

            events = self._events.get(flux_id, [])

            self._log.debug('flux job id: %s -> %d events', task_id, len(events))
            if events:
                del self._events[flux_id]


        for event in events:
            self._log.debug('job id event: %s, event: %s', task_id, event.name)
            self._event_cb(task_id, event)


    # --------------------------------------------------------------------------
    #
    def _job_event_cb(self, q_out, flux_id, event):

      # self._log.debug('=== push flux job event: %s: %s', flux_id, event.name)
        q_out.put(['event', (flux_id, event)])


    # --------------------------------------------------------------------------
    #
    def _job_event_handler(self, flux_id, event):

        self._log.debug('flux event: %s: %s', flux_id, event.name)

        with self._events_lock:

            task_id = self._idmap.get(flux_id)

            if not task_id:
                self._log.info('no task id for flux job %s: %s', flux_id,
                                event.name)
                self._events[flux_id].append(event)
                return

        self._event_cb(task_id, event)


# ------------------------------------------------------------------------------

