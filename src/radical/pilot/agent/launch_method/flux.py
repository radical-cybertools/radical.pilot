
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import copy
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
        _schema = {'name': str}


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, session, prof):

        self._partitions  = list()
        self._create_cb   = None
        self._task_count  = 0

        self._idmap       = dict()             # flux_id -> task_id
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
    def start_flux(self, event_cb, create_cb):

        self._event_cb  = event_cb
        self._create_cb = create_cb

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
        nodelist = ','.join([node['name'] for node in nodes])
        if srun:
            launcher = 'srun --nodes %d --nodelist %s --ntasks-per-node 1 ' \
                       '--cpus-per-task=%d --gpus-per-task=%d ' \
                       '--export=ALL' \
                       % (len(nodes), nodelist, threads_per_node, gpus_per_node)

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

            tasks = None

            try:
                tasks = q_in.get(timeout=1)

            except queue.Empty:
                continue

            try:
                specs = list()
                tids  = list()

                for task in tasks:

                    tid = task['uid']
                    tids.append(tid)

                    self._prof.prof('part_0', uid=tid)
                    specs.append(self.task_to_spec(task))

                for task in tasks:
                    self._prof.prof('submit_0', uid=task['uid'])

                fids = part.helper.submit(specs)
                for fid, tid in zip(fids, tids):
                    self._prof.prof('submit_1', uid=tid)
                  # self._log.debug('push flux job id: %s -> %s', tid, fid)
                    q_out.put(['job_id', (tid, fid)])

                self._log.debug('%s: submitted %d tasks: %s', part.uid,
                                len(tids), tids)

            except Exception:
                self._log.exception('LM flux submit failed')
                for task in tasks:
                    self._event_cb(tid, self.Event(name='lm_failed'))



    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, tasks):

        # round robin on available flux partitions
        parts  = defaultdict(list)
        for task in tasks:

            tid = task['uid']

            try:

                # FIXME: pre_launch commands are synchronous and thus
                #        potentially slow.
                self._prof.prof('work_0', uid=tid)
                self._log.debug('LM submit %s', tid)

                cmds = task['description'].get('pre_launch')
                if cmds:
                    sbox = task['task_sandbox_path']
                    ru.rec_makedir(sbox)

                    for cmd in cmds:
                        self._log.debug('pre-launch %s: %s', task['uid'], cmd)
                        out, err, ret = ru.sh_callout(cmd, shell=True,
                                              cwd=task['task_sandbox_path'])
                        self._log.debug('pre-launch %s: %s [%s][%s]',
                                        tid, ret, out, err)

                        if ret:
                            raise RuntimeError('cmd failed: %s' % cmd)

                self._prof.prof('work_1', uid=task['uid'])
                part_id = task['description']['partition']
                if part_id is None:
                    part_id = self._task_count % len(self._in_queues)
                    self._task_count += 1

                self._prof.prof('work_2', uid=task['uid'])
                parts[part_id].append(task)
                task['description']['environment']['RP_PARTITION_ID'] = part_id
                self._log.debug('task %s on partition %s', task['uid'], part_id)

                self._prof.prof('work_3', uid=task['uid'])

            except:
                self._log.exception('LM flux submit failed for %s', tid)
                self._event_cb(tid, self.Event(name='lm_failed'))

        for part_id in parts:
            self._in_queues[part_id].put(parts[part_id])


    # --------------------------------------------------------------------------
    #
    def task_to_spec(self, task):

        td     = task['description']
        uid    = task['uid']
        sbox   = task['task_sandbox_path']
        stdout = td.get('stdout') or '%s/%s.out' % (sbox, uid)
        stderr = td.get('stderr') or '%s/%s.err' % (sbox, uid)

        task['stdout'] = ''
        task['stderr'] = ''

        task['stdout_file'] = stdout
        task['stderr_file'] = stderr

        self._prof.prof('task_create_exec_start', uid=uid)
        exec_path = self._create_cb(task)
        self._prof.prof('task_create_exec_ok', uid=uid)

        command = '%(cmd)s 1>%(out)s 2>%(err)s' % {'cmd': exec_path,
                                                   'out': stdout,
                                                   'err': stderr}
        spec_dict = copy.deepcopy(td)
        spec_dict['uid']        = uid
        spec_dict['executable'] = '/bin/sh'
        spec_dict['arguments']  = ['-c', command]

        self._prof.prof('task_to_flux_start', uid=uid)
        ret = ru.flux.spec_from_dict(spec_dict)

        self._prof.prof('task_to_spec_stop', uid=uid)

        return ret


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

