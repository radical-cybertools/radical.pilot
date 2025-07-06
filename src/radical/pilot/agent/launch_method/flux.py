
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import copy
import queue

from collections import defaultdict

import threading as mt

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


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, session, prof):

        self._partitions  = list()
        self._id_cb       = None
        self._event_cb    = None
        self._fail_cb     = None
        self._create_cb   = None
        self._part_queues = list()
        self._task_count  = 0

        super().__init__(name, lm_cfg, rm_info, session, prof)


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

        self._log.debug('=== flux core   : %s', fm.core)
        self._log.debug('=== flux job    : %s', fm.job)
        self._log.debug('=== flux exe    : %s', fm.exe)
        self._log.debug('=== flux version: %s', fm.version)

        lm_info = {'env'       : env,
                   'env_sh'    : env_sh}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def start_flux(self, id_cb, event_cb, fail_cb, create_cb):

        self._id_cb     = id_cb
        self._event_cb  = event_cb
        self._fail_cb   = fail_cb
        self._create_cb = create_cb

        # start one flux instance per partition.  Flux instances are hosted in
        # partition threads which are fed tasks through queues.
        self._prof.prof('flux_start')

        # first partition nodelist into, well, partitions
        partitions = ru.partition(self._rm_info.node_list,
                                  self._rm_info.n_partitions)

        # run one partition thread per partition
        part_events = list()
        for part_id, nodes in enumerate(partitions):

            self._log.debug('starting flux partition %d on %d nodes',
                            part_id, len(nodes))

            q_in = queue.Queue()
            evt  = mt.Event()
            part_thread = mt.Thread(target=self._part_thread,
                                    args=(part_id, nodes, q_in, evt),
                                    name='rp.flux.part.%d' % part_id)
            part_thread.daemon = True
            part_thread.start()
            part_events.append(evt)

            self._part_queues.append(q_in)

        for evt in part_events:

            # wait for all partition threads to finish starting
            if not evt.wait(timeout=600):
                self._prof.prof('flux_start_failed')
                raise RuntimeError('partition thread did not start')

        self._prof.prof('flux_start_ok')


    # --------------------------------------------------------------------------
    #
    def _part_thread(self, part_id, nodes, q_in, evt):

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
            launcher = 'srun --nodes %s --ntasks-per-node 1 ' \
                       '--cpus-per-task=%d --gpus-per-task=%d ' \
                       '--export=ALL' \
                       % (nodelist, threads_per_node, gpus_per_node)

        part.service = ru.FluxService(launcher=launcher)
        part.service.start()

        if not part.service.ready(timeout=600):
            raise RuntimeError('flux service did not start')

        part.uri = part.service.r_uri

        self._log.debug('flux service %s: %s', part.service.uid, part.uri)

        part.helper = ru.FluxHelper(uri=part.uri)
        part.helper.start()
        part.helper.register_cb(self._event_cb)

        self._log.debug('flux helper: %s', part.helper.uid)

        evt.set()


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
                    self._id_cb(tid, fid)

                self._log.debug('%s: submitted %d tasks: %s', part.uid,
                                len(tids), tids)

            except Exception:
                self._log.exception('LM flux submit failed')
                self._fail_cb(tasks)



    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        pass


    # --------------------------------------------------------------------------
    #
    def submit_tasks(self, tasks):

        # round robin on available flux partitions
        parts  = defaultdict(list)
        for task in tasks:

            try:
                tid  = task['uid']

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
                    part_id = self._task_count % len(self._part_queues)
                    self._task_count += 1

                self._prof.prof('work_2', uid=task['uid'])
                parts[part_id].append(task)
                task['description']['environment']['RP_PARTITION_ID'] = part_id
                self._log.debug('task %s on partition %s', task['uid'], part_id)

                self._prof.prof('work_3', uid=task['uid'])

            except Exception:
                self._fail_cb(task)

        for part_id in parts:
            self._part_queues[part_id].put(parts[part_id])

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


# ------------------------------------------------------------------------------

