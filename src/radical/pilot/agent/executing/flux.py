
__copyright__ = 'Copyright 2013-2020, http://radical.rutgers.edu'
__license__   = 'MIT'

import time
import copy

from collections import defaultdict

import radical.utils as ru

from ...   import states as rps
from ..    import LaunchMethod
from .base import AgentExecutingComponent


# ------------------------------------------------------------------------------
#
class Flux(AgentExecutingComponent) :

    _shell = ru.which('bash') or '/bin/sh'

    # --------------------------------------------------------------------------
    #
    def initialize(self):
        '''
        This components has 3 strands of activity (threads):

          - the main thread listens for incoming tasks from the scheduler, and
            pushes them toward the watcher thread;
          - an event listener thread listens for flux events which signify task
            state updates, and pushes those events also to the watcher thread;
          - the watcher thread matches events and tasks, enacts state updates,
            and pushes completed tasks toward output staging.

        NOTE: we get tasks in *AGENT_SCHEDULING* state, and enact all
              further state changes in this component.
        '''

        super().initialize()

        # translate Flux states to RP states
        self._event_map = {'submit'   : None,   # rps.AGENT_SCHEDULING,
                           'depend'   : None,
                           'alloc'    : rps.AGENT_EXECUTING_PENDING,
                         # 'start'    : rps.AGENT_EXECUTING,
                           'cleanup'  : None,
                           'finish'   : rps.AGENT_STAGING_OUTPUT_PENDING,
                           'release'  : 'unschedule',
                           'free'     : None,
                           'clean'    : None,
                           'priority' : None,
                           'exception': rps.FAILED,
                          }

        lm_cfg = self.session.rcfg.launch_methods.get('FLUX')
        lm_cfg['pid']       = self.session.cfg.pid
        lm_cfg['reg_addr']  = self.session.cfg.reg_addr
        self._lm            = LaunchMethod.create('FLUX', lm_cfg, self._rm.info,
                                                  self._log, self._prof)

        # we only start the flux backend here
        self._lm.start_flux()

        # register state cb
        self._log.debug('register flux state cb: %s',
                        len(self._lm.partitions))

        if not self._lm.partitions:
            raise RuntimeError('no partitions found')

        for part in self._lm.partitions:
            self._log.debug('register flux state cb: %s', part.helper)
            part.helper.register_cb(self._job_event_cb)

        # local state management
        self._tasks  = dict()             # task_id -> task
        self._events = defaultdict(list)  # flux_id -> [events]
        self._idmap  = dict()             # flux_id -> task_id

        self._task_count = 0


      # self._test_flux()


    # --------------------------------------------------------------------------
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def flux_state_cb(self, task_id, state):

        self._log.debug('flux state cb: %s: %s', task_id, state)


    # --------------------------------------------------------------------------
    #
    def _test_flux(self, n=128, count=2):

        def state_cb(task_id, state):
          # self._log.debug('=====', task_id, state)
            pass

        fs = ru.FluxService()
        fs.start(timeout=-1)
        fh = ru.FluxHelper(fs.r_uri)

        t0 = time.time()
        specs = [ru.flux.spec_from_dict({'executable': 'true',
                                         'uid'       : 'task.%06d' % i})
                        for i in range(n)]
        dt  = time.time() - t0
        jps = len(specs) / dt
        self._log.debug("===== create %4d tasks in %5.1fs - %8.1fjob/s"
                                                                 % (n, dt, jps))

        fh.register_cb(state_cb)
        fh.start()

        with ru.ru_open('flux_async.prof', 'w') as fout:
            for c in range(count):

                specs = [ru.flux.spec_from_dict(
                            {'executable': 'sleep',
                             'arguments' : ['1'],
                             'uid'       : 'task.%06d.%04d' % (i, c)})
                                         for i in range(n)]
                start = time.time()

                tids = fh.submit(specs)
                fh.wait(tids)

                stop = time.time()
                jps = n / (stop - start)
                self._log.debug('===== waited %4d tasks in %5.1fs - %8.1fjob/s'
                                                         % (n, stop-start, jps))
                fout.write('%4d %8.1f\n' % (c, jps))
                fout.flush()

        fh.stop()
        fs.stop()


    # --------------------------------------------------------------------------
    #
    def get_task(self, tid):

        return self._tasks.get(tid)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task):

        # FIXME: clarify how to cancel tasks in Flux
        pass


    # --------------------------------------------------------------------------
    #
    def _job_event_cb(self, flux_id, event):

        self._log.debug('flux event: %s: %s', flux_id, event.name)

        while True:
            task = self._tasks.get(self._idmap.get(flux_id))
            if task:
                break
            time.sleep(0.1)

        if not task:
            self._log.error('no task for flux job %s: %s %s', flux_id,
                            event.name, list(self._tasks.keys()))
            self._events[flux_id].append(event)

        else:
            self._handle_event(task, flux_id, event)


    # --------------------------------------------------------------------------
    #
    def _handle_event(self, task, flux_id, event):

        ename = event.name
        state = self._event_map.get(ename)

        if ename == 'alloc':
            self._log.debug('map fluxid: %s: %s', flux_id, task['uid'])

        self._log.debug('flux event: %s: %s [%s]', flux_id, ename, state)
        self._log.debug_3('          : %s', str(event.context))

        if state is None:
            return

        if state == rps.AGENT_STAGING_OUTPUT_PENDING:

            task['exit_code'] = event.context.get('status', 1)
            if task['exit_code']: task['target_state'] = rps.FAILED
            else                : task['target_state'] = rps.DONE

            # FIXME: run post-launch commands here.  Alas, this is
            #        synchronous, and thus potentially rather slow.
            tid  = task['uid']
            cmds = task['description'].get('post_launch')
            if cmds:
                for cmd in cmds:
                    self._log.debug('post-launch %s: %s', task['uid'], cmd)
                    out, err, ret = ru.sh_callout(cmd, shell=True,
                                                  cwd=task['task_sandbox_path'])
                    self._log.debug('post-launch %s: %s [%s][%s]',
                                                             tid, ret, out, err)
                    if ret:
                        failed.append(task)
                        continue

            # on completion, push toward output staging
            self.advance_tasks(task, state, ts=event.timestamp,
                               publish=True, push=True)

        elif state == 'unschedule':

            # free task resources
            self._prof.prof('unschedule_start', uid=task['uid'])
            self._prof.prof('unschedule_stop',  uid=task['uid'])  # ?
          # self.publish(rpc.AGENT_UNSCHEDULE_PUBSUB, task)

        else:
            # otherwise only push a state update
            self.advance_tasks(task, state, ts=event.timestamp,
                               publish=True, push=False)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        self.advance(tasks, rps.AGENT_EXECUTING, publish=True, push=False)

        try:
            # round robin on available flux partitions
            parts  = defaultdict(list)
            failed = list()
            for task in tasks:

                # FIXME: run pre-launch commands here.  Alas, this is
                #        synchronous, and thus potentially rather slow.
                tid  = task['uid']
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
                            failed.append(task)
                            continue

                part_id = task['description']['partition']
                if part_id is None:
                    part_id = self._task_count % len(self._lm.partitions)
                    self._task_count += 1

                parts[part_id].append(task)
                task['description']['environment']['RP_PARTITION_ID'] = part_id
                self._log.debug('task %s on partition %s', task['uid'], part_id)

            for part_id, part_tasks in parts.items():

                part  = self._lm.partitions[part_id]
                specs = list()
                for task in part_tasks:
                    tid = task['uid']
                    self._tasks[tid] = task
                    specs.append(self.task_to_spec(task))

                tids = [task['uid'] for task in part_tasks]
                fids = part.helper.submit(specs)

                for fid, tid in zip(fids, tids):
                    self._idmap[fid] = tid

                self._log.debug('%s: submitted %d tasks: %s', part.uid,
                                len(tids), tids)

            if failed:
                for task in failed:
                    task['target_state'] = rps.FAILED
                self.advance(failed, rps.FAILED, publish=True, push=False)

        except Exception as e:
            self._log.exception('flux submit failed: %s', e)
            raise


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
        _, exec_path = self._create_exec_script(self._lm, task)
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


# ------------------------------------------------------------------------------

