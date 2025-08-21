
import os
import sys
import time
import queue
import asyncio

import threading         as mt
import multiprocessing   as mp

import radical.utils     as ru

from .worker            import Worker
from ..resource_config  import Slot
from ..task_description import TASK_FUNC, TASK_METH


# ------------------------------------------------------------------------------
#
class DefaultWorker(Worker):

    # --------------------------------------------------------------------------
    #
    def __init__(self, raptor_id : str):

        # only rank 0 (the manager) registers with the master.
        rank = int(os.environ.get('RP_RANK', 0))
        if rank == 0: manager = True
        else        : manager = False

        self._res_evt = mp.Event()          # set on free resources
        self._my_term = mt.Event()          # for start/stop/join

        super().__init__(manager=manager, rank=rank, raptor_id=raptor_id)

        # connect to the master queues
        self._res_put = ru.zmq.Putter('result',  self._res_addr_put)
        self._req_get = ru.zmq.Getter('request', self._req_addr_get,
                                                 cb=self._request_cb)

        # the master should have stored our own task description in the registry
        self._descr = self._reg['raptor.%s.cfg' % self._uid] or {}

        # keep worker ID and rank
        self._n_cores = int(self._descr.get('cores_per_rank') or
                            os.getenv('RP_CORES_PER_RANK', '1'))
        self._n_gpus  = int(self._descr.get('gpus_per_rank') or
                            os.getenv('RP_GPUS_PER_RANK', '0'))

        # We need to make sure to run only up to `gpn` tasks using a gpu
        # within that pool, so need a separate counter for that.
        self._rlock     = mt.Lock()
        self._resources = {'cores' : [0] * self._n_cores,
                           'gpus'  : [0] * self._n_gpus}

      # self._log.debug('cores %s', str(self._resources['cores']))
      # self._log.debug('gpus  %s', str(self._resources['gpus']))

        # resources are initially all free
        self._res_evt.set()

        self._pool  = dict()     # map task uid to process instance
        self._plock = mt.Lock()  # lock _pool

        # We also create a queue for communicating results back, and a thread to
        # watch that queue
        self._result_queue  = mp.Queue()
        self._result_thread = mt.Thread(target=self._result_watcher)
        self._result_thread.daemon = True
        self._result_thread.start()


    # --------------------------------------------------------------------------
    #
    def start(self):

        pass


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._my_term.set()


    # --------------------------------------------------------------------------
    #
    def join(self):

        # FIXME
        while True:
            if self._my_term.wait(1):
                break


    # --------------------------------------------------------------------------
    #
    def _alloc(self, task):
        '''
        allocate task resources
        '''

        uid = task['uid']
        self._prof.prof('schedule_try', uid=uid)

        with self._rlock:

            cores = task.get('cores', 1)
            gpus  = task.get('gpus' , 0)

            assert cores >= 1
            assert cores <= self._n_cores
            assert gpus  <= self._n_gpus

            if cores > self._resources['cores'].count(0): return False
            if gpus  > self._resources['gpus' ].count(0): return False

            alloc_cores = list()
            alloc_gpus  = list()

            if cores:
                for n in range(self._n_cores):
                    if not self._resources['cores'][n]:
                        self._resources['cores'][n] = 1
                        alloc_cores.append(n)
                        if len(alloc_cores) == cores:
                            break

            if gpus:
                for n in range(self._n_gpus):
                    if not self._resources['gpus'][n]:
                        self._resources['gpus'][n] = 1
                        alloc_gpus.append(n)
                        if len(alloc_gpus) == gpus:
                            break

            # FIXME: `Slot` serialization for the `mp.Queue` seems broken, just
            #        using a plain dict for now
            # task['slots'] = [Slot(cores=alloc_cores,
            #                       gpus=alloc_gpus)]
            task['slots'] = [{'cores': alloc_cores,
                              'gpus' : alloc_gpus}]

        self._prof.prof('schedule_ok', uid=uid)

        return True


    # --------------------------------------------------------------------------
    #
    def _dealloc(self, task):
        '''
        deallocate task resources
        '''

        self._prof.prof('unschedule_start', uid=task['uid'])

        with self._rlock:

            resources = task['slots'][0]

            for n in resources['cores']:
                assert self._resources['cores'][n]
                self._resources['cores'][n] = 0

            for n in resources['gpus']:
                assert self._resources['gpus'][n]
                self._resources['gpus'][n] = 0

            # signal available resources
            self._res_evt.set()
            self._prof.prof('unschedule_stop', uid=task['uid'])

        return True


    # --------------------------------------------------------------------------
    #
    def _request_cb(self, tasks):
        '''
        grep call type from tasks, check if methods are registered, and
        invoke them.
        '''

        for task in ru.as_list(tasks):

            task['worker'] = self._uid

            try:

                # ok, we have work to do.  Check the requirements to see how
                # many cpus and gpus we need to mark as busy
                while not self._alloc(task):

                    # no resource - wait for new resources
                    #
                    # NOTE: this will block smaller tasks from being executed
                    #       right now.  alloc_task is not a proper scheduler,
                    #       after all.
                  # while not self._res_evt.wait(timeout=1.0):
                  #     self._log.debug('req_alloc_wait %s', task['uid'])
                  # #   FIXME: `clear` should be locked
                  # self._res_evt.clear()

                    time.sleep(0.01)

                self._prof.prof('req_start', uid=task['uid'], msg=self._uid)

                # we got an allocation for this task, and can run it, so apply
                # to the process pool.  The callback (`self._result_cb`) will
                # pick the task up on completion and free resources.
                #
                # NOTE: we don't use mp.Pool - see __init__ for details

                env = self._task_env
                env['RP_TASK_ID'] = task['uid']

              # ret = self._pool.apply_async(func=self._dispatch, args=[task],
              #                              callback=self._result_cb,
              #                              error_callback=self._error_cb)
                proc = mp.Process(target=self._dispatch, args=(task, env))
              # proc.daemon = True

                with self._plock:

                    # we need to include `proc.start()` in the lock, as
                    # otherwise we may end up getting the `self._result_cb`
                    # before the pid could be registered in `self._pool`.
                    proc.start()
                    self._pool[proc.pid] = proc
                self._log.debug('applied: %s: %s: %s',
                                task['uid'], proc.pid, self._pool.keys())


            except Exception as e:

                self._log.exception('request failed')

                # free resources again for failed task
                self._dealloc(task)

                task['exception']        = repr(e)
                task['exception_detail'] = '\n'.join(ru.get_exception_trace())

                self._res_put.put(task)


    # --------------------------------------------------------------------------
    #
    def _dispatch(self, task, env):

        # this method is running in a process of the process pool, and will now
        # apply the task to the respective execution mode.
        #
        # NOTE: application of pre_exec directives may got here

        task['pid'] = os.getpid()

        # apply task env settings
        for k,v in env.items():
            os.environ[k] = v

        for k,v in task.get('environment', {}).items():
            os.environ[k] = v

        # ----------------------------------------------------------------------
        def _worker_proc(res_lock):
            # FIXME: do we still need this thread?

            import setproctitle
            setproctitle.setproctitle('rp.dispatch.%s' % task['uid'])

            # make CUDA happy
            # FIXME: assume physical device numbering for now
            if task['slots'][0]['gpus']:
                os.environ['CUDA_VISIBLE_DEVICES'] = \
                             ','.join(str(i) for i in task['slots'][0]['gpus'])

            out = None
            err = None
            ret = 1
            val = None
            exc = [None, None]
            try:

                sbox = task['task_sandbox_path']
                ru.rec_makedir(sbox)
                os.chdir(sbox)

                mode = task['description']['mode']
                dispatcher = self.get_dispatcher(mode)

                if mode in [TASK_METH, TASK_FUNC]:
                    out, err, ret, val, exc = asyncio.run(dispatcher(task))
                else:
                    out, err, ret, val, exc = dispatcher(task)

            except Exception as e:
                exc = [repr(e), '\n'.join(ru.get_exception_trace())]

            finally:
                os.chdir(self._sbox)

            res = [task, out, err, ret, val, exc]

            with res_lock:
                self._result_queue.put(res)
        # ----------------------------------------------------------------------


        ret = None
        try:
            tout = task['description']['timeout'] or None

          # self._log.debug('dispatch: %s: %d (%.1f)',
          #                 task['uid'], task['pid'], tout)

            res_lock = mp.Lock()
            worker_proc = mp.Process(target=_worker_proc, args=(res_lock,))
            worker_proc.daemon = True
            worker_proc.start()
            worker_proc.join(timeout=tout)

            with res_lock:
                if worker_proc.is_alive():
                    worker_proc.terminate()
                    worker_proc.join()
                    out = None
                    err = 'timeout (>%s)' % tout
                    ret = 1
                    val = None
                    exc = ['TimeoutError("task timed out")', None]
                    res = [task, str(out), str(err), int(ret), val, exc]
                    self._log.debug('put 2 result: task %s', task['uid'])
                    self._result_queue.put(res)
                    self._log.debug('worker_proc killed: %s', task['uid'])

        except Exception as e:

            self._log.exception('dispatch failed')
            out = None
            err = 'dispatch failed: %s' % e
            ret = 1
            val = None
            exc = [repr(e), '\n'.join(ru.get_exception_trace())]
            res = [task, str(out), str(err), int(ret), val, exc]
            self._log.debug('put 3 result: task %s', task['uid'])

            self._result_queue.put(res)

        finally:
            # if we kill the process too quickly, the result put above
            # will not make it out, thus make sure the queue is empty
            # first.
            ret = 1
            self._result_queue.close()
            self._result_queue.join_thread()
            sys.exit(ret)
          # os.kill(os.getpid(), signal.SIGTERM)



    # --------------------------------------------------------------------------
    #
    def _result_watcher(self):

        try:
            while True:

                try:
                    res = self._result_queue.get(timeout=0.1)
                    self._log.debug('got result: %s', res)
                    self._result_cb(res)

                except queue.Empty:
                    pass

        except:
            self._log.exception('queue error')
            raise


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, result):

        task, out, err, ret, val, exc = result
        self._log.debug('result cb: task %s', task['uid'])

        with self._plock:
            pid  = task['pid']
            del self._pool[pid]

        # free resources again for the task
        self._dealloc(task)

        task['stdout']           = out
        task['stderr']           = err
        task['exit_code']        = ret
        task['return_value']     = val
        task['exception']        = exc[0]
        task['exception_detail'] = exc[1]

        self._res_put.put(task)
        self._prof.prof('req_stop', uid=task['uid'], msg=self._uid)


    # --------------------------------------------------------------------------
    #
    def _error_cb(self, error):

        self._log.debug('error: %s', error)
        raise RuntimeError(error)


# ------------------------------------------------------------------------------

