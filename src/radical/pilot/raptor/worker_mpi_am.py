
import io
import os
import sys
import time
import queue

import threading         as mt
import multiprocessing   as mp

import radical.utils     as ru

from .. import constants as rpc

from .worker import Worker


# ------------------------------------------------------------------------------
#
class MPIWorker(Worker):
    '''
    This worker manages a certain number of cores and gpus.  The master will
    start this worker by placing one rank per managed core (the GPUs are used
    dynamically).

    The first rank (rank 0) will manage the worker and for that purpose spawns
    two threads.  The first will pull tasks from the master's queue, and upon
    arrival will:

      - schedule incoming tasks over the available ranks
      - sent each target rank the required task startup info

    The second thread will collect the results from the tasks and send them back
    to the master.

    The main thread of rank 0 will function like the other threads: wait for
    task startup info and enact them.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, session=None):

        self._rank  = int(os.environ.get('RP_RANK', -1))
        self._ranks = int(os.environ.get('RP_RANK', -1))

        if self._rank < 0 or self._ranks < 1:
            raise RuntimeError('no rank(s): MPI worker needs MPI')

        if self._rank == 0: self._manager = True
        else              : self._manager = False

        if self._manager:

            # rank 0 spawns manager threads
            self._pull_ok      = mt.Event()
            self._push_ok      = mt.Event()

            self._pull_threads = mt.Thread(target=self._pull_tasks)
            self._push_threads = mt.Thread(target=self._push_results)

            self._pull_threads.daemon = True
            self._push_threads.daemon = True

            self._pull_threads.start()
            self._push_threads.start()

            self._pull_ok.wait(timeout=1)
            self._push_ok.wait(timeout=1)

            if not self._pull_ok.is_set():
                raise RuntimeError('failed to start pull thread')

            if not self._push_ok.is_set():
                raise RuntimeError('failed to start push thread')

            # rank 0 will also register the worker with the master and connect
            # to the task and result queues
            super().__init__(cfg=cfg, session=session, register=True)

            self._log.debug('=== rank 0 is manager')


        if not self._manager:

            # all other ranks will *not* register with the master nor will they
            # connect to the master queues
            super().__init__(cfg=cfg, session=session, register=False)

            self._log.debug('=== rank %d is slave', self._rank)


        # keep worker ID and rank
        self._cfg['rank'] = self._rank
        self._cfg['uid']  = '%s.%03d' % (self._cfg['uid'], self._rank)

        self._n_cores = self._cfg.worker_descr.cores_per_rank
        self._n_gpus  = self._cfg.worker_descr.gpus_per_rank

        self._res_evt = mp.Event()          # set on free resources

        self._mlock   = ru.Lock(self._uid)  # lock `_modes`
        self._modes   = dict()              # call modes (call, exec, eval, ...)

        # We need to make sure to run only up to `gpn` tasks using a gpu
        # within that pool, so need a separate counter for that.
        self._resources = {'cores': [0] * self._n_cores,
                           'gpus' : [0] * self._n_gpus}

      # self._log.debug('cores %s', str(self._resources['cores']))
      # self._log.debug('gpus  %s', str(self._resources['gpus']))

        # resources are initially all free
        self._res_evt.set()

        # all ranks will run the work loop forever (until termination)
        while not self._term.is_set():

            self._work()


    # --------------------------------------------------------------------------
    #
    def _pull_tasks(self):

        # connect to the master's task queue
        self._req_get = ru.zmq.Getter('to_req', self._info.req_addr_get)

        while not self._term.is_set():

            task = self._req_get.get_nowait(timeout=0.1)

            if not task:
                continue

            # got a task: allocate worker ranks and send for execution




    # --------------------------------------------------------------------------
    #
    def _push_results(self):

        self._res_put = ru.zmq.Putter('to_res', self._info.res_addr_put)


    # --------------------------------------------------------------------------
    #
    def _alloc_task(self, task):
        '''
        allocate task resources
        '''

        with self._mlock:

            cores = task.get('cores', 1)
            gpus  = task.get('gpus' , 0)

            assert(cores >= 1)
            assert(cores <= self._n_cores)
            assert(gpus  <= self._n_gpus)

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

            task['resources'] = {'cores': alloc_cores,
                                 'gpus' : alloc_gpus}
            return True


    # --------------------------------------------------------------------------
    #
    def _dealloc_task(self, task):
        '''
        deallocate task resources
        '''

        with self._mlock:

            resources = task['resources']

            for n in resources['cores']:
                assert(self._resources['cores'][n])
                self._resources['cores'][n] = 0

            for n in resources['gpus']:
                assert(self._resources['gpus'][n])
                self._resources['gpus'][n] = 0

            # signal available resources
            self._res_evt.set()

            return True


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):
        '''
        grep call type from tasks, check if methods are registered, and
        invoke them.
        '''

        for task in ru.as_list(tasks):

            task['worker'] = self._uid

            try:

                # ok, we have work to do.  Check the requirements to see how
                # many cpus and gpus we need to mark as busy
                while not self._alloc_task(task):

                    # no resource - wait for new resources
                    #
                    # NOTE: this will block smaller tasks from being executed
                    #       right now.  alloc_task is not a proper scheduler,
                    #       after all.
                    while not self._res_evt.wait(timeout=1.0):
                        self._log.debug('req_alloc_wait %s', task['uid'])

                    time.sleep(0.01)

                    # break on termination
                    if self._term.is_set():
                        return False

                    self._res_evt.clear()


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
                proc = mp.Process(target=self._dispatch, args=[task, env])
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
                self._dealloc_task(task)

                res = {'req': task['uid'],
                       'out': None,
                       'err': 'req_cb error: %s' % e,
                       'ret': 1}

                self._res_put.put(res)


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
        def _dispatch_proc(res_lock):
            # FIXME: do we still need this thread?

            import setproctitle
            setproctitle.setproctitle('rp.dispatch.%s' % task['uid'])

            # make CUDA happy
            # FIXME: assume physical device numbering for now
            if task['resources']['gpus']:
                os.environ['CUDA_VISIBLE_DEVICES'] = \
                             ','.join(str(i) for i in task['resources']['gpus'])

            out, err, ret, val = self._modes[mode](task.get('data'))
            res = [task, str(out), str(err), int(ret), val]

            with res_lock:
                self._result_queue.put(res)
        # ----------------------------------------------------------------------


        ret = None
        try:
          # self._log.debug('dispatch: %s: %d', task['uid'], task['pid'])
            mode = task['mode']
            assert(mode in self._modes), 'no such call mode %s' % mode

            tout = task.get('timeout')
            self._log.debug('dispatch with tout %s', tout)

          # result = self._modes[mode](task.get('data'))
          # self._log.debug('got result: task %s: %s', task['uid'], result)
          # out, err, ret, val = result
          # # TODO: serialize `val`?
          # res = [task, str(out), str(err), int(ret), val]
          # self._result_queue.put(res)

            res_lock = mp.Lock()
            dispatcher = mp.Process(target=_dispatch_proc, args=(res_lock,))
            dispatcher.daemon = True
            dispatcher.start()
            dispatcher.join(timeout=tout)

            with res_lock:
                if dispatcher.is_alive():
                    dispatcher.kill()
                    dispatcher.join()
                    out = None
                    err = 'timeout (>%s)' % tout
                    ret = 1
                    res = [task, str(out), str(err), int(ret), None]
                    self._log.debug('put 2 result: task %s', task['uid'])
                    self._result_queue.put(res)
                    self._log.debug('dispatcher killed: %s', task['uid'])

        except Exception as e:

            self._log.exception('dispatch failed')
            out = None
            err = 'dispatch failed: %s' % e
            ret = 1
            res = [task, str(out), str(err), int(ret), None]
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
            while not self._term.is_set():

                try:
                    res = self._result_queue.get(timeout=0.1)
                    self._log.debug('got   result: %s', res)
                    self._result_cb(res)

                except queue.Empty:
                    pass

        except:
            self._log.exception('queue error')
            raise

        finally:
            # FIXME: we should unregister for all ranks on error maybe?
            if self._cfg['rank'] == 0:
                self.publish(rpc.CONTROL_PUBSUB,
                             {'cmd': 'worker_unregister',
                              'arg': {'uid' : self._cfg['uid']}})


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, result):

        try:
            task, out, err, ret, val = result
            self._log.debug('result cb: task %s', task['uid'])

            with self._plock:
                pid  = task['pid']
                del(self._pool[pid])

            # free resources again for the task
            self._dealloc_task(task)

            res = {'req': task['uid'],
                   'out': out,
                   'err': err,
                   'ret': ret,
                   'val': val}

            self._res_put.put(res)
            self.task_post_exec(task)
            self._prof.prof('req_stop', uid=task['uid'], msg=self._uid)

        except:
            self._log.exception('result cb failed')
            raise



    # --------------------------------------------------------------------------
    #
    def _error_cb(self, error):

        self._log.debug('error: %s', error)
        raise RuntimeError(error)


    # --------------------------------------------------------------------------
    #
    def test(self, idx, seconds):
        # pylint: disable=reimported
        import time
        print('start idx %6d: %.1f' % (idx, time.time()))
        time.sleep(seconds)
        print('stop  idx %6d: %.1f' % (idx, time.time()))


# ------------------------------------------------------------------------------

