

import io
import os
import sys
import time
import queue

import threading         as mt
import multiprocessing   as mp

import radical.utils     as ru

from .worker import Worker


# ------------------------------------------------------------------------------
#
class DefaultWorker(Worker):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, session=None):

        # generate a MPI rank dependent UID for each worker process
        # FIXME: this should be delegated to ru.generate_id
        # FIXME: rank determination should be moved to RU
        rank = None

        if rank is None: rank = os.environ.get('PMIX_RANK')
        if rank is None: rank = os.environ.get('PMI_RANK')
        if rank is None: rank = os.environ.get('OMPI_COMM_WORLD_RANK')

        if rank is None: rank = 0
        else           : rank = int(rank)

        # only rank 0 registers with the master
        if rank == 0: register = True
        else        : register = False

        super().__init__(cfg=cfg, session=session, register=register)

        # connect to the master queues
        self._res_put = ru.zmq.Putter('result',  self._cfg.info.res_addr_put)
        self._req_get = ru.zmq.Getter('request', self._cfg.info.req_addr_get,
                                                 cb=self.request_cb)

        # keep worker ID and rank
        self._cfg['rank'] = rank
        self._cfg['uid']  = '%s.%03d' % (self._cfg['uid'], rank)

        self._n_cores = self._cfg.worker_descr.cores_per_rank
        self._n_gpus  = self._cfg.worker_descr.gpus_per_rank

        self._res_evt = mp.Event()          # set on free resources
        self._my_term = mt.Event()          # for start/stop/join

        self._mlock   = ru.Lock(self._uid)  # lock `_modes`
        self._modes   = dict()              # call modes (call, exec, eval, ...)

        # We need to make sure to run only up to `gpn` tasks using a gpu
        # within that pool, so need a separate counter for that.
        self._resources = {'cores' : [0] * self._n_cores,
                           'gpus'  : [0] * self._n_gpus}

      # self._log.debug('cores %s', str(self._resources['cores']))
      # self._log.debug('gpus  %s', str(self._resources['gpus']))

        # resources are initially all free
        self._res_evt.set()

      # # create a multiprocessing pool with `cpn` worker processors.  Set
      # # `maxtasksperchild` to `1` so that we get a fresh process for each
      # # task.  That will also allow us to run command lines via `exec`,
      # # effectively replacing the worker process in the pool for a specific
      # # task.
      # #
      # # We use a `fork` context to inherit log and profile handles.
      # #
      # # NOTE: The mp documentation is wrong; mp.Pool does *not* have a context
      # #       parameters.  Instead, the Pool has to be created within
      # #       a context.
      # ctx = mp.get_context('fork')
      # self._pool = ctx.Pool(processes=self._n_cores,
      #                       initializer=None,
      #                       maxtasksperchild=1)
      # NOTE: a multiprocessing pool won't work, as pickle is not able to
      #       serialize our worker object.  So we use our own process pool.
      #       It's not much of a loss since we want to respawn new processes for
      #       each task anyway (to improve isolation).
        self._pool  = dict()  # map task uid to process instance
        self._plock = ru.Lock('p' + self._uid)  # lock _pool

        # We also create a queue for communicating results back, and a thread to
        # watch that queue
        self._result_queue  = mp.Queue()
        self._result_thread = mt.Thread(target=self._result_watcher)
        self._result_thread.daemon = True
        self._result_thread.start()

        # run worker initialization *before* starting to work on requests.
        # the worker provides three builtin methods:
        #     eval:  evaluate a piece of python code with `eval`
        #     exec:  evaluate a piece of python code with `exec`
        #     call:  execute  a method or function call
        #     proc:  execute  a command line (fork/exec)
        #     shell: execute  a shell command
        self.register_mode('eval',  self._eval)
        self.register_mode('exec',  self._exec)
        self.register_mode('call',  self._call)
        self.register_mode('proc',  self._proc)
        self.register_mode('shell', self._shell)

        self.pre_exec()

        # prepare base env dict used for all tasks
        self._task_env = dict()
        for k,v in os.environ.items():
            if k.startswith('RP_'):
                self._task_env[k] = v


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
    def pre_exec(self):
        '''
        This method can be overloaded by the Worker implementation to run any
        pre_exec commands before spawning worker processes.
        '''

        pass


    # --------------------------------------------------------------------------
    #
    def register_mode(self, name, executor):

        assert name not in self._modes

        self._modes[name] = executor


    # --------------------------------------------------------------------------
    #
    def _eval(self, data):
        '''
        We expect data to have a single entry: 'code', containing the Python
        code to be eval'ed
        '''

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            val = eval(data['code'])
            out = strout.getvalue()
            err = strerr.getvalue()
            ret = 0

        except Exception as e:
            self._log.exception('_eval failed: %s' % (data))
            self._log.exception('_eval failed: %s' % (data))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\neval failed: %s' % e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr


        return out, err, ret, val


    # --------------------------------------------------------------------------
    #
    def _exec(self, data):
        '''
        We expect data to have a single entry: 'code', containing the Python
        code to be exec'ed.  Data can have an optional entry `pre_exec` which
        can be used for any import statements and the like which need to run
        before the executed code.
        '''

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            pre  = data.get('pre_exec', '')
            code = data['code']

            # create a wrapper function around the given code
            lines = code.split('\n')
            outer = 'def _my_exec():\n'
            for line in lines:
                outer += '    ' + line + '\n'

            # call that wrapper function via exec, and keep the return value
            src = '%s\n\n%s\n\nresult=_my_exec()' % (pre, outer)

            # assign a local variable to capture the code's return value.
            loc = dict()
            exec(src, {}, loc)                # pylint: disable=exec-used # noqa
            val = loc['result']
            out = strout.getvalue()
            err = strerr.getvalue()
            ret = 0

        except Exception as e:
            self._log.exception('_exec failed: %s' % (data))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\nexec failed: %s' % e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr


        return out, err, ret, val


    # --------------------------------------------------------------------------
    #
    def _call(self, data):
        '''
        We expect data to have a three entries: 'method' or 'function',
        containing the name of the member method or the name of a free function
        to call, `args`, an optional list of unnamed parameters, and `kwargs`,
        and optional dictionary of named parameters.
        '''

        if 'method' in data:
            to_call = getattr(self, data['method'], None)

        elif 'function' in data:
            names   = dict(list(globals().items()) + list(locals().items()))
            to_call = names.get(data['function'])

        else:
            raise ValueError('no method or function specified: %s' % data)

        if not to_call:
            raise ValueError('callable not found: %s' % data)


        args   = data.get('args',   [])
        kwargs = data.get('kwargs', {})

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            val = to_call(*args, **kwargs)
            out = strout.getvalue()
            err = strerr.getvalue()
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % (data))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr


        return out, err, ret, val


    # --------------------------------------------------------------------------
    #
    def _proc(self, data):
        '''
        We expect data to have two entries: 'exe', containing the executabele to
        run, and `args` containing a list of arguments (strings) to pass as
        command line arguments.  We use `sp.Popen` to run the fork/exec, and to
        collect stdout, stderr and return code
        '''

        try:
            import subprocess as sp

            exe  = data['exe']
            args = data.get('args', list())
            env  = data.get('env',  dict())

            args = '%s %s' % (exe, ' '.join(args))

            proc = sp.Popen(args=args,      env=env,
                            stdin=None,     stdout=sp.PIPE, stderr=sp.PIPE,
                            close_fds=True, shell=True)
            out, err = proc.communicate()
            ret      = proc.returncode

        except Exception as e:
            self._log.exception('popen failed: %s' % (data))
            out = None
            err = 'exec failed: %s' % e
            ret = 1

        return out, err, ret, None


    # --------------------------------------------------------------------------
    #
    def _shell(self, data):
        '''
        We expect data to have a single entry: 'cmd', containing the command
        line to be called as string.
        '''

        try:
            out, err, ret = ru.sh_callout(data['cmd'], shell=True)

        except Exception as e:
            self._log.exception('_shell failed: %s' % (data))
            out = None
            err = 'shell failed: %s' % e
            ret = 1

        return out, err, ret, None


    # --------------------------------------------------------------------------
    #
    # FIXME: an MPI call mode should be added.  That could work along these
    #        lines of:
    #
    # --------------------------------------------------------------------------
    #  def _mpi(self, data):
    #
    #      try:
    #          cmd = rp.agent.launch_method.construct_command(data,
    #                  executable=self.exe, args=data['func'])
    #          out = rp.sh_callout(cmd)
    #          err = None
    #          ret = 0
    #
    #      except Exception as e:
    #          self._log.exception('_mpi failed: %s' % (data))
    #          out = None
    #          err = 'mpi failed: %s' % e
    #          ret = 1
    #
    #      return out, err, ret, None
    # --------------------------------------------------------------------------
    #
    # For that to work we would need to be able to create a LM here, but ideally
    # not replicate the work done in the agent executor.


    # --------------------------------------------------------------------------
    #
    def _alloc_task(self, task):
        '''
        allocate task resources
        '''

        with self._mlock:

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

            task['slots'] = {'cores': alloc_cores,
                             'gpus' : alloc_gpus}
            return True


    # --------------------------------------------------------------------------
    #
    def _dealloc_task(self, task):
        '''
        deallocate task resources
        '''

        with self._mlock:

            resources = task['slots']

            for n in resources['cores']:
                assert self._resources['cores'][n]
                self._resources['cores'][n] = 0

            for n in resources['gpus']:
                assert self._resources['gpus'][n]
                self._resources['gpus'][n] = 0

            # signal available resources
            self._res_evt.set()

            return True


    # --------------------------------------------------------------------------
    #
    def task_pre_exec(self, task):
        '''
        This method is called upon receiving a new request, and can be
        overloaded to perform any preperatory action before the request is acted
        upon
        '''
        pass


    # --------------------------------------------------------------------------
    #
    def task_post_exec(self, task):
        '''
        This method is called upon completing a request, and can be
        overloaded to perform any cleanup action before the request is reported
        as complete.
        '''
        pass


    # --------------------------------------------------------------------------
    #
    def request_cb(self, tasks):
        '''
        grep call type from tasks, check if methods are registered, and
        invoke them.
        '''

        for task in ru.as_list(tasks):

            task['worker'] = self._uid

            self.task_pre_exec(task)

            try:

                # ok, we have work to do.  Check the requirements to see how
                # many cpus and gpus we need to mark as busy
                while not self._alloc_task(task):

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
            if task['slots']['gpus']:
                os.environ['CUDA_VISIBLE_DEVICES'] = \
                             ','.join(str(i) for i in task['slots']['gpus'])

            out, err, ret, val = self._modes[mode](task.get('data'))
            res = [task, str(out), str(err), int(ret), val]

            with res_lock:
                self._result_queue.put(res)
        # ----------------------------------------------------------------------


        ret = None
        try:
          # self._log.debug('dispatch: %s: %d', task['uid'], task['pid'])
            mode = task['mode']
            assert mode in self._modes, 'no such call mode %s' % mode

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
                    dispatcher.terminate()
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
            while True:

                try:
                    res = self._result_queue.get(timeout=0.1)
                    self._log.debug('got   result: %s', res)
                    self._result_cb(res)

                except queue.Empty:
                    pass

        except:
            self._log.exception('queue error')
            raise


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, result):

        try:
            task, out, err, ret, val = result
            self._log.debug('result cb: task %s', task['uid'])

            with self._plock:
                pid  = task['pid']
                del self._pool[pid]

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

