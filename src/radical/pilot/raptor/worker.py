


import os
import sys
import time
import queue

import threading         as mt
import multiprocessing   as mp

import radical.utils     as ru

from .. import Session
from .. import utils     as rpu
from .. import constants as rpc


# ------------------------------------------------------------------------------
#
class Worker(rpu.Component):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session=None):

        self._session = session

        if isinstance(cfg, str): cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : cfg = ru.Config(cfg=cfg)


        # generate a MPI rank dependent UID for each worker process
        # FIXME: this should be delegated to ru.generate_id
        # FIXME: why do we need to import `os` again after MPI Spawn?
        import os                                   # pylint: disable=reimported

        # FIXME: rank determination should be moved to RU
        rank = None

        if rank is None: rank = os.environ.get('PMIX_RANK')
        if rank is None: rank = os.environ.get('PMI_RANK')
        if rank is None: rank = os.environ.get('OMPI_COMM_WORLD_RANK')

        # keep worker ID and rank
        cfg['wid']    = cfg['uid']
        cfg['rank']   = rank

        if rank is not None:
            cfg['uid'] = '%s.%03d' % (cfg['uid'], int(rank))

        self._n_cores = cfg.cores
        self._n_gpus  = cfg.gpus

        self._info    = ru.Config(cfg=cfg.get('info', {}))


        if not self._session:
            self._session = Session(cfg=cfg, uid=cfg.sid, _primary=False)


        rpu.component.debug = True
        rpu.Component.__init__(self, cfg, self._session)

        self._res_evt = mp.Event()          # set on free resources

        self._mlock   = ru.Lock(self._uid)  # lock `_modes`
        self._modes   = dict()              # call modes (call, exec, eval, ...)

        # We need to make sure to run only up to `gpn` tasks using a gpu
        # within that pool, so need a separate counter for that.
        self._resources = {'cores' : [0] * self._n_cores,
                           'gpus'  : [0] * self._n_gpus}

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

        # connect to master
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # run worker initialization *before* starting to work on requests.
        # the worker provides three builtin methods:
        #     eval:  evaluate a piece of python code
        #     exec:  execute  a command line (fork/exec)
        #     shell: execute  a shell command
        #     call:  execute  a method or function call
        self.register_mode('call',  self._call)
        self.register_mode('eval',  self._eval)
        self.register_mode('exec',  self._exec)
        self.register_mode('shell', self._shell)

        self.pre_exec()

        # connect to the request / response ZMQ queues
        self._res_put = ru.zmq.Putter('to_res', self._info.res_addr_put)
        self._req_get = ru.zmq.Getter('to_req', self._info.req_addr_get,
                                                cb=self._request_cb)

        # the worker can return custom information which will be made available
        # to the master.  This can be used to communicate, for example, worker
        # specific communication endpoints.

        # `info` is a placeholder for any additional meta data communicated to
        # the worker.  Only first rank publishes.
        if self._cfg['rank'] == 0:
            self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                              'arg': {'uid' : self._cfg['wid'],
                                                      'info': self._info}})

        # prepare base env dict used for all tasks
        self._task_env = dict()
        for k,v in os.environ.items():
            if k.startswith('RP_'):
                self._task_env[k] = v


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Stager
    #
    @classmethod
    def create(cls, cfg, session):

        return Worker(cfg, session)


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

        assert(name not in self._modes)

        self._modes[name] = executor


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

        try:
            out = to_call(*args, **kwargs)
            err = None
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % (data))
            out = None
            err = 'call failed: %s' % e
            ret = 1

        return out, err, ret


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
    #      return out, err, ret
    # --------------------------------------------------------------------------
    #
    # For that to work we would need to be able to create a LM here, but ideally
    # not replicate the work done in the agent executor.


    # --------------------------------------------------------------------------
    #
    def _eval(self, data):
        '''
        We expect data to have a single entry: 'code', containing the Python
        code to be eval'ed
        '''

        try:
            out = eval(data['code'])
            err = None
            ret = 0

        except Exception as e:
            self._log.exception('_eval failed: %s' % (data))
            out = None
            err = 'eval failed: %s' % e
            ret = 1

        return out, err, ret


    # --------------------------------------------------------------------------
    #
    def _exec(self, data):
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
            self._log.exception('_exec failed: %s' % (data))
            out = None
            err = 'exec failed: %s' % e
            ret = 1

        return out, err, ret


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

        return out, err, ret


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
    def _request_cb(self, tasks):
        '''
        grep call type from tasks, check if methods are registered, and
        invoke them.
        '''

        self._log.debug('=== req_loop %s', len(ru.as_list(tasks)))
        for task in ru.as_list(tasks):

            self._log.debug('=== req_recv %s', task['uid'])
            task['worker'] = self._uid

            try:

                # ok, we have work to do.  Check the requirements to see how
                # many cpus and gpus we need to mark as busy
                while not self._alloc_task(task):

                    self._log.debug('=== req_alloc %s', task['uid'])
                    # no resource - wait for new resources
                    #
                    # NOTE: this will block smaller tasks from being executed
                    #       right now.  alloc_task is not a proper scheduler,
                    #       after all.
                  # while not self._res_evt.wait(timeout=1.0):
                  #     self._log.debug('=== req_alloc_wait %s', task['uid'])

                    time.sleep(0.01)

                    # break on termination
                    if self._term.is_set():
                        return False

                    self._res_evt.clear()


                self._log.debug('=== req_alloced %s', task['uid'])
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

                self._log.debug('=== req_started %s: %s', task['uid'], proc.pid)


            except Exception as e:

                self._log.exception('request failed')

                # free resources again for failed task
                self._dealloc_task(task)

                res = {'req': task['uid'],
                       'out': None,
                       'err': 'req_cb error: %s' % e,
                       'ret': 1}

                self._res_put.put(res)

        self._log.debug('=== req_looped')


    def _after_fork():
        with open('/tmp/after_fork', 'a+') as fout:
            fout.write('after fork %s %s\n' % (os.getpid(), mt.current_thread().name))

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
        def _dispatch_thread():
            import setproctitle
            setproctitle.setproctitle('dispatch.%s' % task['uid'])
            out, err, ret = self._modes[mode](task.get('data'))
            res = [task, str(out), str(err), int(ret)]
            self._log.debug('put 1 result: task %s', task['uid'])
            self._result_queue.put(res)
        # ----------------------------------------------------------------------


        ret = None
        try:
          # self._log.debug('dispatch: %s: %d', task['uid'], task['pid'])
            mode = task['mode']
            assert(mode in self._modes), 'no such call mode %s' % mode

            self._log.debug('=== debug %s: %s', task['uid'], task)
            tout = task.get('timeout')
            self._log.debug('dispatch with tout %s', tout)

            out, err, ret = self._modes[mode](task.get('data'))
            res = [task, str(out), str(err), int(ret)]
            self._log.debug('put 1 result: task %s', task['uid'])
            self._result_queue.put(res)

          # dispatcher = mp.Process(target=_dispatch_thread)
          # dispatcher.daemon = True
          # dispatcher.start()
          # self._log.debug('=== join %s: %s', task['uid'], task)
          # dispatcher.join(timeout=tout)
          # self._log.debug('=== joined %s: %s', task['uid'], tout)
          #
          # if dispatcher.is_alive():
          #     dispatcher.kill()
          #     dispatcher.join()
          #     out = None
          #     err = 'timeout (>%s)' % tout
          #     ret = 1
          #     res = [task, str(out), str(err), int(ret)]
          #     self._log.debug('put 2 result: task %s', task['uid'])
          #     self._result_queue.put(res)
          #     self._log.debug('dispatcher killed: %s', task['uid'])

        except Exception as e:

            self._log.exception('dispatch failed')
            out = None
            err = 'dispatch failed: %s' % e
            ret = 1
            res = [task, str(out), str(err), int(ret)]
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

              # self._log.debug('=== waiting for results')

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
            self._log.debug('=== send unregister')
            if self._cfg['rank'] == 0:
                self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_unregister',
                                                  'arg': {'uid' : self._cfg['wid']}})


    # --------------------------------------------------------------------------
    #
    def _result_cb(self, result):

        try:
            task, out, err, ret = result
          # self._log.debug('result cb: task %s', task['uid'])

            with self._plock:
                pid  = task['pid']
                del(self._pool[pid])

            # free resources again for the task
            self._dealloc_task(task)

            res = {'req': task['uid'],
                   'out': out,
                   'err': err,
                   'ret': ret}

            self._res_put.put(res)
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
    def _control_cb(self, topic, msg):

        if msg['cmd'] == 'terminate':
            self._term.set()

        elif msg['cmd'] == 'worker_terminate':
            if msg['arg']['uid'] == self._cfg['wid']:
                self._term.set()


    # --------------------------------------------------------------------------
    #
    def start(self):

        # note that this overwrites `Component.start()` - this worker component
        # is not using the registered input channels, but listens to it's own
        # set of channels in `_request_cb`.
        pass


    # --------------------------------------------------------------------------
    #
    def join(self):

        while not self._term.is_set():
            time.sleep(1.0)


# ------------------------------------------------------------------------------
