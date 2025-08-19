
import io
import os
import sys
import time
import shlex
import asyncio

import threading         as mt

import radical.utils     as ru

from .. import states    as rps
from .. import constants as rpc

from ..pytask           import PythonTask
from ..utils            import DeserializationError
from ..task_description import TASK_FUNC, TASK_METH, TASK_EXEC
from ..task_description import TASK_PROC, TASK_SHELL, TASK_EVAL



# ------------------------------------------------------------------------------
#
class Worker(object):
    '''
    Implement the Raptor protocol for dispatching multiple Tasks on persistent
    resources.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, manager, rank, raptor_id):

        self._manager   = manager
        self._rank      = rank
        self._raptor_id = raptor_id
        self._reg_event = mt.Event()
        self._reg_addr  = os.environ['RP_REGISTRY_ADDRESS']
        self._sbox      = os.environ['RP_TASK_SANDBOX']
        self._uid       = os.environ['RP_TASK_ID']
        self._sid       = os.environ['RP_SESSION_ID']
        self._ranks     = int(os.environ['RP_RANKS'])

        self._reg       = ru.zmq.RegistryClient(url=self._reg_addr)
        self._cfg       = ru.Config(cfg=self._reg['cfg'])

        self._hb_delay  = self._reg['rcfg.raptor.hb_delay']

        self._log  = ru.Logger(name=self._uid,
                               ns='radical.pilot.worker',
                               level=self._cfg.log_lvl,
                               debug=self._cfg.debug_lvl,
                               targets=self._cfg.log_tgt,
                               path=self._cfg.path)
        self._prof = ru.Profiler(name='%s.%04d' % (self._uid, self._rank),
                                 ns='radical.pilot.worker',
                                 path=self._sbox)

        # register for lifetime management messages on the control pubsub
        psbox     = os.environ['RP_PILOT_SANDBOX']
        state_cfg = self._reg['bridges.%s' % rpc.STATE_PUBSUB]
        ctrl_cfg  = self._reg['bridges.%s' % rpc.CONTROL_PUBSUB]

        ru.zmq.Subscriber(rpc.STATE_PUBSUB, url=state_cfg['addr_sub'],
                          log=self._log, prof=self._prof, cb=self._state_cb,
                          topic=rpc.STATE_PUBSUB)
        ru.zmq.Subscriber(rpc.CONTROL_PUBSUB, url=ctrl_cfg['addr_sub'],
                          log=self._log, prof=self._prof, cb=self._control_cb,
                          topic=rpc.CONTROL_PUBSUB)

        # we push hertbeat and registration messages on that pubsub also
        self._ctrl_pub = ru.zmq.Publisher(rpc.CONTROL_PUBSUB,
                                          url=ctrl_cfg['addr_pub'],
                                          log=self._log,
                                          prof=self._prof)
        # let ZMQ settle
        time.sleep(1)

        self._hb_register_count = 60
        # run heartbeat thread in all ranks (one hb msg every `n` seconds)
        self._log.debug('hb delay: %s', self._hb_delay)
        self._hb_thread = mt.Thread(target=self._hb_worker)
        self._hb_thread.daemon = True
        self._hb_thread.start()

        # run worker initialization *before* starting to work on requests.
        # the worker provides these builtin methods:
        #     eval:  evaluate a piece of python code with `eval`
        #     exec:  evaluate a piece of python code with `exec`
        #     call:  execute  a method or function call
        #     proc:  execute  a command line (fork/exec)
        #     shell: execute  a shell command
        self._modes = dict()
        self.register_mode(TASK_FUNC,  self._dispatch_func)
        self.register_mode(TASK_METH,  self._dispatch_meth)
        self.register_mode(TASK_EVAL,  self._dispatch_eval)
        self.register_mode(TASK_EXEC,  self._dispatch_exec)
        self.register_mode(TASK_PROC,  self._dispatch_proc)
        self.register_mode(TASK_SHELL, self._dispatch_shell)

        # prepare base env dict used for all tasks
        # NOTE: raptor tasks run in the same environment as the raptor worker
        self._task_env = dict()
        for k,v in os.environ.items():
            if not k.startswith('RP_'):
                self._task_env[k] = v

        reg_msg = {'cmd': 'worker_register',
                   'arg': {'uid'        : self._uid,
                           'raptor_id'  : self._raptor_id,
                           'ranks'      : self._ranks}}

        # the manager (rank 0) registers the worker with the master
        if self._manager:

            self._log.debug('register: %s / %s', self._uid, self._raptor_id)
            self._ctrl_pub.put(rpc.CONTROL_PUBSUB, reg_msg)

          # # FIXME: we never unregister on termination
          # self._ctrl_pub.put(rpc.CONTROL_PUBSUB, {'cmd': 'worker_unregister',
          #                                         'arg': {'uid' : self._uid}})

        # wait for raptor response (*all* ranks*)
        self._log.debug('wait for registration to complete')
        count = 0
        while not self._reg_event.wait(timeout=5):
            if count < self._hb_register_count:
                count += 1
                if self._manager:
                    self._log.debug('re-register: %s / %s', self._uid, self._raptor_id)
                    self._ctrl_pub.put(rpc.CONTROL_PUBSUB, reg_msg)
            else:
                self.stop()
                self.join()
                self._log.error('registration with master timed out')
                raise RuntimeError('registration with master timed out')

        if self._manager:
            self._log.debug('registration with master ok')


    # --------------------------------------------------------------------------
    #
    def _hb_worker(self):

        while True:

            self._ctrl_pub.put(rpc.CONTROL_PUBSUB,
                    {'cmd': 'worker_rank_heartbeat',
                     'arg': {'uid' : self._uid,
                             'rank': self._rank}})

            time.sleep(self._hb_delay)


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, topic, msgs):

        for msg in ru.as_list(msgs):

            cmd = msg.get('cmd')
            arg = msg.get('arg')

            if cmd not in ['update']:
                continue

            for thing in arg:

                uid   = thing['uid']
                state = thing['state']

                if uid == self._raptor_id:

                    if state in rps.FINAL + [rps.AGENT_STAGING_OUTPUT_PENDING]:
                        # master completed - terminate this worker
                        self._log.info('master %s final: %s - terminate',
                                       uid, state)
                        self.stop()
                        return False

        return True


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        if cmd == 'worker_registered':

            if arg['uid'] != self._uid:
                return

            if self._reg_event.is_set():
                # registration was completed already
                return

            self._ts_addr      = arg['info']['ts_addr']
            self._res_addr_put = arg['info']['res_addr_put']
            self._req_addr_get = arg['info']['req_addr_get']

            self._reg_event.set()

        elif cmd == 'terminate':
            self.stop()
            self.join()
            sys.exit()

        elif cmd == 'worker_terminate':

            if arg['uid'] == self._uid:
                self._log.debug('worker_terminate signal')
                self.stop()
                self.join()
                sys.exit()


    # --------------------------------------------------------------------------
    #
    def get_master(self):
        '''
        The worker can submit tasks back to the master - this method will
        return a small shim class to provide that capability.  That class has
        a single method `run_task` which accepts a single `rp.TaskDescription`
        from which a `rp.Task` is created and executed.  The call then waits for
        the task's completion before returning it in a dict representation, the
        same as when passed to the master's `result_cb`.

        Note: the `run_task` call is running in a separate thread and will thus
              not block the master's progress.

        Returns:
            Master: a shim class with only one method: `run_task(td)` where
                `td` is a `TaskDescription` to run.
        '''

        # ----------------------------------------------------------------------
        class Master(object):

            def __init__(self, addr):
                self._task_service_ep = ru.zmq.Client(url=addr)

            def run_task(self, td):
                return self._task_service_ep.request('run_task', td)
        # ----------------------------------------------------------------------

        return Master(self._ts_addr)


    # --------------------------------------------------------------------------
    #
    def start(self):
        '''Start the workers main work loop.
        '''

        raise NotImplementedError('`start()` must be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        '''Signal the workers to stop the main work loop.
        '''

        raise NotImplementedError('`stop()` must be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def join(self):
        '''Wait until the worker's main work loop completed.
        '''

        raise NotImplementedError('`join()` must be implemented by child class')


    # --------------------------------------------------------------------------
    #
    def register_mode(self, name, dispatcher) -> None:
        '''
        Register a new task execution mode that this worker can handle.
        The specified dispatcher callable should accept a single argument: the
        task to execute.

        Args:
            name (str): name of the mode to register
            dispatcher (callable): function which implements the execution mode
        '''

        if name in self._modes:
            raise ValueError('mode %s already registered' % name)

        self._modes[name] = dispatcher


    # --------------------------------------------------------------------------
    #
    def get_dispatcher(self, name):
        '''Query a registered execution mode.

        Args:
            name (str): name of execution mode to query for

        Returns:
            Callable: the dispatcher method for that execution mode
        '''

        if name not in self._modes:
            raise ValueError('mode %s unknown' % name)

        return self._modes[name]


    # --------------------------------------------------------------------------
    #
    def _dispatch_meth(self, task):
        '''
        _dispatch_meth is a simple wrapper around _dispatch_func which points to
        private methods to be called.
        '''

        task['description']['function'] = task['description']['method']

        return self._dispatch_func(task)


    # --------------------------------------------------------------------------
    #
    async def _dispatch_func(self, task):
        '''
        We expect three attributes: 'function', containing the name of the
        member method or free function to call, `args`, an optional list of
        unnamed parameters, and `kwargs`, and optional dictionary of named
        parameters.

        *function* is resolved first against `locals()`, then `globals()`, then
        attributes of the implementation class (member functions of *base*, as
        provided to `MPIWorkerRank()`). Finally, an attempt is made to
        deserialize a PythonTask from *function*. The first non-null resolution
        of *function* is used as the callable.

        NOTE: MPI function tasks will get a private communicator passed as first
              unnamed argument.

        Args:
            task (Dict[str, Any]): dictionary representation of the task to
                execute

        Returns:
            Tuple[str, str, int, Any, Tuple[str, str]]:
                - standard output (str)
                - standard error (str)
                - exit code (int)
                - return value (Any)
                - exception (Tuple[type (str), message (str)])

        Raises:
            KeyError
                if the task dictionary misses required entries
            ValueError
                if `task['description']['function']` cannot be resolved
            Assert
                if `task['description']['function']` is not set
        '''

        uid  = task['uid']
        func = task['description']['function']
        assert func

        args    = task['description'].get('args',   [])
        kwargs  = task['description'].get('kwargs', {})
        py_func = False

        self._log.debug('orig args: %s : %s', args, kwargs)

        # check if `func_name` is a global name
        names   = dict(list(globals().items()) + list(locals().items()))
        to_call = names.get(func)

        # if not, check if this is a class method of this worker implementation
        if not to_call:
            to_call = getattr(self, func, None)

        # check if we have a serialized object
        if not to_call:
            self._log.debug('func serialized: %d: %s', len(func), func)

            try:
                to_call, _args, _kwargs = PythonTask.get_func_attr(func)

            except DeserializationError as e:
                self._log.warn('failed to deserialize function for [%s]: %s', uid, str(e))
                out, val = None, None
                exc = (repr(e), '\n'.join(ru.get_exception_trace()))
                err = f'call failed: {e}'
                ret = 1
                return out, err, ret, val, exc

            except Exception:
                self._log.warn('function is not a PythonTask [%s]', uid)
            else:
                py_func = True
                if args or kwargs:
                    raise ValueError('`args` and `kwargs` must be empty for '
                                     'PythonTask function [%s]' % uid)
                else:
                    args   = _args
                    kwargs = _kwargs

        if not to_call:
            self._log.error('could not obtain callable from %s' % uid)
            raise ValueError('%s callable %s not found: %s' % (uid, func, task))

        comm = task.get('mpi_comm')
        if comm:
            # we have an MPI communicator we need to inject into the function's
            # arguments.
            if py_func:
                # For a `py_func` we add the communicator as `comm` kwarg if
                # that is set to None, and otherwise as first `arg` if that is
                # None.  If neither is true we'll error out.
                # NOTE that we don't change the number of arguments either way.
                if 'comm' in kwargs and kwargs['comm'] is None:
                    kwargs['comm'] = comm
                elif args and args[0] is None:
                    args[0] = comm
                else:
                    raise RuntimeError("can't inject communicator for %s: %s: %s",
                                       task['uid'], args, kwargs)
            else:
                args.insert(0, comm)

        # make sure we capture stdout / stderr
        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        # set the task environment
        old_env = os.environ.copy()

        for k, v in task['description'].get('environment', {}).items():
            os.environ[k] = str(v)

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            self._prof.prof('rank_start', uid=uid)
            self._log.debug('to call %s: %s : %s', to_call, args, kwargs)
            if asyncio.iscoroutinefunction(to_call):
                self._log.debug('to call is async')
                val = await to_call(*args, **kwargs)
            else:
                self._log.debug('to call is sync')
                val = to_call(*args, **kwargs)
            self._prof.prof('rank_stop', uid=uid)
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = (None, None)
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s', task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            exc = (repr(e), '\n'.join(ru.get_exception_trace()))
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            # remove communicator from args again
            if comm:
                if py_func:
                    if 'comm' in kwargs:
                        del kwargs['comm']
                    elif args:
                        args[0] = None
                else:
                    args.pop(0)

            os.environ = old_env

        self._log.debug('%s: got %s', uid, out)

        return out, err, ret, val, exc


    # --------------------------------------------------------------------------
    #
    def _dispatch_eval(self, task):
        '''
        We expect a single attribute: 'code', containing the Python
        code to be eval'ed
        '''

        uid  = task['uid']
        code = task['description']['code']
        assert code

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        old_env = os.environ.copy()

        for k, v in task['description'].get('environment', {}).items():
            os.environ[k] = str(v)

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            self._log.debug('eval [%s] [%s]', code, task['uid'])

            self._prof.prof('rank_start', uid=uid)
            val = eval(code)
            self._prof.prof('rank_stop', uid=uid)
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = (None, None)
            ret = 0

        except Exception as e:
            self._log.exception('_eval failed: %s', task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\neval failed: %s' % e)
            exc = (repr(e), '\n'.join(ru.get_exception_trace()))
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env

        return out, err, ret, val, exc



    # --------------------------------------------------------------------------
    #
    def _dispatch_exec(self, task):
        '''
        We expect a single attribute: 'code', containing the Python code to be
        exec'ed.  The optional attribute `pre_exec` can be used for any import
        statements and the like which need to run before the executed code.
        '''

        bak_stdout = sys.stdout
        bak_stderr = sys.stderr

        strout = None
        strerr = None

        old_env = os.environ.copy()

        for k, v in task['description'].get('environment', {}).items():
            os.environ[k] = str(v)

        try:
            # redirect stdio to capture them during execution
            sys.stdout = strout = io.StringIO()
            sys.stderr = strerr = io.StringIO()

            uid  = task['uid']
            pre  = task['description'].get('pre_exec', [])
            code = task['description']['code']

            # create a wrapper function around the given code
            lines = code.split('\n')
            outer = 'def _my_exec():\n'
            for line in lines:
                outer += '    ' + line + '\n'

            # call that wrapper function via exec, and keep the return value
            src = '%s\n\n%s\n\nresult=_my_exec()' % ('\n'.join(pre), outer)

            # assign a local variable to capture the code's return value.
            loc = dict()
            self._prof.prof('rank_start', uid=uid)
            exec(src, {}, loc)                # pylint: disable=exec-used # noqa
            self._prof.prof('rank_stop', uid=uid)
            val = loc['result']
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = (None, None)
            ret = 0

        except Exception as e:
            self._log.exception('_exec failed: %s', task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\nexec failed: %s' % e)
            exc = (repr(e), '\n'.join(ru.get_exception_trace()))
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

            os.environ = old_env

        return out, err, ret, val, exc


    # --------------------------------------------------------------------------
    #
    def _dispatch_proc(self, task):
        '''
        We expect two attributes: 'executable', containing the executabele to
        run, and `arguments` containing a list of arguments (strings) to pass as
        command line arguments.  We use `sp.Popen` to run the fork/exec, and to
        collect stdout, stderr and return code
        '''

        try:
            import subprocess as sp

            uid  = task['uid']
            exe  = task['description']['executable']
            args = task['description'].get('arguments', list())
            env  = dict(self._task_env)
            env.update(task['description']['environment'])

            cmd  = '%s %s' % (exe, ' '.join([shlex.quote(arg) for arg in args]))
            self._prof.prof('rank_start', uid=uid)
            proc = sp.Popen(cmd, env=env,  stdin=None,
                            stdout=sp.PIPE, stderr=sp.PIPE,
                            close_fds=True, shell=True)
            out, err = proc.communicate()
            ret      = proc.returncode
            exc      = (None, None)
            self._prof.prof('rank_stop', uid=uid)

        except Exception as e:
            self._log.exception('proc failed: %s', task['uid'])
            out = None
            err = 'exec failed: %s' % e
            exc = (repr(e), '\n'.join(ru.get_exception_trace()))
            ret = 1

        return out, err, ret, None, exc


    # --------------------------------------------------------------------------
    #
    def _dispatch_shell(self, task):
        '''
        We expect a single attribute: 'command', containing the command
        line to be called as string.
        '''

        try:
            uid = task['uid']
            cmd = task['description']['command']
            env = dict(self._task_env)
            env.update(task['description']['environment'])

          # self._log.debug('shell: --%s--', cmd)

            self._prof.prof('rank_start', uid=uid)
            out, err, ret = ru.sh_callout(cmd, shell=True, env=env)
            exc = (None, None)
            self._prof.prof('rank_stop', uid=uid)

        except Exception as e:
            self._log.exception('_shell failed: %s', task['uid'])
            out = None
            err = 'shell failed: %s' % e
            exc = (repr(e), '\n'.join(ru.get_exception_trace()))
            ret = 1

      # os.environ = old_env

        return out, err, ret, None, exc


    # --------------------------------------------------------------------------
    #
    def hello(self, msg, sleep=0):

        print('hello %s: %.3f' % (msg, time.time()))
        time.sleep(sleep)
        print('hello %s: %.3f' % (msg, time.time()))
        return 'hello %s' % msg


# ------------------------------------------------------------------------------

