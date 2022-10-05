# pylint: disable=import-error

import io
import os
import sys
import time
import shlex

import threading           as mt
import radical.utils       as ru

from .worker            import Worker

from ..pytask           import PythonTask
from ..task_description import TASK_FUNCTION
from ..task_description import TASK_EXEC, TASK_PROC, TASK_SHELL, TASK_EVAL


# MPI message tags
TAG_REGISTER_REQUESTS    = 110
TAG_REGISTER_REQUESTS_OK = 111
TAG_SEND_TASK            = 120
TAG_RECV_RESULT          = 121
TAG_REGISTER_RESULTS     = 130
TAG_REGISTER_RESULTS_OK  = 131

# message payload constants
MSG_PING = 210
MSG_PONG = 211
MSG_OK   = 220
MSG_NOK  = 221

# resource allocation flags
FREE = 0
BUSY = 1


# ------------------------------------------------------------------------------
#
class _Resources(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, log, prof, ranks):

        self._log   = log
        self._prof  = prof
        self._ranks = ranks

        # FIXME: RP considers MPI tasks to be homogeneous, in that all ranks of
        #        the task have the same set of resources allocated.  That
        #        implies that all ranks in this MPI worker have the same
        #        resources allocated.  That implies that, in most cases, no rank
        #        has GPUs alloated (we place one rank per core, but the number
        #        of cores per node is in general different than the number of
        #        GPUs per node).
        #
        #        RP will need to support heterogeneous MPI tasks to allow this
        #        worker to also assign GPUs to specific ranks.
        #
        self._res_evt   = mt.Event()  # signals free resources
        self._res_lock  = mt.Lock()   # lock resource for alloc / deallock
        self._resources = {
                'cores': [0] * self._ranks
              # 'gpus' : [0] * self._n_gpus
        }

        # resources are initially all free
        self._res_evt.set()


    # --------------------------------------------------------------------------
    #
    def __str__(self):

        out = ':'
        for r in self._resources['cores']:
            if r == FREE: out += '-'
            else        : out += '#'
        out += ':'
      # for r in self._resources['gpus']:
      #     if r == FREE: out += '-'
      #     else        : out += '#'
      # out += ':'
        return out


    # --------------------------------------------------------------------------
    #
    @property
    def log(self): return self._log

    @property
    def prof(self): return self._prof

    @property
    def ranks(self): return self._ranks


    # --------------------------------------------------------------------------
    #
    def alloc(self, task):
        '''
        This call will search for free cores and gpus to run the task.  More
        precisely, the core will wait for a sufficient number of ranks to become
        available whose resources are suitable to run the task.  The call will
        block until those ranks are found.
        '''

        # FIXME: handle threads
        # FIXME: handle GPUs

        uid = task['uid']

        self._log.debug_5('alloc %s', uid)
        self._prof.prof('schedule_try', uid=uid)

        cores = task['description'].get('ranks', 1)

        if cores > self._ranks:
            raise ValueError('insufficient resources to run task (%d > %d'
                    % (cores, self._ranks))

        self._log.debug_5('alloc %s: %s', task['uid'], cores)
        self._log.info('  alloc    %30s: %s', uid, self)

        while True:

            if self._res_evt.is_set():

                with self._res_lock:

                    if cores > self._resources['cores'].count(FREE):
                        self._res_evt.clear()
                        continue

                    ranks = list()
                    for rank in range(self._ranks):

                        if self._resources['cores'][rank] == FREE:

                            self._resources['cores'][rank] = BUSY
                            ranks.append(rank)

                            if len(ranks) == cores:
                                self._prof.prof('schedule_ok', uid=uid)
                                self._log.info('  alloc ok %30s: %s', uid, self)
                                return ranks
            else:
                self._res_evt.wait(timeout=0.1)


    # --------------------------------------------------------------------------
    #
    def dealloc(self, task):
        '''
        deallocate task ranks
        '''

        uid   = task['uid']
        ranks = task['ranks']
        self._prof.prof('unschedule_start', uid=uid)
        self._log.info('dealloc    %30s: %s', uid, self)

        with self._res_lock:

            for rank in ranks:
                self._resources['cores'][rank] = FREE

            # signal available resources
            self._res_evt.set()

        self._prof.prof('unschedule_stop', uid=uid)
        self._log.info('dealloc ok %30s: %s', uid, self)

        # remove temporary information from task
        del task['rank']
        del task['ranks']


# ------------------------------------------------------------------------------
#
class _TaskPuller(mt.Thread):
    '''
    This class will pull tasks from the master, allocate suitable ranks for
    it's execution, and push the task to those ranks
    '''

    def __init__(self, worker_task_q_get, worker_result_q_put,
                       rank_task_q_put, event, resources, log, prof):

        super().__init__()

        self.daemon               = True
        self._worker_task_q_get   = worker_task_q_get
        self._worker_result_q_put = worker_result_q_put
        self._rank_task_q_put     = rank_task_q_put
        self._event               = event
        self._resources           = resources
        self._log                 = log
        self._prof                = prof
        self._ranks               = self._resources.ranks


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This callback gets tasks from the master, schedules resources for the
        tasks, and pushes them out to the respective ranks for execution.  If
        a task arrives for which no resources are available, the thread will
        block until such resources do become available.
        '''
        self._log.debug('init task puller 0 wtq_get:%s wrq_put:%s rtq_put:%s',
                        self._worker_task_q_get, self._worker_result_q_put,
                        self._rank_task_q_put)

        try:
            # register callback to receive tasks
            # connect to the master's task queue
            worker_task_q = ru.zmq.Getter('raptor_tasks',
                                          url=self._worker_task_q_get,
                                          log=self._log, prof=self._prof)

            # send tasks to worker ranks
            rank_task_q = ru.zmq.Putter('rank_tasks', url=self._rank_task_q_put,
                                        log=self._log, prof=self._prof)

            # also connect to the master's result queue to inform about errors
            worker_result_q = ru.zmq.Putter('raptor_results',
                                            url=self._worker_result_q_put,
                                            log=self._log, prof=self._prof)

            time.sleep(1)
            # setup is completed - signal main thread
            self._event.set()

            while True:

                tasks = None
                try:
                    tasks = worker_task_q.get_nowait(timeout=1)
                except:
                    self._log.exception('pull error')

                if not tasks:
                    continue

                tasks = ru.as_list(tasks)
                self._log.debug('wtq tasks: %s', len(tasks))

                # TODO: sort tasks by size
                for task in ru.as_list(tasks):

                    self._log.debug('wtq %s 0 - task pulled', task['uid'])

                    try:
                        task['ranks'] = self._resources.alloc(task)
                        for rank in task['ranks']:
                            task['rank'] = rank
                            self._log.debug('wtq %s 1 - task send to %d %s',
                                             task['uid'], rank, task['ranks'])
                            rank_task_q.put(task, qname=str(rank))

                    except Exception as e:
                        self._log.exception('failed to place task')
                        task['error'] = str(e)
                        worker_result_q.put(task)

        except:
            self._log.exception('task puller cb failed')


# --------------------------------------------------------------------------
#
class _ResultPusher(mt.Thread):
    '''
    This helper class will wait for result messages from ranks which completed
    the execution of a task.  It will collect results from all ranks which
    belong to that specific task and then send the results back to the master.
    '''

    def __init__(self, worker_result_q_put, rank_result_q_get, event,
                       resources, log, prof):

        super().__init__()

        self.daemon               = True
        self._worker_result_q_put = worker_result_q_put
        self._rank_result_q_get   = rank_result_q_get
        self._event               = event
        self._resources           = resources
        self._log                 = log
        self._prof                = prof


    # --------------------------------------------------------------------------
    #
    def _check_ranks(self, task):
        '''
        collect results of task ranks

        Returns `True` once all ranks are collected - the task then contains the
        collected results
        '''
        uid   = task['uid']
        ranks = task['description'].get('ranks', 1)

        if uid not in self._cache:
            self._cache[uid] = list()

        self._cache[uid].append(task)

        # do we have all ranks?
        if len(self._cache[uid]) < ranks:
            self._log.info('< results - recv: %s [%d < %d]', task['uid'],
                    len(self._cache[uid]), ranks)

            return False

        self._log.info('< results - recv: %s [%d = %d]', task['uid'],
                        len(self._cache[uid]), ranks)

        task['stdout']       = [t['stdout']       for t in self._cache[uid]]
        task['stderr']       = [t['stderr']       for t in self._cache[uid]]
        task['return_value'] = [t['return_value'] for t in self._cache[uid]]

        exit_codes           = [t['exit_code']    for t in self._cache[uid]]
        task['exit_code']    = sorted(list(set(exit_codes)))[-1]

        return True


    # --------------------------------------------------------------------------
    #
    def run(self):
        '''
        This thread pulls tasks from the master, schedules resources for the
        tasks, and pushes them out to the respective ranks for execution.  If
        a task arrives for which no resources are available, the thread will
        block until such resources do become available.
        '''

        try:
            self._log.debug('init result pusher wrq_put:%s rrq_get:%s',
                            self._worker_result_q_put, self._rank_result_q_get)

            # collect the results from all MPI ranks before returning
            self._cache = dict()

            # collect results from worker ranks
            rank_result_q = ru.zmq.Getter(channel='rank_results',
                                          url=self._rank_result_q_get,
                                          log=self._log,
                                          prof=self._prof)

            # collect results from worker ranks
            worker_result_q = ru.zmq.Putter(channel='raptor_results',
                                            url=self._worker_result_q_put,
                                            log=self._log, prof=self._prof)

            time.sleep(1)
            # signal success
            self._event.set()

            while True:

                # FIXME: use poller of callback
                tasks = ru.as_list(rank_result_q.get_nowait(timeout=100))

                for task in tasks:

                    self._log.debug('rrq %s %s [%s]', task['uid'],
                            task['rank'], task['ranks'])

                    # did all ranks complete?
                    if self._check_ranks(task):
                        self._resources.dealloc(task)
                        worker_result_q.put(task)


        except:
            self._log.exception('result pusher thread failed')


# ------------------------------------------------------------------------------
#
class _Worker(mt.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, rank_task_q_get, rank_result_q_put,
                       event, log, prof, base):

        super().__init__()

        self.daemon             = True
        self._rank_task_q_get   = rank_task_q_get
        self._rank_result_q_put = rank_result_q_put
        self._event             = event
        self._log               = log
        self._prof              = prof
        self._base              = base

        self._modes = dict()

        self.register_mode(TASK_FUNCTION, self._dispatch_function)
        self.register_mode(TASK_EVAL,     self._dispatch_eval)
        self.register_mode(TASK_EXEC,     self._dispatch_exec)
        self.register_mode(TASK_PROC,     self._dispatch_proc)
        self.register_mode(TASK_SHELL,    self._dispatch_shell)


    # --------------------------------------------------------------------------
    #
    def register_mode(self, name, dispatcher):

        if name in self._modes:
            raise ValueError('mode %s already registered' % name)

        self._modes[name] = dispatcher


    # --------------------------------------------------------------------------
    #
    def run(self):

        from mpi4py import MPI

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()
        self._rank  = self._world.rank
        self._ranks = self._world.size


        try:
            self._log.debug('init worker [%d] [%d] rtq_get:%s rrq_put:%s',
                            self._rank, self._ranks,
                            self._rank_task_q_get, self._rank_result_q_put)

            # get tasks from rank 0
            rank_task_q = ru.zmq.Getter('rank_tasks', url=self._rank_task_q_get,
                                        log=self._log, prof=self._prof)
            # send results back to rank 0
            rank_result_q = ru.zmq.Putter('rank_results',
                                          url=self._rank_result_q_put,
                                          log=self._log,  prof=self._prof)

            # signal success
            self._event.set()

            # get tasks, do them, push results back
            while True:

                # FIXME: use poller of callback
                # FIXME: avoid interrupts
                tasks = rank_task_q.get_nowait(qname=str(self._rank), timeout=100)

                if not tasks:
                    continue

                # FIXME: this worker should be scheduled for one task at
                #        a time - so why do we get more than one sometimes?
                if len(tasks) != 1:
                    self._log.error('more than one task for rank %s: %s',
                            self._rank, [t['uid'] for t in tasks])

                for task in tasks:

                    # this should never happen
                    if self._rank not in task['ranks']:
                        raise RuntimeError('internal error: inconsistent rank info')

                    # FIXME: task_exec_start
                    try:
                        out, err, ret, val, exc = self._dispatch(task)
                        self._log.debug('dispatch result: %s: %s', task['uid'], out)

                        task['error']        = None
                        task['stdout']       = out
                        task['stderr']       = err
                        task['exit_code']    = ret
                        task['return_value'] = val
                        task['exception']    = exc

                    except Exception as e:
                        task['error']        = repr(e)
                        task['stdout']       = ''
                        task['stderr']       = str(e)
                        task['exit_code']    = -1
                        task['return_value'] = None
                        task['exception']    = [e.__class__.__name__, str(e)]
                        self._log.exception('recv err  %s  to  0' % (task['uid']))

                    finally:
                        # send task back to rank 0
                        # FIXME: task_exec_stop
                        rank_result_q.put(task)

        except:
            self._log.exception('work thread failed [%s]', self._rank)


    # --------------------------------------------------------------------------
    #
    def _dispatch(self, task):

        task['description']['environment'].update(
              {'RP_TASK_ID'         : task['uid'],
               'RP_TASK_NAME'       : task.get('name'),
               'RP_TASK_SANDBOX'    : os.environ['RP_TASK_SANDBOX'],  # FIXME?
               'RP_PILOT_ID'        : os.environ['RP_PILOT_ID'],
               'RP_SESSION_ID'      : os.environ['RP_SESSION_ID'],
               'RP_RESOURCE'        : os.environ['RP_RESOURCE'],
               'RP_RESOURCE_SANDBOX': os.environ['RP_RESOURCE_SANDBOX'],
               'RP_SESSION_SANDBOX' : os.environ['RP_SESSION_SANDBOX'],
               'RP_PILOT_SANDBOX'   : os.environ['RP_PILOT_SANDBOX'],
               'RP_GTOD'            : os.environ['RP_GTOD'],
               'RP_PROF'            : os.environ['RP_PROF'],
               'RP_PROF_TGT'        : os.environ['RP_PROF_TGT'],
               'RP_RANKS'           : 1,  # dispatch_mpi will oveerwrite this
               'RP_RANK'            : 0,  # dispatch_mpi will oveerwrite this
               })


        uid = task['uid']
        self._prof.prof('rp_exec_start', uid=uid)
        try:
            if task['description']['ranks'] > 1:
                return self._dispatch_mpi(task)
            else:
                return self._dispatch_non_mpi(task)

        finally:
            self._prof.prof('rp_exec_stop', uid=uid)



    # --------------------------------------------------------------------------
    #
    def _dispatch_mpi(self, task):

        comm  = None
        group = None

        # create new communicator with all workers assigned to this task
        group = self._group.Incl(task['ranks'])
        comm  = self._world.Create_group(group)
        if not comm:
            out = None
            err = 'MPI setup failed'
            ret = 1
            val = None
            exc = None
            return out, err, ret, val, exc

        task['description']['environment']['RP_RANK']   = str(comm.rank)
        task['description']['environment']['RP_RANKS']  = str(comm.size)

        task['mpi_comm'] = comm

        try:
            return self._dispatch_non_mpi(task)

        finally:
            if 'mpi_comm' in task:
                del task['mpi_comm']

            # sub-communicator must always be destroyed
            if group: group.Free()
            if comm : comm.Free()


    # --------------------------------------------------------------------------
    #
    def _dispatch_non_mpi(self, task):

        # work on task
        mode       = task['description']['mode']
        dispatcher = self._modes.get(mode)

        if not dispatcher:
            raise ValueError('no execution mode defined for %s' % mode)

        return dispatcher(task)


    # --------------------------------------------------------------------------
    #
    def _dispatch_function(self, task):
        '''
        We expect three attributes: 'function', containing the name of the
        member method or free function to call, `args`, an optional list of
        unnamed parameters, and `kwargs`, and optional dictionary of named
        parameters.

        *function* is resolved first against `locals()`, then `globals()`, then
        attributes of the implementation class (member functions of *base*, as
        provided to `_Worker()`). Finally, an attempt is made to deserialize a
        PythonTask from *function*. The first non-null resolution of *function*
        is used as the callable.

        Raises
        ------
        ValueError
            if *function* cannot be resolved.

        NOTE: MPI function tasks will get a private communicator passed as first
              unnamed argument.
        '''

        uid  = task['uid']
        func = task['description']['function']

        to_call = ''
        names   = ''
        args    = task['description'].get('args',   [])
        kwargs  = task['description'].get('kwargs', {})
        py_func = False

        self._log.debug('=== orig args: %s : %s', args, kwargs)

        # check if we have a serialized object
        self._log.debug('func serialized: %d: %s', len(func), func)
        try:
            # FIXME: can we have a better test than try/except?  This hides
            #        potential errors...
            # FIXME: ensure we did not get args and kwargs from above
            to_call, args, kwargs = PythonTask.get_func_attr(func)
            py_func = True
        except:
            pass

        # check if `func_name` is a global name
        if not to_call:
            assert func
            names   = dict(list(globals().items()) + list(locals().items()))
            to_call = names.get(func)

        # if not, check if this is a class method of this worker implementation
        if not to_call:
            to_call = getattr(self._base, func, None)

        # check if we have a serialized object
        if not to_call:
            self._log.debug('func serialized: %d: %s', len(func), func)

            try:
                to_call, _args, _kwargs = PythonTask.get_func_attr(func)

            except Exception:
                self._log.exception('function is not a PythonTask [%s] ', uid)

            else:
                py_func = True
                if args or kwargs:
                    raise ValueError('`args` and `kwargs` must be empty for'
                                     'PythonTask function [%s]' % uid)
                else:
                    args   = _args
                    kwargs = _kwargs

        if not to_call:
            self._log.error('no %s in \n%s\n\n%s', func, names, dir(self._base))
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
            self._log.debug('=== to call %s: %s : %s', to_call, args, kwargs)
            val = to_call(*args, **kwargs)
            self._prof.prof('rank_stop', uid=uid)
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            exc = [e.__class__.__qualname__, str(e)]
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

        self._log.debug('=== %s: got %s', uid, out)

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

            self._log.debug('eval [%s] [%s]' % (code, task['uid']))

            self._prof.prof('rank_start', uid=uid)
            val = eval(code)
            self._prof.prof('rank_stop', uid=uid)
            out = strout.getvalue()
            err = strerr.getvalue()
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_eval failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\neval failed: %s' % e)
            exc = [e.__class__.__name__, str(e)]
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
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_exec failed: %s' % task['uid'])
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\nexec failed: %s' % e)
            exc = [e.__class__.__name__, str(e)]
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
            tenv = task['description'].get('environment', dict())

            cmd  = '%s %s' % (exe, ' '.join([shlex.quote(arg) for arg in args]))
          # self._log.debug('proc: --%s--', args)
            self._prof.prof('rank_start', uid=uid)
            proc = sp.Popen(cmd, env=tenv,  stdin=None,
                            stdout=sp.PIPE, stderr=sp.PIPE,
                            close_fds=True, shell=True)
            out, err = proc.communicate()
            ret      = proc.returncode
            exc      = None
            self._prof.prof('rank_stop', uid=uid)

        except Exception as e:
            self._log.exception('proc failed: %s' % task['uid'])
            out = None
            err = 'exec failed: %s' % e
            exc = [e.__class__.__name__, str(e)]
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
            env = task['description']['environment']
          # self._log.debug('shell: --%s--', cmd)
            self._prof.prof('rank_start', uid=uid)
            out, err, ret = ru.sh_callout(cmd, shell=True, env=env)
            exc = None
            self._prof.prof('rank_stop', uid=uid)

        except Exception as e:
            self._log.exception('_shell failed: %s' % task['uid'])
            out = None
            err = 'shell failed: %s' % e
            exc = [e.__class__.__name__, str(e)]
            ret = 1

      # os.environ = old_env

        return out, err, ret, None, exc


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
    to the master.  The communication between rank 0 and the other ranks is
    through two ZMQ queues which are created and managed by rank 0.

    The main thread of rank 0 will function like the other threads: wait for
    task startup info and enact them.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg=None, session=None):

        self._my_term = mt.Event()

        from mpi4py import MPI                                            # noqa

        self._world = MPI.COMM_WORLD
        self._group = self._world.Get_group()

        self._rank  = self._world.rank
        self._ranks = self._world.size

        if self._rank == 0: self._manager = True
        else              : self._manager = False

        # rank 0 will register the worker with the master and connect
        # to the task and result queues
        super().__init__(cfg=cfg, session=session, register=self._manager)


        # rank 0 starts two ZMQ queues: one to send tasks to the worker ranks
        # (rank_task_q), and one to collect results from the ranks
        # (rank_result_q)
        info = None
        if self._manager:

            self._worker_task_q_get   = self._cfg.info.req_addr_get
            self._worker_result_q_put = self._cfg.info.res_addr_put

            self._rank_task_q   = ru.zmq.Queue(channel='rank_task_q')
            self._rank_result_q = ru.zmq.Queue(channel='rank_result_q')

            self._rank_task_q.start()
            self._rank_result_q.start()

            info = {'rank_task_q_put'  : str(self._rank_task_q.addr_put),
                    'rank_task_q_get'  : str(self._rank_task_q.addr_get),
                    'rank_result_q_put': str(self._rank_result_q.addr_put),
                    'rank_result_q_get': str(self._rank_result_q.addr_get)}

            # let channels settle
            time.sleep(1)

        # broadcast the queue endpoint addresses to all worker ranks
        info = self._world.bcast(info, root=0)

        self._rank_result_q_put = info['rank_result_q_put']
        self._rank_result_q_get = info['rank_result_q_get']
        self._rank_task_q_put   = info['rank_task_q_put']
        self._rank_task_q_get   = info['rank_task_q_get']


    # --------------------------------------------------------------------------
    #
    def start(self):

        # all ranks run a worker thread
        # the worker should be started before the managers as the manager
        # contacts the workers with queue endpoint information
        self._log.info('rank %s starts [%s]', self._rank, self._manager)
        worker_ok = mt.Event()
        self._work_thread = _Worker(rank_task_q_get   = self._rank_task_q_get,
                                    rank_result_q_put = self._rank_result_q_put,
                                    event             = worker_ok,
                                    log               = self._log,
                                    prof              = self._prof,
                                    base              = self)
        self._work_thread.start()
        worker_ok.wait(timeout=60)
        self._log.info('rank %s starts worker [%s]', self._rank, self._manager)

        if not worker_ok.is_set():
            raise RuntimeError('failed to start worker thread')


        # the manager (rank 0) will start two threads - one to pull tasks from
        # the master, one to push results back to the master
        if self._manager:

            self._log.info('rank %s starts managers', self._rank)

            resources = _Resources(self._log,   self._prof, self._ranks)
            self._log.info('resources: %s', resources)

            # rank 0 spawns manager threads
            pull_ok = mt.Event()
            push_ok = mt.Event()

            self._log.debug('wrq_put:%s wtq_get:%s',
                            self._worker_result_q_put, self._worker_task_q_get)

            self._pull = _TaskPuller(
                    worker_task_q_get   = self._worker_task_q_get,
                    worker_result_q_put = self._worker_result_q_put,
                    rank_task_q_put     = self._rank_task_q.addr_put,
                    event               = pull_ok,
                    resources           = resources,
                    log                 = self._log,
                    prof                = self._prof)

            self._push = _ResultPusher(
                    worker_result_q_put = self._worker_result_q_put,
                    rank_result_q_get   = self._rank_result_q.addr_get,
                    event               = push_ok,
                    resources           = resources,
                    log                 = self._log,
                    prof                = self._prof)

            self._pull.start()
            self._push.start()

            pull_ok.wait(timeout=60)
            push_ok.wait(timeout=60)

            if not pull_ok.is_set():
                raise RuntimeError('failed to start pull thread')

            if not push_ok.is_set():
                raise RuntimeError('failed to start push thread')

        self._log.info('rank %s starts [%s] ok', self._rank, self._manager)


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
    def _call(self, task):
        '''
        We expect data to have a three entries: 'method' or 'function',
        containing the name of the member method or the name of a free function
        to call, `args`, an optional list of unnamed parameters, and `kwargs`,
        and optional dictionary of named parameters.
        '''

        data = task['data']

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
            exc = None
            ret = 0

        except Exception as e:
            self._log.exception('_call failed: %s' % (data))
            val = None
            out = strout.getvalue()
            err = strerr.getvalue() + ('\ncall failed: %s' % e)
            exc = [e.__class__.__name__, str(e)]
            ret = 1

        finally:
            # restore stdio
            sys.stdout = bak_stdout
            sys.stderr = bak_stderr

        res = [task, out, err, ret, val, exc]

        return res


    # --------------------------------------------------------------------------
    #
    def hello(self, msg, sleep=0):

        print('hello: %s' % msg)
        time.sleep(sleep)


    # --------------------------------------------------------------------------
    #
    def hello_mpi(self, comm, msg, sleep=0):

        print('hello %d/%d: %s' % (comm.rank, comm.size, msg))
        time.sleep(sleep)


# ------------------------------------------------------------------------------

