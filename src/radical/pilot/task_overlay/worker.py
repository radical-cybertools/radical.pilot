
import sys
import time
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
    def __init__(self, cfg, info=None):

        if isinstance(cfg, str): cfg = ru.Config(cfg=ru.read_json(cfg))
        else                   : cfg = ru.Config(cfg=cfg)

        self._uid     = cfg.uid
        self._cpn     = cfg.cpn
        self._gpn     = cfg.gpn
        self._info    = info
        self._term    = mp.Event()
        self._modes   = dict()
        self._mdata   = dict()
        self._info    = ru.Config(cfg=cfg.get('info', {}))
        self._session = Session(cfg=cfg, _primary=False)

        rpu.Component.__init__(self, cfg, self._session)

        # We need to make sure to run only up to `gpn` tasks using a gpu
        # within that pool, so need a separate counter for that.  We use
        # a `fork` context to inherit log and profile handles.
        self._cpus_used = 0
        self._gpus_used = 0

        # create a multiprocessing pool with `cpn` worker processors.  Set
        # `maxtasksperchild` to `1` so that we get a fresh process for each
        # task.  That will also allow us to run command lines via `exec`,
        # effectively replacing the worker process in the pool for a specific
        # task.
        #
        # NOTE: The mp documentation is wrong; mp.Pool dies *not* have a context
        #       parameters.  Instead, the Pool has to be created within
        #       a context.
        ctx = mp.get_context('fork')
        self._pool = ctx.Pool(processes=self._cpn,
                              initializer=None,
                              maxtasksperchild=1)

        # connect to master
        self.register_subscriber(rpc.CONTROL_PUBSUB, self._control_cb)
        self.register_publisher(rpc.CONTROL_PUBSUB)

        # connect to the request / response ZMQ queues
        self._req_get = ru.zmq.Getter('to_req', self._info.req_addr_get,
                                                cb=self._request_cb)
        self._res_put = ru.zmq.Putter('to_res', self._info.res_addr_put)

        # the worker can return custom information which will be made available
        # to the master.  This can be used to communicate, for example, worker
        # specific communication endpoints.

        # `info` is a placeholder for any additional meta data communicated to
        # the worker
        self.publish(rpc.CONTROL_PUBSUB, {'cmd': 'worker_register',
                                          'arg': {'uid' : self._uid,
                                                  'info': self._info}})

        # the worker provides three builtin methods:
        # eval:  evaluate a piece of python code
        # exec:  execute  a command line (fork/exec)
        # shell: execute  a shell command
        # call:  execute  a method call of the worker class implementation
        self.register_mode('call',  self._call)
        self.register_mode('eval',  self._eval)
        self.register_mode('exec',  self._exec)
        self.register_mode('shell', self._shell)


    # --------------------------------------------------------------------------
    #
    def register_mode(self, name, executor):

        assert(name not in self._modes)

        self._modes[name] = executor
        self._mdata[name] = dict()


    # --------------------------------------------------------------------------
    #
    def register_call(self, name, method):

        # ensure the call mode is usable
        mode = 'call'

        assert(mode     in self._modes)
        assert(name not in self._mdata[mode])

        self._mdata[mode][name] = method


    # --------------------------------------------------------------------------
    #
    def _call(self, data):
        '''
        We expect data to have a three entries: 'method', containing the name of
        the member method to call, `args`, an optional list of unnamed
        parameters, and `kwargs`, and optional dictionary of named parameters.
        '''

        method = getattr(self, data['call'])
        args   = data.get('args',   [])
        kwargs = data.get('kwargs', {})
        self._log.debug('_call    : %s(%s, %s)' % (method, args, kwargs))

        try:
            out = method(*args, **kwargs)
            err = None
            ret = 0
            self._log.debug('_call ok : %s(%s, %s)' % (method, args, kwargs))

        except Exception as e:
            self._log.exception('_call nok: %s(%s, %s)' % (method, args, kwargs))
            out = None
            err = str(e)
            ret = 1

        return out, err, ret


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
            out = None
            err = str(e)
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
        import subprocess as sp

        exe  = data['exe'],
        args = data.get('args', []),
        env  = data.get('env',  {}),


        proc = sp.Popen(executable=exe, args=args,       env=env,
                        stdin=None,     stdout=sp.Pipe, stderr=sp.Pipe,
                        close_fds=True, shell=False)
        out, err = proc.communicate()
        ret      = proc.returncode

        return out, err, ret


    # --------------------------------------------------------------------------
    #
    def _shell(self, data):
        '''
        We expect data to have a single entry: 'cmd', containing the command
        line to be called as string.
        '''

        out, err, ret = ru.sh_callout(data['cmd'])
        return out, err, ret


    # --------------------------------------------------------------------------
    #
    def _request_cb(self, msg):
        '''
        grep call type from msg, check if such a method is registered, and
        invoke it.
        '''

        self._log.debug('requested %s', msg)

        try:
            mode = msg['mode']
            assert(mode in self._modes), 'no such call mode %s' % mode


            # ok, we have work to do.  Check the requirements to see  how many
            # cpus and gpus we need to mark as busy, and run the request in our
            # process pool
            #
            # FIXME: GPU / CPU accounting and assignment
            # FIXME: async pool assignment + collection in thread
            self._log.debug('request_cb: %s : %s : %s', mode, self._modes[mode],
                                                        msg.get('data'))
            out, err, ret = self._modes[mode](msg.get('data'))


        except Exception as e:
            self._log.exception('request failed')
            out = None
            err = str(e)
            ret = 1

        res  = {'req': msg['uid'],
                'out': out,
                'err': err,
                'ret': ret}

        self._res_put.put(res)


    # --------------------------------------------------------------------------
    #
    def _control_cb(self, topic, msg):

        self._log.debug('got control msg: %s: %s', topic, msg)

        if msg['cmd'] == 'worker_terminate':
            if msg['arg']['uid'] == self._uid:
                self._term.set()
                self.stop()
                sys.exit(0)


    # --------------------------------------------------------------------------
    #
    def run(self):

        while not self._term.is_set():
            time.sleep(1)


# ------------------------------------------------------------------------------

