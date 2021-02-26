__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import dill
import pickle
import codecs

from io import StringIO
import subprocess      as sp
import multiprocessing as mp
import threading       as mt

from whichcraft import which

import radical.utils   as ru


class MPI_Func_Worker():

    def __init__(self):

        self._log     = ru.Logger(name='mpi_func_exec', level='DEBUG')
        #ru.Logger(self._uid,   ns='radical.pilot', path=self._pwd)
        self._log.debug('MPI worker got init')

        self.THIS_SCRIPT = os.path.realpath(__file__)
        self.MPIRUN      = ['mpirun']
        self.MPIRUN[0]   = which(self.MPIRUN[0])
        if not os.path.exists(self.MPIRUN[0]):
            raise RuntimeError('Cannot find mpirun')
        #self._pwd           = os.getcwd()
        #self._uid     = os.environ['RP_FUNCS_ID']
        

    
    # --------------------------------------------------------------------------
    #
    def prepare_func(self, func):

        self._log.debug('Prepare func Got called with')
        self._log.debug(func)
        function_info = {}
        function_info = pickle.loads(codecs.decode(func.encode(), "base64"))
    
        code        = function_info["_cud_code"]
        args        = function_info["_cud_args"]
        kwargs      = function_info["_cud_kwargs"]
    
        from radical.pilot.serialize import serializer as serialize
    
        fn = serialize.FuncSerializer.deserialize(code)
        return fn, args, kwargs
    
    
    def launch_mpirun_func(self, func, **kwargs):

        self._log.debug('launch mpirun task file Got called with')
        self._log.debug(func)
        cmds = self.mpirun_cmds(func, **kwargs)
        self._log.debug('MPIRUN Command is %s',cmds)
        p_env = os.environ.copy()
        p_env['PYTHONPATH'] = ':'.join([os.getcwd()] + os.environ.get('PYTHONPATH', '').split(':'))
        
        p = sp.Popen(cmds, env=p_env, stdout = sp.PIPE,
                                      stderr = sp.PIPE)
        retcode = p.wait()
        result = []

        if retcode != 0:
            self._log.error('Failed to run task')
            for line in iter(p.stderr.readline,b''):
                result.append(line.rstrip())

            proc_err = ru.as_string(result)
            return 'FAILED', proc_err 
        else:
            for line in iter(p.stdout.readline,b''):
                result.append(line.rstrip())
            proc_out = ru.as_string(result)
            return 'DONE', proc_out
    
    def mpirun_cmds(self, func, **kwargs):
        self._log.debug('mpirun cmds Got called with')
        self._log.debug(func)
        cmds = list(self.MPIRUN)
        for k in kwargs:
            mpiarg = '-{}'.format(str(k).replace('_', '-'))
            cmds.append(mpiarg)
            v = kwargs[k]
            if v is not None:
                cmds.append(str(v))
        cmds.extend([sys.executable, self.THIS_SCRIPT, func])
        
        return cmds
    
    def mpirun_func(self, func):
        self._log.debug('mpirun task file got called with')
        self._log.debug(func)
        fn, args, kwargs = self.prepare_func(func)
        self._log.debug(fn)
        self._log.debug(args)

        try:
            from mpi4py import MPI
            from mpi4py.futures import MPICommExecutor
            MPI.pickle.__init__(dill.dumps, dill.loads)
            with MPICommExecutor(MPI.COMM_WORLD, root=0) as executor:
                 if executor is not None:
                    result =  executor.map(fn, *args)
                    self._log.debug(result)

        except Exception as e:
            self._log.error(e)
            raise RuntimeError('Task failed to run here %s', e)

if __name__ == '__main__':
    func = sys.argv[1]
    execute   = MPI_Func_Worker()
    execute.mpirun_func(func)