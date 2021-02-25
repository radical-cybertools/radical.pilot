__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import dill
import pickle
import codecs

from io import StringIO
from subprocess import Popen
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
    
    
    def launch_mpirun_task_file(self, task_file, **kwargs):

        self._log.debug('launch mpirun task file Got called with')
        self._log.debug(task_file)
        cmds = self.mpirun_cmds(task_file, **kwargs)
        self._log.debug('MPIRUN Command is %s',cmds)
        p_env = os.environ.copy()
        p_env['PYTHONPATH'] = ':'.join([os.getcwd()] + os.environ.get('PYTHONPATH', '').split(':'))
        
        old_stdout = sys.stdout
        new_stdout = None
        result     = None
        
        try:
            sys.stdout = new_stdout = StringIO()
            p = Popen(cmds, env=p_env)
            retcode = p.wait()
        except Exception as e:
            return e
            
        if retcode != 0:
            self._log.error('Failed to run task')
            return ('Failed to run task')
        else:
            return new_stdout.getvalue()
    
    def mpirun_cmds(self, task_file, **kwargs):
        self._log.debug('mpirun cmds Got called with')
        self._log.debug(task_file)
        cmds = list(self.MPIRUN)
        for k in kwargs:
            mpiarg = '-{}'.format(str(k).replace('_', '-'))
            cmds.append(mpiarg)
            v = kwargs[k]
            if v is not None:
                cmds.append(str(v))
        cmds.extend([sys.executable, self.THIS_SCRIPT, task_file])
        
        return cmds
    
    def mpirun_task_file(self, func):
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
    task_file = sys.argv[1]
    execute   = MPI_Func_Worker()
    execute.mpirun_task_file(task_file)