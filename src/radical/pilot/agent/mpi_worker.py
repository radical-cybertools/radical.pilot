__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import sys
import dill

import subprocess      as sp

import radical.utils   as ru


class MPI_Func_Worker():

    def __init__(self):

        self._pwd  = os.getcwd()
        self._uid  = os.environ.get('RP_FUNCS_ID')
        self._log  = ru.Logger(self._uid,   ns='radical.pilot', path=self._pwd)
        self._log.debug('MPI worker got init')

        self.THIS_SCRIPT = os.path.realpath(__file__)
        self.MPIRUN      = ['mpirun']
        self.MPIRUN[0]   = ru.which(self.MPIRUN[0])
        if not os.path.exists(self.MPIRUN[0]):
            raise RuntimeError('mpirun not found')

    # --------------------------------------------------------------------------
    #    
    def launch_mpirun_func(self, func, args, hosts,**kwargs):

        self._log.debug('launch mpirun task file Got called with')
        self._log.debug(func)

        cmds = self.construct_mpirun_cmds(func, args, hosts, **kwargs)
        self._log.debug('MPIRUN Command is %s',cmds)
        p_env = os.environ.copy()
        p_env['PYTHONPATH'] = ':'.join([os.getcwd()] + os.environ.get('PYTHONPATH', '').split(':'))

        with sp.Popen(cmds, env=p_env, stdout= sp.PIPE, stderr= sp.PIPE) as proc:
            stdout, stderr = proc.communicate()
            retcode = proc.returncode

        if retcode != 0:
            self._log.error('Failed to run task due to {0}'.format(stderr))
            proc_err = stderr
            return 'FAILED', proc_err 
        else:
            proc_out = stdout
            return 'DONE', proc_out

    def construct_mpirun_cmds(self, func, args, hosts, **kwargs):

        mpi_kwargs = {}

        if 'cpu_processes' in kwargs:
            if hosts:
                p1 = eval(hosts)
                p2 = ",".join([str(s) for s in list(p1)])
                mpi_kwargs['np']   = len(p1)
                mpi_kwargs['host'] = p2
            else:
                mpi_kwargs['np'] = kwargs['cpu_processes']

        else:
            pass
        if 'cpu_threads' in kwargs:
            os.environ['OMP_NUM_THREADS'] = str(kwargs['cpu_threads'])
        else:
            pass

        self._log.debug('mpirun cmds Got called with')
        self._log.debug(func)
        cmds = list(self.MPIRUN)
        for k in mpi_kwargs:
            mpi_arg = '-{}'.format(str(k).replace('_', '-'))
            cmds.append(mpi_arg)
            v = mpi_kwargs[k]
            if v is not None:
                cmds.append(str(v))
        cmds.extend([sys.executable, self.THIS_SCRIPT, func, args])

        return cmds


    def mpirun_func(self, hex_func, args):
        self._log.debug('mpirun task file got called with')
        args = eval(args)

        try:
            from mpi4py import MPI
            from mpi4py.futures import MPICommExecutor
            MPI.pickle.__init__(dill.dumps, dill.loads)

            func = bytes.fromhex(hex_func)
            fn = MPI.pickle.loads(func)
            with MPICommExecutor(MPI.COMM_WORLD, root=0) as executor:
                if executor is not None:
                    result =  executor.map(fn, *args)

        except Exception as e:
            self._log.exception('MPICommExecutor failed due to: %s', e)


if __name__ == '__main__':
    func = sys.argv[1]
    args = sys.argv[2]
    execute   = MPI_Func_Worker()
    execute.mpirun_func(func, args)
