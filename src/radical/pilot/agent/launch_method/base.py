
# pylint: disable=protected-access

__copyright__ = 'Copyright 2016-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import time
import signal

import radical.utils as ru


# 'enum' for launch method types
LM_NAME_APRUN         = 'APRUN'
LM_NAME_CCMRUN        = 'CCMRUN'
LM_NAME_FORK          = 'FORK'
LM_NAME_IBRUN         = 'IBRUN'
LM_NAME_DRAGON        = 'DRAGON'
LM_NAME_MPIEXEC       = 'MPIEXEC'
LM_NAME_MPIEXEC_MPT   = 'MPIEXEC_MPT'
LM_NAME_MPIRUN        = 'MPIRUN'
LM_NAME_MPIRUN_MPT    = 'MPIRUN_MPT'
LM_NAME_MPIRUN_CCMRUN = 'MPIRUN_CCMRUN'
LM_NAME_MPIRUN_DPLACE = 'MPIRUN_DPLACE'
LM_NAME_MPIRUN_RSH    = 'MPIRUN_RSH'
LM_NAME_JSRUN         = 'JSRUN'
LM_NAME_JSRUN_ERF     = 'JSRUN_ERF'
LM_NAME_PRTE          = 'PRTE'
LM_NAME_FLUX          = 'FLUX'
LM_NAME_RSH           = 'RSH'
LM_NAME_SSH           = 'SSH'
LM_NAME_SRUN          = 'SRUN'

PWD = os.getcwd()


# FIXME: add a well defined LMInfo TypedDict, similar to RMInfo


# ------------------------------------------------------------------------------
#
class LaunchMethod(object):

    MPI_FLAVOR_OMPI     = 'OMPI'
    MPI_FLAVOR_HYDRA    = 'HYDRA'
    MPI_FLAVOR_SPECTRUM = 'SPECTRUM'
    MPI_FLAVOR_PALS     = 'PALS'
    MPI_FLAVOR_UNKNOWN  = 'unknown'

    LM_INVALID          = 'invalid'
    LM_EMPTY            = 'empty'

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self.name       = name
        self._lm_cfg    = lm_cfg
        self._rm_info   = rm_info
        self._log       = log
        self._prof      = prof
        self._pwd       = os.getcwd()
        self._env_orig  = ru.env_eval('env/bs0_orig.env')
        self._in_pytest = False

        reg     = ru.zmq.RegistryClient(url=self._lm_cfg.reg_addr)
        lm_info = reg.get('lm.%s' % self.name.lower())

        self._log.debug('initialize LM %s', self.name)

        if lm_info == self.LM_INVALID:
            self._log.warn('LM info invalid - skip %s', lm_info)

        elif lm_info == self.LM_EMPTY:
            self._log.info('LM info empty for %s', lm_info)

        elif not lm_info:

            # The registry does not yet contain any info for this LM - we need
            # to initialize the LM from scratch.  That happens in the env
            # defined by lm_cfg (derived from the original bs0 env)
            env_sh   = 'env/lm_%s.sh' % self.name.lower()
            env_lm   = ru.env_prep(environment=self._env_orig,
                          pre_exec=lm_cfg.get('pre_exec'),
                          pre_exec_cached=lm_cfg.get('pre_exec_cached'),
                          script_path=env_sh)

            lm_info = self._init_from_scratch(env_lm, env_sh)

            # store the info in the registry for any other instances of the LM
            reg.put('lm.%s' % self.name.lower(), lm_info)

        if lm_info != self.LM_INVALID:
            self.init_from_info(lm_info)

        reg.close()


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, lm_cfg, rm_info, log, prof):

        # Make sure that we are the base-class!
        if cls != LaunchMethod:
            raise TypeError("LaunchMethod create only available to base class!")

        # In case of undefined LM just return None
        if not name:
            return None

        from .aprun          import APRun
        from .ccmrun         import CCMRun
        from .fork           import Fork
        from .ibrun          import IBRun
        from .dragon         import Dragon
        from .mpiexec        import MPIExec
        from .mpirun         import MPIRun
        from .jsrun          import JSRUN
        from .prte           import PRTE
        from .flux           import Flux
        from .rsh            import RSH
        from .ssh            import SSH
        from .srun           import Srun

        impl = {
            LM_NAME_APRUN         : APRun,
            LM_NAME_CCMRUN        : CCMRun,
            LM_NAME_DRAGON        : Dragon,
            LM_NAME_FORK          : Fork,
            LM_NAME_FLUX          : Flux,
            LM_NAME_IBRUN         : IBRun,
            LM_NAME_MPIEXEC       : MPIExec,
            LM_NAME_MPIEXEC_MPT   : MPIExec,
            LM_NAME_MPIRUN        : MPIRun,
            LM_NAME_MPIRUN_CCMRUN : MPIRun,
            LM_NAME_MPIRUN_RSH    : MPIRun,
            LM_NAME_MPIRUN_MPT    : MPIRun,
            LM_NAME_MPIRUN_DPLACE : MPIRun,
            LM_NAME_JSRUN         : JSRUN,
            LM_NAME_JSRUN_ERF     : JSRUN,
            LM_NAME_PRTE          : PRTE,
            LM_NAME_RSH           : RSH,
            LM_NAME_SSH           : SSH,
            LM_NAME_SRUN          : Srun,
        }

        if name not in impl:
            raise ValueError('LaunchMethod %s unknown' % name)

        return impl[name](name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    def _init_from_scratch(self, env, env_sh):

        lm_info = None

        # run init_from_scratch in a process under the LM env
        try:
            self._envp = ru.EnvProcess(env=env)
            with self._envp:
                if self._envp:
                    try:
                        data = self.init_from_scratch(env, env_sh)
                        self._envp.put(data)
                    except:
                        self._log.exception('LM init failed')
                        raise

            lm_info = self._envp.get()

        except Exception:
            self._log.warn('LM init failed')
            lm_info = self.LM_INVALID

        # force non-empty lm_info
        if not lm_info:
            self._log.warn('LM init came up empty')
            lm_info = self.LM_EMPTY

        return lm_info


    # --------------------------------------------------------------------------
    #
    def init_from_scratch(self, env, env_sh):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def init_from_info(self, lm_info):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def cancel_task(self, task, pid):
        '''
        This method cancels the task, in the default case by killing the task's
        launch process identified by its process ID.
        '''

        # kill the process group which should include the actual launch method
        try:
            self._log.debug('killing task %s (%d)', task['uid'], pid)
            os.killpg(pid, signal.SIGTERM)

            # also send a SIGKILL to drive the message home.
            # NOTE: the `sleep` will limit the cancel throughput!
            try:
                time.sleep(0.1)
                os.killpg(pid, signal.SIGKILL)
            except OSError:
                pass

        except OSError:
            # lost race: task is already gone, we ignore this
            self._log.debug('task already gone: %s', task['uid'])


    # --------------------------------------------------------------------------
    #
    def get_task_named_env(self, env_name):

        # we assume that the launcher env is still active in the task execution
        # script.  We thus remove the launcher env from the task env before
        # applying the task env's pre_exec commands
        base = '%s/env/rp_named_env.%s' % (self._pwd, env_name)
        src  = '%s.env'                 %  base
        tgt  = '%s.%s.sh'               % (base, self.name.lower())

        # if the env does not yet exists - create
        # FIXME: this would need some file locking for concurrent executors. or
        #        add self._uid to path name
        if not os.path.isfile(tgt):

            src_env = ru.env_read(src)
            for ep in self.get_env_preserved():
                if ep in src_env:
                    src_env[ep] = f'${ep}:{src_env[ep]}'

            blacklist = self.get_env_blacklist()
            self._log.debug_5('blacklist: %s', blacklist)

            ru.env_prep(environment=src_env,
                        blacklist=blacklist,
                        unset=list(os.environ.keys()),
                        script_path=tgt)

        return tgt


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def get_exec(self, task):

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td['arguments']
        task_argstr  = self._create_arg_string(task_args)
        command      = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_partition_ids(self):

        # by default, launchers will only support a single partition
        return [0]


    # --------------------------------------------------------------------------
    #
    def _create_arg_string(self, args):

        if args:
            return ' '.join([ru.sh_quote(arg) for arg in args])
        else:
            return ''


    # --------------------------------------------------------------------------
    #
    def _get_mpi_info(self, exe):
        '''
        returns version and flavor of MPI version.
        '''

        version, flavor = None, None

        for info_opts in ['-V', '--version', '-info']:
            out, _, ret = ru.sh_callout('%s %s' % (exe, info_opts))
            if not ret:
                break

        if ret:
            raise RuntimeError('cannot identify MPI flavor [%s]' % exe)

        if 'pals' in exe.lower():
            flavor = self.MPI_FLAVOR_PALS

        for line in out.splitlines():

            if 'intel(r) mpi library for linux' in line.lower():
                # Intel MPI is hydra based
                version = line.split(',')[1].strip()
                flavor = self.MPI_FLAVOR_HYDRA

            elif 'hydra build details:' in line.lower():
                version = line.split(':', 1)[1].strip()
                flavor  = self.MPI_FLAVOR_HYDRA

            elif 'mvapich2' in line.lower():
                version = line.strip()
                flavor  = self.MPI_FLAVOR_HYDRA

            elif '(open mpi)' in line.lower():
                version = line.split(')', 1)[1].strip()
                flavor  = self.MPI_FLAVOR_OMPI

            elif 'ibm spectrum mpi' in line.lower():
                version = line.split(')', 1)[1].strip()
                flavor  = self.MPI_FLAVOR_SPECTRUM

            elif 'version' in line.lower():
                version = line.lower().split('version')[1].\
                          replace(':', '').strip()
                if not flavor:
                    flavor = self.MPI_FLAVOR_OMPI

            if version:
                break

        if not flavor:
            flavor = self.MPI_FLAVOR_UNKNOWN

        self._log.debug('mpi details [%s]: %s', exe, out)
        self._log.debug('mpi version: %s [%s]', version, flavor)

        return version, flavor


    # --------------------------------------------------------------------------
    #
    def get_env_blacklist(self):
        '''
        return a list of environment variable names which the launcher needs to
        ensure are *not* overwritten by the execution script or mechanism.
        Specifically, we need to make sure that any applied `named_env` will not
        set these variables.

        Individual launchers may extend this list with additional names.
        '''

        return [
                # RP internal variables
                'RP_*',

                # SANDBOX location
                'PWD',

                # resource details
                'ROCR_VISIBLE_DEVICES',
                'CUDA_VISIBLE_DEVICES',
        ]

    def get_env_preserved(self):
        '''List of environment variables that will be preserved. If a
        corresponding environment variable will be reassigned, then we extend
        its value with the original one (e.g., ENV_VAR=$ENV_VAR:new_value)'''
        return []


# ------------------------------------------------------------------------------

