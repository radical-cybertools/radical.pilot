# pylint: disable=protected-access

__copyright__ = 'Copyright 2016-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import shlex

import radical.utils as ru


# 'enum' for launch method types
LM_NAME_APRUN         = 'APRUN'
LM_NAME_CCMRUN        = 'CCMRUN'
LM_NAME_FORK          = 'FORK'
LM_NAME_IBRUN         = 'IBRUN'
LM_NAME_MPIEXEC       = 'MPIEXEC'
LM_NAME_MPIEXEC_MPT   = 'MPIEXEC_MPT'
LM_NAME_MPIRUN        = 'MPIRUN'
LM_NAME_MPIRUN_MPT    = 'MPIRUN_MPT'
LM_NAME_MPIRUN_CCMRUN = 'MPIRUN_CCMRUN'
LM_NAME_MPIRUN_DPLACE = 'MPIRUN_DPLACE'
LM_NAME_MPIRUN_RSH    = 'MPIRUN_RSH'
LM_NAME_JSRUN         = 'JSRUN'
LM_NAME_PRTE          = 'PRTE'
LM_NAME_FLUX          = 'FLUX'
LM_NAME_RSH           = 'RSH'
LM_NAME_SSH           = 'SSH'
LM_NAME_SRUN          = 'SRUN'

PWD = os.getcwd()


# ------------------------------------------------------------------------------
#
class LaunchMethod(object):

    MPI_FLAVOR_OMPI    = 'OMPI'
    MPI_FLAVOR_HYDRA   = 'HYDRA'
    MPI_FLAVOR_UNKNOWN = 'unknown'


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self.name      = name
        self._lm_cfg   = lm_cfg
        self._rm_info  = rm_info
        self._log      = log
        self._prof     = prof
        self._pwd      = os.getcwd()
        self._env_orig = ru.env_eval('env/bs0_orig.env')

        reg     = ru.zmq.RegistryClient(url=self._lm_cfg.reg_addr)
        lm_info = reg.get('lm.%s' % self.name.lower())
      # import pprint
      # self._log.debug('addr: %s', self._lm_cfg.reg_addr)
      # self._log.debug('name: %s', self.name)
      # self._log.debug('info: %s', pprint.pformat(lm_info))

        if not lm_info:

            # The registry does not yet contain any info for this LM - we need
            # to initialize the LM from scratch.  That happens in the env
            # defined by lm_cfg (derived from the original bs0 env)
            env_orig = ru.env_eval('env/bs0_orig.env')
            env_sh   = 'env/lm_%s.sh' % self.name.lower()
            env_lm   = ru.env_prep(environment=env_orig,
                          pre_exec=lm_cfg.get('pre_exec'),
                          pre_exec_cached=lm_cfg.get('pre_exec_cached'),
                          script_path=env_sh)

            # run init_from_scratch in a process under that derived env
            # FIXME
          # envp = ru.EnvProcess(env=env_lm)
          # with envp:
          #     data = self._init_from_scratch(env_lm, env_sh)
          #     envp.put(data)
          # lm_info = envp.get()

            lm_info = self._init_from_scratch(env_lm, env_sh)

            # store the info in the registry for any other instances of the LM
            reg.put('lm.%s' % self.name.lower(), lm_info)
          # self._log.debug('INFO: %s', pprint.pformat(lm_info))

        reg.close()
        self._init_from_info(lm_info)


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
        from .mpiexec        import MPIExec
        from .mpirun         import MPIRun
        from .jsrun          import JSRUN
        from .prte           import PRTE
        from .flux           import Flux
        from .rsh            import RSH
        from .ssh            import SSH
        from .srun           import Srun

        try:
            impl = {
                LM_NAME_APRUN         : APRun,
                LM_NAME_CCMRUN        : CCMRun,
                LM_NAME_FORK          : Fork,
                LM_NAME_IBRUN         : IBRun,
                LM_NAME_MPIEXEC       : MPIExec,
                LM_NAME_MPIEXEC_MPT   : MPIExec,
                LM_NAME_MPIRUN        : MPIRun,
                LM_NAME_MPIRUN_CCMRUN : MPIRun,
                LM_NAME_MPIRUN_RSH    : MPIRun,
                LM_NAME_MPIRUN_MPT    : MPIRun,
                LM_NAME_MPIRUN_DPLACE : MPIRun,
                LM_NAME_JSRUN         : JSRUN,
                LM_NAME_PRTE          : PRTE,
                LM_NAME_FLUX          : Flux,
                LM_NAME_RSH           : RSH,
                LM_NAME_SSH           : SSH,
                LM_NAME_SRUN          : Srun,

            }

            if name not in impl:
                raise ValueError('LaunchMethod %s unknown' % name)

            return impl[name](name, lm_cfg, rm_info, log, prof)

        except Exception:
            log.exception('unusable lm %s' % name)
            raise


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

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
    def get_task_named_env(self, env_name):

        # we assume that the launcher env is still active in the task execution
        # script.  We thus remove the launcher env from the task env before
        # applying the task env's pre_exec commands
        base = '%s/env/rp_named_env.%s' % (self._pwd, env_name)
        src  = '%s.sh'                  %  base
        tgt  = '%s.%s.sh'               % (base, self.name.lower())

        # the env does not yet exists - create
        # FIXME: this would need some file locking for concurrent executors. or
        #        add self._uid to path name
        if not os.path.isfile(tgt):
            ru.env_prep(ru.env_read(src), script_path=tgt)

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
    def get_rank_exec(self, task, rank_id, rank):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def get_partitions(self):

        return None


    # --------------------------------------------------------------------------
    #
    def _create_arg_string(self, args):

        if args:
            return ' '.join([shlex.quote(arg) for arg in args])
        else:
            return ''


    # --------------------------------------------------------------------------
    #
    def _get_mpi_info(self, exe):
        '''
        returns version and flavor of MPI version.
        '''

        if not exe:
            raise ValueError('no executable found')

        version = None
        flavor  = self.MPI_FLAVOR_UNKNOWN

        out, _, ret = ru.sh_callout('%s -V' % exe)

        if ret:
            out, _, ret = ru.sh_callout('%s --version' % exe)

        if ret:
            out, _, ret = ru.sh_callout('%s -info' % exe)

        if not ret:
            for line in out.splitlines():
                if 'intel(r) mpi library for linux' in line.lower():
                    # Intel MPI is hydra based
                    version = line.split(',')[1].strip()
                    flavor  = self.MPI_FLAVOR_HYDRA
                    break

                if 'hydra build details:' in line.lower():
                    version = line.split(':', 1)[1].strip()
                    flavor  = self.MPI_FLAVOR_HYDRA
                    break

                if 'mvapich2' in line.lower():
                    version = line
                    flavor  = self.MPI_FLAVOR_HYDRA
                    break

                if '(open mpi)' in line.lower():
                    version = line.split(')', 1)[1].strip()
                    flavor  = self.MPI_FLAVOR_OMPI
                    break

                if 'version:' in line.lower():
                    version = line.split(':', 1)[1].strip()
                    flavor  = self.MPI_FLAVOR_OMPI
                    break

        self._log.debug('mpi details [%s]: %s', exe, out)
        self._log.debug('mpi version: %s [%s]', version, flavor)

        if not flavor:
            raise RuntimeError('cannot identify MPI flavor [%s]' % exe)

        return version, flavor


# ------------------------------------------------------------------------------

