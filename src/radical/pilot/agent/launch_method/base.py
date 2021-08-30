# pylint: disable=protected-access

__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

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
LM_NAME_PRTE2         = 'PRTE2'
LM_NAME_FLUX          = 'FLUX'
LM_NAME_ORTE          = 'ORTE'
LM_NAME_ORTE_LIB      = 'ORTE_LIB'
LM_NAME_RSH           = 'RSH'
LM_NAME_SSH           = 'SSH'
LM_NAME_YARN          = 'YARN'
LM_NAME_SPARK         = 'SPARK'
LM_NAME_SRUN          = 'SRUN'

# deprecated
# LM_NAME_POE           = 'POE'
# LM_NAME_DPLACE        = 'DPLACE'
# LM_NAME_RUNJOB        = 'RUNJOB'

# deprecated
# LM_NAME_POE           = 'POE'
# LM_NAME_DPLACE        = 'DPLACE'
# LM_NAME_RUNJOB        = 'RUNJOB'

PWD = os.getcwd()


# ------------------------------------------------------------------------------
#
class LaunchMethod(object):

    # List of environment variables that designated Launch Methods should export
    # FIXME: we should find out what env vars are changed or added by
    #        td.pre_exec, and then should also export those.  That would make
    #        our launch script ore complicated though...
    EXPORT_ENV_VARIABLES = [
        'LD_LIBRARY_PATH',
        'PATH',
        'PYTHONPATH',
        'OMP_NUM_THREADS',
        'RP_AGENT_ID',
        'RP_GTOD',
        'RP_PILOT_ID',
        'RP_PILOT_STAGING',
        'RP_PROF',
        'RP_SESSION_ID',
        'RP_SPAWNER_ID',
        'RP_TMP',
        'RP_TASK_ID',
        'RP_TASK_NAME',
        'RP_PILOT_SANDBOX',
        'RADICAL_BASE'
    ]

    MPI_FLAVOR_OMPI    = 'OMPI'
    MPI_FLAVOR_HYDRA   = 'HYDRA'
    MPI_FLAVOR_UNKNOWN = 'unknown'

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        self.name     = name
        self._cfg     = cfg
        self._session = session
        self._log     = self._session._log    # pylint: disable=protected-access
        self._log.debug('create LaunchMethod: %s', type(self))

        # A per-launch_method list of env vars to remove from the Task env
        self.env_removables = []

        self._configure()


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, cfg, session):

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
        from .prte2          import PRTE2
        from .flux           import Flux
        from .rsh            import RSH
        from .ssh            import SSH
        from .yarn           import Yarn
        from .spark          import Spark
        from .srun           import Srun

      # # deprecated
      # from .orte           import ORTE
      # from .orte_lib       import ORTELib
      # from .mpirun_ccmrun  import MPIRunCCMRun
      # from .mpirun_dplace  import MPIRunDPlace
      # from .mpirun_mpt     import MPIRun_MPT
      # from .mpirun_rsh     import MPIRunRSH
      # from .dplace         import DPlace
      # from .poe            import POE
      # from .runjob         import Runjob
      # from .mpirun_ccmrun  import MPIRunCCMRun
      # from .mpirun_dplace  import MPIRunDPlace
      # from .mpirun_mpt     import MPIRun_MPT
      # from .mpirun_rsh     import MPIRunRSH
      # from .dplace         import DPlace
      # from .poe            import POE
      # from .runjob         import Runjob

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
                LM_NAME_PRTE2         : PRTE2,
                LM_NAME_FLUX          : Flux,
                LM_NAME_RSH           : RSH,
                LM_NAME_SSH           : SSH,
                LM_NAME_YARN          : Yarn,
                LM_NAME_SPARK         : Spark,
                LM_NAME_SRUN          : Srun,

              # # deprecated
              # LM_NAME_ORTE          : ORTE,
              # LM_NAME_ORTE_LIB      : ORTELib,
              # LM_NAME_DPLACE        : DPlace,
              # LM_NAME_POE           : POE,
              # LM_NAME_RUNJOB        : Runjob,
            }[name]
            return impl(name, cfg, session)

        except KeyError:
            session._log.exception('invalid lm %s' % name)
            return None

        except Exception:
            session._log.exception('unusable lm %s' % name)
            return None


    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, profiler):
        '''
        This hook will allow the ResourceManager to perform launch methods
        specific configuration steps.  The ResourceManager layer MUST ensure
        that this hook is called exactly once (globally).  This will be a NOOP
        for LaunchMethods which do not overload this method.  Exceptions fall
        through to the ResourceManager.
        '''

        # Make sure that we are the base-class!
        if cls != LaunchMethod:
            raise TypeError("LaunchMethod config hook only available to base class!")

        from .fork           import Fork
        from .prte           import PRTE
        from .prte2          import PRTE2
        from .flux           import Flux
        from .jsrun          import JSRUN
        from .yarn           import Yarn
        from .spark          import Spark

        # # deprecated
        # from .orte           import ORTE

        impl = {
            LM_NAME_FORK          : Fork,
            LM_NAME_PRTE          : PRTE,
            LM_NAME_PRTE2         : PRTE2,
            LM_NAME_FLUX          : Flux,
            LM_NAME_JSRUN         : JSRUN,
            LM_NAME_YARN          : Yarn,
            LM_NAME_SPARK         : Spark,

            # # deprecated
            # LM_NAME_ORTE          : ORTE,

        }.get(name)

        if not impl:
            log.info('no config hook defined for LaunchMethod %s' % name)
            return None

        log.info('ResourceManager config hook for LaunchMethod %s: %s' % (name, impl))
        return impl.rm_config_hook(name, cfg, rm, log, profiler)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_shutdown_hook(cls, name, cfg, rm, lm_info, log, profiler):
        '''
        This hook is symmetric to the config hook above, and is called during
        shutdown sequence, for the sake of freeing allocated resources.
        '''

        # Make sure that we are the base-class!
        if cls != LaunchMethod:
            raise TypeError("LaunchMethod shutdown hook only available to base class!")

        from .prte           import PRTE
        from .prte2          import PRTE2
      # from .flux           import Flux
        from .yarn           import Yarn
        from .spark          import Spark

        # # deprecated
        # from .orte           import ORTE

        impl = {
            LM_NAME_PRTE          : PRTE,
            LM_NAME_PRTE2         : PRTE2,
          # LM_NAME_FLUX          : Flux,
            LM_NAME_YARN          : Yarn,
            LM_NAME_SPARK         : Spark

            # # deprecated
            # LM_NAME_ORTE          : ORTE,
        }.get(name)

        if not impl:
            log.info('no shutdown hook defined for LaunchMethod %s' % name)
            return None

        log.info('ResourceManager shutdown hook for LaunchMethod %s: %s' % (name, impl))
        return impl.rm_shutdown_hook(name, cfg, rm, lm_info, log, profiler)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def _create_arg_string(self, args):

        # task Arguments (if any)
        arg_string = ''
        if args:
            for arg in args:
                if not arg:
                    # ignore empty args
                    continue

                if arg in ['>', '>>', '<', '<<', '|', '||', '&&', '&']:
                    # Don't quote shell direction arguments, etc.
                    arg_string += '%s ' % arg
                    continue

                if any([c in arg for c in ['?', '*']]):
                    # Don't quote arguments with wildcards
                    arg_string += '%s ' % arg
                    continue

                arg = arg.replace('"', '\\"')    # Escape all double quotes
                if arg[0] == arg[-1] == "'" :    # between outer single quotes?
                    arg_string += '%s ' % arg    # ... pass it as is.

                else:
                    arg_string += '"%s" ' % arg  # else return double quoted

        return arg_string


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

        out, _, ret = ru.sh_callout('%s -v' % exe)

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

