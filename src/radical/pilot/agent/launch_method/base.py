
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import fractions
import collections

import radical.utils as ru
from functools import reduce


# 'enum' for launch method types
LM_NAME_APRUN         = 'APRUN'
LM_NAME_CCMRUN        = 'CCMRUN'
LM_NAME_FORK          = 'FORK'
LM_NAME_IBRUN         = 'IBRUN'
LM_NAME_MPIEXEC       = 'MPIEXEC'
LM_NAME_MPIRUN        = 'MPIRUN'
LM_NAME_MPIRUN_MPT    = 'MPIRUN_MPT'
LM_NAME_MPIRUN_CCMRUN = 'MPIRUN_CCMRUN'
LM_NAME_MPIRUN_DPLACE = 'MPIRUN_DPLACE'
LM_NAME_MPIRUN_RSH    = 'MPIRUN_RSH'
LM_NAME_JSRUN         = 'JSRUN'
LM_NAME_PRTE          = 'PRTE'
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
    #        cud.pre_exec, and then should also export those.  That would make
    #        our launch script ore complicated though...
    EXPORT_ENV_VARIABLES = [
        'LD_LIBRARY_PATH',
        'PATH',
        'PYTHONPATH',
        'OMP_NUM_THREADS',
        'CUDA_VISIBLE_DEVICES',
        'RP_AGENT_ID',
        'RP_GTOD',
        'RP_PILOT_ID',
        'RP_PILOT_STAGING',
        'RP_PROF',
        'RP_SESSION_ID',
        'RP_SPAWNER_ID',
        'RP_TMP',
        'RP_UNIT_ID',
        'RP_UNIT_NAME',
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

        # A per-launch_method list of env vars to remove from the CU env
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
        from .flux           import Flux
        from .jsrun          import JSRUN
        from .yarn           import Yarn
        from .spark          import Spark

        # # deprecated
        # from .orte           import ORTE

        impl = {
            LM_NAME_FORK          : Fork,
            LM_NAME_PRTE          : PRTE,
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
        from .flux           import Flux
        from .yarn           import Yarn
        from .spark          import Spark

        # # deprecated
        # from .orte           import ORTE

        impl = {
            LM_NAME_PRTE          : PRTE,
            LM_NAME_FLUX          : Flux,
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
    def construct_command(self, cu, launch_script_hop):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _create_hostfile(cls, sandbox, uid, all_hosts, separator=' ',
                         impaired=False):

        # Open appropriately named temporary file
        # NOTE: we make an assumption about the unit sandbox here
        filename = '%s/%s.hosts' % (sandbox, uid)
        with open(filename, 'w') as fout:

            if not impaired:
                # Write "hostN x\nhostM y\n" entries
                # Create a {'host1': x, 'host2': y} dict
                counter = collections.Counter(all_hosts)

                # Convert it into an ordered dict,
                # which hopefully resembles the original ordering
                count_dict = collections.OrderedDict(sorted(counter.items(),
                                                     key=lambda t: t[0]))

                for (host, count) in count_dict.items():
                    fout.write('%s%s%d\n' % (host, separator, count))

            else:
                # Write "hostN\nhostM\n" entries
                for host in all_hosts:
                    fout.write('%s\n' % host)

        # Return the filename, caller is responsible for cleaning up
        return filename


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _compress_hostlist(cls, all_hosts):

        # Return gcd of a list of numbers
        def gcd_list(l):
            return reduce(fractions.gcd, l)

        # Create a {'host1': x, 'host2': y} dict
        count_dict = dict(collections.Counter(all_hosts))
        # Find the gcd of the host counts
        host_gcd = gcd_list(set(count_dict.values()))

        # Divide the host counts by the gcd
        for host in count_dict:
            count_dict[host] /= host_gcd

        # Recreate a list of hosts based on the normalized dict
        hosts = list()
        for (host, count) in list(count_dict.items()):
            hosts.extend([host] * count)

        # sort the list for readbility
        hosts.sort()

        return hosts


    # --------------------------------------------------------------------------
    #
    def _create_arg_string(self, args):

        # unit Arguments (if any)
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
                if 'hydra build details:' in line.lower():
                    version = line.split(':', 1)[1].strip()
                    flavor  = self.MPI_FLAVOR_HYDRA
                    break

                if 'mvapich2' in line.lower():
                    version = line
                    flavor  = self.MPI_FLAVOR_HYDRA
                    break

                if 'version:' in line.lower():
                    version = line.split(':', 1)[1].strip()
                    flavor  = self.MPI_FLAVOR_OMPI
                    break

                if '(open mpi)' in line.lower():
                    version = line.split(')', 1)[1].strip()
                    flavor  = self.MPI_FLAVOR_OMPI
                    break

        if not flavor:
            raise RuntimeError('cannot identify MPI flavor [%s]' % exe)

        self._log.debug('mpi version: %s [%s]', version, flavor)

        return version, flavor


# ------------------------------------------------------------------------------

