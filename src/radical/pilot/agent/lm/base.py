
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import fractions
import collections

import radical.utils as ru


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


# ==============================================================================
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
        self._log     = self._session._log
        self._log.debug('create LM: %s', type(self))

        # A per-launch_method list of env vars to remove from the CU env
        self.env_removables = []

        self._configure()

        # TODO: This doesn't make too much sense for LM's that use multiple
        #       commands, perhaps this needs to move to per LM __init__.
        if self.launch_command is None:
            raise RuntimeError("Launcher not found for LaunchMethod '%s'"
                              % self.name)

        self._log.debug('launch_command: %s', self.launch_command)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, cfg, session):

        # Make sure that we are the base-class!
        if cls != LaunchMethod:
            raise TypeError("LM create() only available to base class!")

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
        from .orte           import ORTE
        from .orte_lib       import ORTELib
        from .rsh            import RSH
        from .ssh            import SSH
        from .yarn           import Yarn
        from .spark          import Spark
        from .srun           import Srun

      # # deprecated
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
                LM_NAME_ORTE          : ORTE,
                LM_NAME_ORTE_LIB      : ORTELib,
                LM_NAME_RSH           : RSH,
                LM_NAME_SSH           : SSH,
                LM_NAME_YARN          : Yarn,
                LM_NAME_SPARK         : Spark,
                LM_NAME_SRUN          : Srun,

              # # deprecated
              # LM_NAME_DPLACE        : DPlace,
              # LM_NAME_POE           : POE,
              # LM_NAME_RUNJOB        : Runjob,
            }[name]
            return impl(name, cfg, session)

        except KeyError:
            session._log.exception("LM '%s' unknown or defunct" % name)

        except Exception as e:
            session._log.exception("LM cannot be used: %s!" % e)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger, profiler):
        """
        This hook will allow the LRMS to perform launch methods specific
        configuration steps.  The LRMS layer MUST ensure that this hook is
        called exactly once (globally).  This will be a NOOP for LMs which do
        not overload this method.  Exceptions fall through to the LRMS.
        """

        # Make sure that we are the base-class!
        if cls != LaunchMethod:
            raise TypeError("LM config hook only available to base class!")

        from .fork           import Fork
        from .prte           import PRTE
        from .orte           import ORTE
        from .yarn           import Yarn
        from .spark          import Spark

        impl = {
            LM_NAME_FORK          : Fork,
            LM_NAME_PRTE          : PRTE,
            LM_NAME_ORTE          : ORTE,
            LM_NAME_YARN          : Yarn,
            LM_NAME_SPARK         : Spark
        }.get(name)

        if not impl:
            logger.info('no config hook defined for LaunchMethod %s' % name)
            return None

        logger.info('LRMS config hook for LaunchMethod %s: %s' % (name, impl))
        return impl.lrms_config_hook(name, cfg, lrms, logger, profiler)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger, profiler):
        """
        This hook is symmetric to the config hook above, and is called during
        shutdown sequence, for the sake of freeing allocated resources.
        """

        # Make sure that we are the base-class!
        if cls != LaunchMethod:
            raise TypeError("LM shutdown hook only available to base class!")

        from .prte           import PRTE
        from .orte           import ORTE
        from .yarn           import Yarn
        from .spark          import Spark

        impl = {
            LM_NAME_PRTE          : PRTE,
            LM_NAME_ORTE          : ORTE,
            LM_NAME_YARN          : Yarn,
            LM_NAME_SPARK         : Spark
        }.get(name)

        if not impl:
            logger.info('no shutdown hook defined for LaunchMethod %s' % name)
            return None

        logger.info('LRMS shutdown hook for LM %s: %s' % (name, impl))
        return impl.lrms_shutdown_hook(name, cfg, lrms, lm_info,
                                       logger, profiler)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        raise NotImplementedError("incomplete LM %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        raise NotImplementedError("incomplete LM %s" % self.name)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def _create_hostfile(cls, uid, all_hosts, separator=' ', impaired=False):

        # Open appropriately named temporary file
        # NOTE: we make an assumption about the unit sandbox here
        filename = '%s/%s.hosts' % (uid, uid)
        handle   = open(filename, 'w')

        if not impaired:
            # Write "hostN x\nhostM y\n" entries
            # Create a {'host1': x, 'host2': y} dict
            counter = collections.Counter(all_hosts)

            # Convert it into an ordered dict,
            # which hopefully resembles the original ordering
            count_dict = collections.OrderedDict(sorted(counter.items(),
                                                 key=lambda t: t[0]))

            for (host, count) in count_dict.iteritems():
                os.write(handle, '%s%s%d\n' % (host, separator, count))

        else:
            # Write "hostN\nhostM\n" entries
            for host in all_hosts:
                os.write(handle, '%s\n' % host)

        os.close(handle)

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
        for (host, count) in count_dict.iteritems():
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

                if '(open mpi):' in line.lower():
                    version = line.split(')', 1)[1].strip()
                    flavor  = self.MPI_FLAVOR_OMPI
                    break

        if not flavor:
            raise RuntimeError('cannot identify MPI flavor [%s]' % exe)

        self._log.debug('mpi version: %s [%s]', version, flavor)

        return version, flavor


# ------------------------------------------------------------------------------

