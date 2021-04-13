# pylint: disable=protected-access

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

    MPI_FLAVOR_OMPI    = 'OMPI'
    MPI_FLAVOR_HYDRA   = 'HYDRA'
    MPI_FLAVOR_UNKNOWN = 'unknown'


    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, cfg, log, prof):

        log.debug('===== lm base init start')
        self.name    = name
        self._lm_cfg = cfg
        self._cfg    = cfg
        self._log    = log
        self._prof   = prof
        self._pwd    = os.getcwd()

        self._reg  = ru.zmq.RegistryClient(url=self._cfg.reg_addr)
        lm_info = self._reg.get('lm.%s' % self.name)

        lm_info = None  # FIXME
        if lm_info:
            self._log.debug('=== init from info: %s', lm_info)
            raise NotImplementedError('lm.init_from_info')
          # self._init_from_info(lm_cfg, lm_info)
        else:
            self._log.debug('=== init from scratch')
            lm_info = self._init_from_scratch(lm_cfg)
            self._reg.put('lm.%s' % self.name, lm_info)

        self._info = self._cfg.get('lm_info', {}).get(self.name)
        log.debug('===== lm base init stop: %s', self._info)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Launch Method.
    #
    @classmethod
    def create(cls, name, lm_cfg, cfg, log, prof):

        log.debug('===== lm create %s start', name)
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

            }[name]
            return impl(name, lm_cfg, cfg, log, prof)

        except KeyError:
            log.exception('invalid lm %s' % name)
            raise

        except Exception:
            log.exception('unusable lm %s' % name)
            raise


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, lm_cfg):

        self._log.debug('==== empty init from scratch for %s', self.name)

      # raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        raise NotImplementedError("incomplete LaunchMethod %s" % self.name)


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
    def get_launch_cmd(self):

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
    @classmethod
    def _create_hostfile(cls, sandbox, uid, all_hosts, separator=' ',
                         impaired=False):

        # Open appropriately named temporary file
        # NOTE: we make an assumption about the task sandbox here
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

        if not args:
            return ''

        arg_string = ''
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

