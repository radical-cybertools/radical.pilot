
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIExec(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._mpt    : bool  = False
        self._rsh    : bool  = False
        self._ccmrun : str   = ''
        self._dplace : str   = ''
        self._omplace: str   = ''
        self._command: str   = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        lm_info = {
            'env'    : env,
            'env_sh' : env_sh,
            'command': ru.which([
                'mpiexec',             # General case
                'mpiexec.mpich',       # Linux, MPICH
                'mpiexec.hydra',       # Linux, MPICH
                'mpiexec.openmpi',     # Linux, MPICH
                'mpiexec-mpich-mp',    # Mac OSX MacPorts
                'mpiexec-openmpi-mp',  # Mac OSX MacPorts
                'mpiexec_mpt',         # Cheyenne (NCAR)
            ]),
            'mpt'    : False,
            'rsh'    : False,
            'ccmrun' : '',
            'dplace' : '',
            'omplace': ''
        }

        if '_mpt' in self.name.lower():
            lm_info['mpt'] = True

        if '_rsh' in self.name.lower():
            lm_info['rsh'] = True

        # do we need ccmrun or dplace?
        if '_ccmrun' in self.name.lower():
            lm_info['ccmrun'] = ru.which('ccmrun')
            assert lm_info['ccmrun']

        if '_dplace' in self.name.lower():
            lm_info['dplace'] = ru.which('dplace')
            assert lm_info['dplace']

        # cheyenne always needs mpt and omplace
        if 'cheyenne' in ru.get_hostname():
            lm_info['omplace'] = 'omplace'
            lm_info['mpt']     = True

        mpi_version, mpi_flavor = self._get_mpi_info(lm_info['command'])
        lm_info['mpi_version']  = mpi_version
        lm_info['mpi_flavor']   = mpi_flavor

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env         = lm_info['env']
        self._env_sh      = lm_info['env_sh']
        self._command     = lm_info['command']

        assert self._command

        self._mpt         = lm_info['mpt']
        self._rsh         = lm_info['rsh']
        self._dplace      = lm_info['dplace']
        self._ccmrun      = lm_info['ccmrun']

        self._mpi_version = lm_info['mpi_version']
        self._mpi_flavor  = lm_info['mpi_flavor']

        # ensure empty string on unset omplace
        if not lm_info['omplace']:
            self._omplace = ''
        else:
            self._omplace = 'omplace'


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if not task['description']['executable']:
            return False, 'no executable'

        return True, ''


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        slots = task['slots']

        if 'ranks' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        # extract a map of hosts and #slots from slots.  We count cpu
        # slot sets, but do not account for threads.  Since multiple slots
        # entries can have the same node names, we *add* new information.
        host_slots = dict()
        for rank in slots['ranks']:
            node_name = rank['node_name']
            if node_name not in host_slots:
                host_slots[node_name] = 0
            host_slots[node_name] += len(rank['core_map'])

        # If we have a Task with many cores, and the compression didn't work
        # out, we will create a hostfile and pass that as an argument
        # instead of the individual hosts.  The hostfile has this format:
        #
        #   node_name_1 slots=n_1
        #   node_name_2 slots=n_2
        #   ...
        #
        # where the slot number is the number of processes we intent to spawn.
        #
        # For smaller node sets, we construct command line parameters as
        # clusters of nodes with the same number of processes, like this:
        #
        #   -host node_name_1,node_name_2 -n 8 : -host node_name_3 -n 4 : ...
        #
        # POSIX defines a min argument limit of 4096 bytes, so we try to
        # construct a command line, and switch to hostfile if that limit is
        # exceeded.  We assume that Disk I/O for the hostfile is much larger
        # than the time needed to construct the command line.
        arg_max = 4096

        # This is the command w/o the host string
        cmd_stub = '%s %%s %s %s' % (self._command, self._omplace, exec_path)

        # cluster hosts by number of slots
        host_string = ''
        if not self._mpt:
            for node, nslots in list(host_slots.items()):
                host_string += '-host '
                host_string += '%s -n %s ' % (','.join([node] * nslots), nslots)
        else:
            hosts = list()
            for node, nslots in list(host_slots.items()):
                hosts += [node] * nslots
            host_string += ','.join(hosts)
            host_string += ' -n 1'

        cmd = cmd_stub % host_string

        if len(cmd) > arg_max:

            # Create a hostfile from the list of hosts.  We create that in the
            # task sandbox
            hostfile = '%s/mpi_hostfile' % task['task_sandbox_path']
            with ru.ru_open(hostfile, 'w') as f:
                for node, nslots in list(host_slots.items()):
                    f.write('%20s \tslots=%s\n' % (node, nslots))
            host_string = '-hostfile %s' % hostfile

        cmd = cmd_stub % host_string
        self._log.debug('mpiexec cmd: %s', cmd)

        assert(len(cmd) <= arg_max)

        # Cheyenne is the only machine that requires mpiexec_mpt.
        # We then have to set MPI_SHEPHERD=true
        if self._mpt:
            cmd = 'export MPI_SHEPHERD=true\n%s' % cmd

        return cmd.strip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: we know the MPI flavor, so make this less guesswork

        ret  = 'test -z "$MPI_RANK"  || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK" || export RP_RANK=$PMIX_RANK\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td['arguments']
        task_argstr  = self._create_arg_string(task_args)
        command      = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

