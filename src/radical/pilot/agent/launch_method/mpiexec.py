
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIExec(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, cfg, log, prof):

        self._mpt      = False
        self._rsh      = False
        self._ccmrun   = ''
        self._omplace  = ''
        self._dplace   = ''
        self._info     = None

        self._env_orig = ru.env_eval('env/bs0_orig.env')

        log.debug('===== lm MPIEXEC init start')

        LaunchMethod.__init__(self, name, lm_cfg, cfg, log, prof)

        self._log.debug('===== lm MPIEXEC init stop')



    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, lm_cfg):

        # FIXME: this should happen in the lm_env

        lm_info = dict()
        lm_info['launch_command'] = ru.which([
            'mpiexec',             # General case
            'mpiexec.mpich',       # Linux, MPICH
            'mpiexec.hydra',       # Linux, MPICH
            'mpiexec.openmpi',     # Linux, MPICH
            'mpiexec-mpich-mp',    # Mac OSX MacPorts
            'mpiexec-openmpi-mp',  # Mac OSX MacPorts
            'mpiexec_mpt',         # Cheyenne (NCAR)
        ])

        if '_mpt' in self.name.lower():
            lm_info['mpt'] = True
        else:
            lm_info['mpt'] = False

        # cheyenne also uses omplace
        # FIXME check hostname
        if self._mpt and 'cheyenne' in ru.get_hostname():
            lm_info['omplace'] = True
        else:
            lm_info['omplace'] = False

        mpi_version, mpi_flavor = self._get_mpi_info(lm_info['launch_command'])
        lm_info['mpi_version']  = mpi_version
        lm_info['mpi_flavor']   = mpi_flavor

        self._init_from_info(lm_info, lm_cfg)

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info, lm_cfg):

        self.launch_command = lm_info['launch_command']
        self.mpt            = lm_info['mpt']
        self.omplace        = lm_info['omplace']
        self.mpi_version    = lm_info['mpi_version']
        self.mpi_flavor     = lm_info['mpi_flavor']


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return "echo $PMIX_RANK"


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        slots        = t['slots']
        td           = t['description']
        task_exec    = td['executable']
        task_env     = td.get('environment') or dict()
        task_args    = td.get('arguments')   or list()
        task_args    = td.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        # Construct the executable and arguments
        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        # Cheyenne is the only machine that requires mpirun_mpt.  We then
        # have to set MPI_SHEPHERD=true
        if self._mpt:
            if not td.get('environment'):
                td['environment'] = dict()
            td['environment']['MPI_SHEPHERD'] = 'true'


        if 'ranks' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        # extract a map of hosts and #slots from slots.  We count cpu
        # slot sets, but do not account for threads.  Since multiple slots
        # entries can have the same node names, we *add* new information.
        host_slots = dict()
        for rank in slots['ranks']:
            node_name = rank['node']
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
        omplace = ''
        if self._omplace:
            omplace = 'omplace'
        command_stub = "%s %%s %s %s" % (self.launch_command,
                                         omplace, task_command)

        # cluster hosts by number of slots
        host_string = ''
        if not self._mpt:
            for node,nslots in list(host_slots.items()):
                host_string += '-host '
                host_string += '%s -n %s ' % (','.join([node] * nslots), nslots)
        else:
            hosts = list()
            for node,nslots in list(host_slots.items()):
                hosts += [node] * nslots
            host_string += ','.join(hosts)
            host_string += ' -n 1'

        command = command_stub % host_string

        if len(command) > arg_max:

            # Create a hostfile from the list of hosts.  We create that in the
            # task sandbox
            hostfile = '%s/mpi_hostfile' % t['task_sandbox_path']
            with open(hostfile, 'w') as f:
                for node,nslots in list(host_slots.items()):
                    f.write('%20s \tslots=%s\n' % (node, nslots))
            host_string = "-hostfile %s" % hostfile

        command = command_stub % host_string
        self._log.debug('mpiexec cmd: %s', command)

        assert(len(command) <= arg_max)

        return command, None


# ------------------------------------------------------------------------------

