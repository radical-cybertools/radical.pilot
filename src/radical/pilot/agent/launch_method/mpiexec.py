
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIExec(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        self._mpt     = False
        self._omplace = False

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        if '_mpt' in self.name.lower():
            self._mpt = True
            self.launch_command = ru.which([
                'mpiexec_mpt',        # Cheyenne (NCAR)
            ])
        else:
            self.launch_command = ru.which([
                'mpiexec',            # General case
                'mpiexec.mpich',      # Linux, MPICH
                'mpiexec.hydra',      # Linux, MPICH
                'mpiexec.openmpi',    # Linux, MPICH
                'mpiexec-mpich-mp',   # Mac OSX MacPorts
                'mpiexec-openmpi-mp'  # Mac OSX MacPorts
            ])

        # cheyenne also uses omplace
        # FIXME check hostname
        if self._mpt:
            self._omplace = True

        self.mpi_version, self.mpi_flavor = self._get_mpi_info(self.launch_command)


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
            task_env = td['environment']

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + list(task_env.keys())
        if env_list:

            if self.mpi_flavor == self.MPI_FLAVOR_HYDRA:
                env_string = '-envlist "%s"' % ','.join(env_list)

            elif self.mpi_flavor == self.MPI_FLAVOR_OMPI:
                for var in env_list:
                    env_string += '-x "%s" ' % var
                env_string = env_string.strip()

        if 'nodes' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        # extract a map of hosts and #slots from slots.  We count cpu
        # slot sets, but do not account for threads.  Since multiple slots
        # entries can have the same node names, we *add* new information.
        host_slots = dict()
        for node in slots['nodes']:
            node_name = node['name']
            if node_name not in host_slots:
                host_slots[node_name] = 0
            host_slots[node_name] += len(node['core_map'])

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
        command_stub = "%s %%s %s %s %s" % (self.launch_command,
                                            env_string, omplace, task_command)

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

