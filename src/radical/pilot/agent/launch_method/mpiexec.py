
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

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which([
            'mpiexec',            # General case
            'mpiexec.mpich',      # Linux, MPICH
            'mpiexec.hydra',      # Linux, MPICH
            'mpiexec.openempi',   # Linux, MPICH
            'mpiexec-mpich-mp',   # Mac OSX MacPorts
            'mpiexec-openmpi-mp'  # Mac OSX MacPorts
        ])

        self.mpi_version, self.mpi_flavor = self._get_mpi_info(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_env     = cud.get('environment') or dict()
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        # Construct the executable and arguments
        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + list(task_env.keys())
        if env_list:

            if self.mpi_flavor == self.MPI_FLAVOR_HYDRA:
                env_string = '-envlist "%s"' % ','.join(env_list)

            elif self.mpi_flavor == self.MPI_FLAVOR_OMPI:
                for var in env_list:
                    env_string += '-x "%s" ' % var

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

        # If we have a CU with many cores, and the compression didn't work
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
        command_stub = "%s %%s %s %s" % (self.launch_command,
                                         env_string, task_command)

        # cluster hosts by number of slots
        host_string = ''
        for node,nslots in list(host_slots.items()):
            host_string += '-host %s -n %s ' % (','.join([node] * nslots), nslots)
        command = command_stub % host_string

        if len(command) > arg_max:

            # Create a hostfile from the list of hosts.  We create that in the
            # unit sandbox
            fname = '%s/mpi_hostfile' % cu['unit_sandbox_path']
            with open(fname, 'w') as f:
                for node,nslots in list(host_slots.items()):
                    f.write('%20s \tslots=%s\n' % (node, nslots))
            host_string = "-hostfile %s" % fname

        command = command_stub % host_string
        self._log.debug('mpiexec cmd: %s', command)

        assert(len(command) <= arg_max)

        return command, None


# ------------------------------------------------------------------------------

