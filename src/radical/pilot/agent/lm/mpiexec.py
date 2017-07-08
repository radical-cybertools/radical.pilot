
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from .base import LaunchMethod


# ==============================================================================
#
class MPIExec(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # mpiexec (e.g. on SuperMUC)
        self.launch_command = self._find_executable([
            'mpiexec',            # General case
            'mpiexec-mpich-mp',   # Mac OSX MacPorts
            'mpiexec-openmpi-mp'  # Mac OSX MacPorts
        ])

    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        # Construct the executable and arguments
        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        import pprint
        self._log.debug('\nslots: %s', pprint.pformat(slots))
        self._log.debug('\ncud  : %s', pprint.pformat(cud))

        # extract a map of hosts and #slots from slots.  We count cpu and gpyu
        # slot sets, but do not account for threads,
        host_slots = dict()
        for node in slots['nodes']:
            #               NAME           #CORES         #GPUS
            host_slots[node[0]] = len(node[2]) + len(node[3])
        self._log.debug('\nhslot: %s', pprint.pformat(host_slots))

        # If we have a CU with many cores, and the compression didn't work
        # out, we will create a hostfile and pass that as an argument
        # instead of the individual hosts.  The hostfile has this format:
        #
        #   node_name_1 slots=n_1
        #   node_name_2 slots=n_2
        #   ...
        #
        # where the slot number is the number of processes we intent to spawn,
        # *including* GPU processes,
        #
        # For smaller node sets, we construct command line parameters as
        # clusters of nodes with the same number of processes, like this:
        #
        #   -H node_name_1,node_name_2 -np 8 : -H node_name_3 -np 4 : ...
        #
        # POSIX defines a min argument limit of 4096 bytes, so we try to
        # construct a command line, and switch to histfile if that limit is
        # exceeded.  We assume that Disk I/O for the hostfile is much larger
        # than the time needed to construct the command line.
        arg_max = 4096

        # This is the command w/o the host string
        command_stub = "%s %%s %s" % (self.launch_command, task_command)

        # cluster hosts by number of slots
        num_slots   = {}
        host_string = ''
        for node,nslots in host_slots.iteritems():
            host_string += '-H %s -np %s ' % (','.join([node] * nslots), nslots)
        command = command_stub % host_string

        if len(command) > arg_max:

            # Create a hostfile from the list of hosts.  We create that in the
            # unit sandbox
            fname = '%s/mpi_hostfile' % cu['unit_sandbox']
            with open(fname, 'w') as f:
                for node,nslots in host_slots.iteritems():
                    f.write('%20s \tslots=%s\n' % (node, nslots))
            host_string = "-hostfile %s" % fname

        command = command_stub % host_string
        assert(len(command) <= arg_max)

        self._log.debug('mpiexec cmd: %s', command)

        return command, None


# ------------------------------------------------------------------------------

