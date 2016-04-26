
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from .base import LaunchMethod


# ==============================================================================
#
class MPIExec(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LaunchMethod.__init__(self, cfg, logger)


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

        opaque_slots = cu['opaque_slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        if not 'task_slots' in opaque_slots:
            raise RuntimeError('insufficient information to launch via %s: %s' \
                    % (self.name, opaque_slots))

        task_slots = opaque_slots['task_slots']

        # Extract all the hosts from the slots
        all_hosts = [slot.split(':')[0] for slot in task_slots]

        # Shorten the host list as much as possible
        hosts = self._compress_hostlist(all_hosts)

        # If we have a CU with many cores, and the compression didn't work
        # out, we will create a hostfile and pass  that as an argument
        # instead of the individual hosts
        if len(hosts) > 42:

            # Create a hostfile from the list of hosts
            hostfile = self._create_hostfile(all_hosts, separator=':')
            hosts_string = "-hostfile %s" % hostfile

        else:

            # Construct the hosts_string ('h1 h2 .. hN')
            hosts_string = "-host "+ ",".join(hosts)

        # Construct the executable and arguments
        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        mpiexec_command = "%s -n %s %s %s" % (
            self.launch_command, task_cores, hosts_string, task_command)

        return mpiexec_command, None


# ------------------------------------------------------------------------------

