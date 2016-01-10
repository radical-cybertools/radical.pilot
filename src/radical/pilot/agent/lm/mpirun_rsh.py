
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

from .base import LaunchMethod


# ==============================================================================
#
class MPIRunRSH(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LaunchMethod.__init__(self, cfg, logger)

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # mpirun_rsh (e.g. on Gordon@SDSC, Stampede@TACC)
        if not self._which('mpirun_rsh'):
            raise Exception("mpirun_rsh could not be found")

        # We don't use the full pathname as the user might load a different
        # compiler / MPI library suite from his CU pre_exec that requires
        # the launcher from that version, as experienced on stampede in #572.
        self.launch_command = 'mpirun_rsh'

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

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        # Extract all the hosts from the slots
        hosts = [slot.split(':')[0] for slot in task_slots]

        # If we have a CU with many cores, we will create a hostfile and pass
        # that as an argument instead of the individual hosts
        if len(hosts) > 42:

            # Create a hostfile from the list of hosts
            hostfile = self._create_hostfile(hosts, impaired=True)
            hosts_string = "-hostfile %s" % hostfile

        else:

            # Construct the hosts_string ('h1 h2 .. hN')
            hosts_string = " ".join(hosts)

        export_vars = ' '.join([var+"=$"+var for var in self.EXPORT_ENV_VARIABLES if var in os.environ])

        mpirun_rsh_command = "%s -np %d %s %s %s" % (
            self.launch_command, task_cores, hosts_string, export_vars, task_command)

        return mpirun_rsh_command, None


# ------------------------------------------------------------------------------

