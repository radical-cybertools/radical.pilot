
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

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

        # alas, the way to transplant env variables to the target node differs
        # per mpi(run) version...
        out, err, ret = ru.sh_callout('%s -v' % self.launch_command)

        if ret != 0:
            out, err, ret = ru.sh_callout('%s -info' % self.launch_command)

        self.launch_version = ''
        for line in out.splitlines():
            if 'HYDRA build details:' in line:
                self.launch_version += 'hydra-'
            if 'version:' in line.lower():
                self.launch_version += line.split(':')[1].strip().lower()
                break

        if not self.launch_version:
            self.launch_version = 'unknown'


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        opaque_slots = cu['opaque_slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_env     = cud.get('environment', dict())
        task_args    = cud.get('arguments',   list())
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


        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
        if env_list:
            if 'hydra' in self.launch_version:
                env_string = '-envlist "%s"' % ','.join(env_list)

            elif 'openmpi' in self.launch_version:
                env_string = ''
                for var in env_list:
                    env_string += '-x "%s" ' % var

            else:
                # this is a crude version of env transplanting where we prep the
                # shell command line.  We likely won't survive any complicated vars
                # (multiline, quotes, etc)
                env_string = ' '
                for var in env_string:
                    env_string += '%s="$%s" ' % (var, var)


        command = "%s -n %s %s %s %s" % (self.launch_command,
                  task_cores, hosts_string, env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

