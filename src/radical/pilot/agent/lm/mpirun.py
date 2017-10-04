
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

from .base import LaunchMethod


# ==============================================================================
#
class MPIRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        self.launch_command = self._find_executable([
            'mpirun',            # General case
            'mpirun_rsh',        # Gordon @ SDSC
            'mpirun-mpich-mp',   # Mac OSX MacPorts
            'mpirun-openmpi-mp'  # Mac OSX MacPorts
        ])

        # alas, the way to transplant env variables to the target node differs
        # per mpi(run) version...
        version_info = sp.check_output(['%s -v' % self.launch_command], shell=True)
        if 'version:' in version_info:
            self.launch_version = version_info.split(':')[1].strip().lower()
        else:
            self.launch_version = 'unknown'



    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        opaque_slots = cu['opaque_slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_env     = cud.get('evironment', dict())
        task_args    = cud.get('arguments',  list())
        task_argstr  = self._create_arg_string(task_args)

        if not 'task_slots' in opaque_slots:
            raise RuntimeError('insufficient information to launch via %s: %s' \
                    % (self.name, opaque_slots))

        task_slots = opaque_slots['task_slots']

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        # Construct the hosts_string
        hosts_string = ",".join([slot.split(':')[0] for slot in task_slots])


        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
        if env_list:
            if 'mvapich2' in self.launch_version:
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


        mpirun_command = "%s -np %s -host %s %s %s" % (self.launch_command,
                        task_cores, hosts_string, env_string, task_command)

        return mpirun_command, None


# ------------------------------------------------------------------------------

