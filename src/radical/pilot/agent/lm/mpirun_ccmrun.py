
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class MPIRunCCMRun(LaunchMethod):
    # TODO: This needs both mpirun and ccmrun

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # ccmrun: Cluster Compatibility Mode job launcher for Cray systems
        self.launch_command = ru.which('ccmrun')
        self.mpirun_command = ru.which('mpirun')

        if not self.mpirun_command:
            raise RuntimeError("mpirun not found!")

        # alas, the way to transplant env variables to the target node differs
        # per mpi(run) version...
        version_info = sp.check_output(['%s -v' % self.mpirun_command], shell=True)
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
        task_env     = cud.get('environment', dict())
        task_args    = cud.get('arguments',   list())
        task_argstr  = self._create_arg_string(task_args)

        if 'task_slots' not in opaque_slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, opaque_slots))

        task_slots = opaque_slots['task_slots']

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        # Construct the hosts_string
        # TODO: is there any use in using $HOME/.crayccm/ccm_nodelist.$JOBID?
        hosts_string = ','.join([slot.split(':')[0] for slot in task_slots])


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


        mpirun_ccmrun_command = "%s %s -np %d -host %s %s %s" % (
            self.launch_command, self.mpirun_command,
            task_cores, hosts_string, env_string, task_command)

        return mpirun_ccmrun_command, None


# ------------------------------------------------------------------------------

