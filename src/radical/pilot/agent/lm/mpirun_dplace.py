
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class MPIRunDPlace(LaunchMethod):
    # TODO: This needs both mpirun and dplace

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # dplace: job launcher for SGI systems (e.g. on Blacklight)
        self.launch_command = ru.which('dplace')
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

        if not 'task_offsets' in opaque_slots:
            raise RuntimeError('insufficient information to launch via %s: %s' \
                    % (self.name, opaque_slots))

        task_offsets = opaque_slots['task_offsets']

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        dplace_offset = task_offsets


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


        mpirun_dplace_command = "%s -np %d %s -c %d-%d %s %s" % (self.mpirun_command,
                task_cores, self.launch_command, dplace_offset,
                dplace_offset+task_cores-1, env_string, task_command)

        return mpirun_dplace_command, None


# ------------------------------------------------------------------------------

