
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import subprocess    as sp
import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class MPIRunRSH(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # mpirun_rsh (e.g. on Gordon@SDSC, Stampede@TACC)
        if not ru.which('mpirun_rsh'):
            raise Exception("mpirun_rsh could not be found")

        # We don't use the full pathname as the user might load a different
        # compiler / MPI library suite from his Task pre_exec that requires
        # the launcher from that version, as experienced on stampede in #572.
        self.launch_command = 'mpirun_rsh'

        # alas, the way to transplant env variables to the target node differs
        # per mpi(run) version...
        version_info = sp.check_output(['%s -v' % self.launch_command], shell=True)
        if 'version:' in version_info:
            self.launch_version = version_info.split(':')[1].strip().lower()
        else:
            self.launch_version = 'unknown'


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        opaque_slots = t['slots']
        td          = t['description']
        task_exec    = td['executable']
        task_cores   = td['cores']
        task_env     = td.get('environment') or dict()
        task_args    = td.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)

        if 'task_slots' not in opaque_slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                    % (self.name, opaque_slots))

        task_slots = opaque_slots['task_slots']

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec


        # Extract all the hosts from the slots
        hosts = [slot.split(':')[0] for slot in task_slots]
        hosts_string = ','.join(hosts)


        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + list(task_env.keys())
        if env_list:
            if 'mvapich2' in self.launch_version:
                env_string = '-envlist "%s"' % ','.join(env_list)

            elif 'openmpi' in self.launch_version:
                env_string = ''
                for var in task_env:
                    env_string += '-x "%s" ' % var

            else:
                # this is a crude version of env transplanting where we prep the
                # shell command line.  We likely won't survive any complicated vars
                # (multiline, quotes, etc)
                env_string = ' '
                for var in env_string:
                    env_string += '%s="$%s" ' % (var, var)


        mpirun_rsh_command = "%s -np %d %s %s %s" % (self.launch_command,
                      task_cores, hosts_string, env_string, task_command)

        return mpirun_rsh_command, None


# ------------------------------------------------------------------------------

