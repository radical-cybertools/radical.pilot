
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
# dplace: job launcher for SGI systems (e.g. on Blacklight)
#
class MPIRunDPlace(LaunchMethod):

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

        self.dplace_command = self._find_executable([
            'dplace',            # General case
        ])

        if not self.dplace_command:
            raise RuntimeError("mpirun not found!")

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

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cpu_processes']  # FIXME: handle cpu_threads
        task_env     = cud.get('environment', dict())
        task_args    = cud.get('arguments',   list())
        task_argstr  = self._create_arg_string(task_args)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
        env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
        if env_list:
            if 'hydra' in self.launch_version:
                env_string = '-envlist "%s"' % ','.join(env_list)

            elif 'openmpi' in self.launch_version:
                for var in env_list:
                    env_string += '-x "%s" ' % var

            else:
                raise  RuntimeError('cannot identify MPI flavor [%s]' 
                                   % self.launch_version)

        if 'task_offsets' not in slots:
            raise RuntimeError('insufficient information to launch via %s: %s'
                              % (self.name, slots))

        assert(len(slots['task_offsets']) == 1)
        dplace_offset_start = slots['task_offsets'][0]
        dplace_offset_end   = dplace_offset_start + task_cores - 1

        command = "%s -np %d %s -c %d-%d %s %s" % \
                  (self.dplace_command, task_cores, self.launch_command,
                   dplace_offset_start, dplace_offset_end, 
                   env_string, task_command)

        return command, None


# ------------------------------------------------------------------------------

