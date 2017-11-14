
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


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cpu_processes']  # FIXME: handle cpu_threads
        task_args    = cud.get('arguments', [])
        task_argstr  = self._create_arg_string(task_args)

        if not 'task_offsets' in slots:
            raise RuntimeError('insufficient information to launch via %s: %s' \
                    % (self.name, slots))

        task_offsets = slots['task_offsets']
        assert(len(task_offsets == 1))
        dplace_offset = task_offsets[0]

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec

        mpirun_dplace_command = "%s -np %d %s -c %d-%d %s" % \
            (self.mpirun_command, task_cores, self.launch_command,
             dplace_offset, dplace_offset+task_cores-1, task_command)

        return mpirun_dplace_command, None


# ------------------------------------------------------------------------------

