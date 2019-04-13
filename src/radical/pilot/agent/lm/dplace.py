
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
class DPlace(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # dplace: job launcher for SGI systems (e.g. on Blacklight)
        self.launch_command = ru.which('dplace')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cpu_processes']  # FIXME: cpu_threads
        task_args    = cud.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        if 'task_offsets' not in slots :
            raise RuntimeError('insufficient information to launch via %s: %s' \
                    % (self.name, slots))

        task_offsets = slots['task_offsets']        # This needs to revisited 
        assert(len(task_offsets) == 1)              # since slots structure has 
        dplace_offset = task_offsets[0]             # changed

        if task_argstr:
            task_command = "%s %s" % (task_exec, task_argstr)
        else:
            task_command = task_exec


        dplace_command = "%s -c %d-%d %s" % (
            self.launch_command, dplace_offset,
            dplace_offset+task_cores-1, task_command)

        return dplace_command, None


# ------------------------------------------------------------------------------

