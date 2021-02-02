
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
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
    def construct_command(self, t, launch_script_hop):

        slots        = t['slots']
        td          = t['description']
        task_exec    = td['executable']
        task_cores   = td['cpu_processes']  # FIXME: also use cpu_threads
        task_args    = td.get('arguments') or []
        task_argstr  = self._create_arg_string(task_args)

        if 'task_offsets' not in slots :
            raise RuntimeError('insufficient information to launch via %s: %s'
                               % (self.name, slots))

        # FIXME: This is broken due to changes lot structure
        task_offsets   = slots['task_offsets']
        assert(len(task_offsets) == 1)
        dplace_offset  = task_offsets[0]

        task_command   = "%s %s" % (task_exec, task_argstr)
        dplace_command = "%s -c %d-%d %s" % (self.launch_command, dplace_offset,
                                             dplace_offset + task_cores - 1,
                                             task_command)
        return dplace_command, None


# ------------------------------------------------------------------------------

