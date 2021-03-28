
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Fork(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ''


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        # NOTE: ignore thread and process counts, and expect application to do
        #       the needful

        td          = t['description']
        task_exec   = td['executable']
        task_args   = td.get('arguments') or []
        task_argstr = self._create_arg_string(task_args)

        command = "%s %s" % (task_exec, task_argstr)

        return command.strip(), None


# ------------------------------------------------------------------------------

