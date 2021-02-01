
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Fork(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, log, prof):

        LaunchMethod.__init__(self, name, cfg, log, prof)

        self.node_name = ru.get_hostname()


    # --------------------------------------------------------------------------
    #
    def initialize(self, rm, lmcfg):

        return {'version'       : ru.sh_callout('uname'      )[0],
                'version_detail': ru.sh_callout('uname -irno')[0]}


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        # ensure single rank on localhost
        if len(task['slots']['ranks']) > 1:
            return False

        node = task['slots']['ranks'][0]['node']
        print('====', node, self.node_name)
        if node not in ['localhost', self.node_name]:
            return False

        return True


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return list()


    # --------------------------------------------------------------------------
    #
    def get_launch_cmd(self, task, exec_script):

        return exec_script


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return 'export RP_RANK=0'


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td          = task['description']
        task_exec   = td['executable']
        task_args   = td.get('arguments')
        task_argstr = self._create_arg_string(task_args)
        command     = "%s %s" % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

