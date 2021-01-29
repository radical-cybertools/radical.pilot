
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
        # "Regular" tasks
        self.launch_command = ''

    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, profiler):
        return {'version_info': {
            name: {'version': '0.42', 'version_detail': 'There is no spoon'}}}

    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return list()


    # --------------------------------------------------------------------------
    #
    def get_launch_command(self, task, exec_script):

        return exec_script


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return 'export RP_RANK=0'


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        ret = ''
        gpus = [g[0] for g in rank['gpus']]
        if gpus:
            ret += '    export CUDA_VISIBLE_DEVICES=%s\n' % ','.join(gpus)

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td.get('arguments')
        task_argstr  = self._create_arg_string(task_args)
        command      = "%s %s" % (task_exec, task_argstr)

        ret += '    %s\n' % command.rstrip()

        return ret


# ------------------------------------------------------------------------------

