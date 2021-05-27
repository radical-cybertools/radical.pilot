
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class JSRUN(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, cfg, log, prof):

        LaunchMethod.__init__(self, name, lm_cfg, cfg, log, prof)

        self._command   = None
        self._log.debug('=== JSRUN: after init: %s' % self._command)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, lm_cfg, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('jsrun')}

        self._log.debug('=== JSRUN: from scratch: %s' % lm_info['command'])
        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info, lm_cfg):

        self._env     = lm_info['env']
        self._env_sh  = lm_info['env_sh']
        self._command = lm_info['command']

        self._log.debug('=== JSRUN: from info: %s' % self._command)

        assert(self._command)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        return True


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]


    # --------------------------------------------------------------------------
    #
    def _create_resource_set_file(self, slots, uid, sandbox):
        """
        This method takes as input a Task slots and creates the necessary
        resource set file. This resource set file is then used by jsrun to
        place and execute tasks on nodes.

        An example of a resource file is:

        * Task 1: 2 MPI procs, 2 threads per process and 2 gpus per process*

            rank 0 : {host: 1; cpu:  {0, 1}; gpu: {0,1}}
            rank 1 : {host: 1; cpu: {22,23}; gpu: {3,4}}

        * Task 2: 2 MPI procs, 1 thread per process and 1 gpus per process*

            rank 0 : {host: 2; cpu:  7; gpu: 2}
            rank 1 : {host: 2; cpu: 30; gpu: 5}

        * Task 3: 1 proc, 1 thread per process*

            1 : {host: 2; cpu:  7}

        Parameters
        ----------
        slots : List of dictionaries.

            The slots that the task will be placed. A slot has the following
            format:

            {"nodes"         : [{"name"    : "a",
                                 "uid"     : 1,
                                 "gpu_map" : [],
                                 "core_map": [[0]],
                                 "lfs"     : {"path": "/dev/null", "size": 0}
                                }],
             "cores_per_node": 16,
             "gpus_per_node" : 6
             "lfs_per_node"  : {"size": 0, "path": "/dev/null"},
             "lm_info"       : "INFO",
            }

        uid     : task ID (string)
        sandbox : task sandbox (string)
        mpi     : MPI or not (bool, default: False)

        """

        # if `cpu_index_using: physical` is set to run at Lassen@LLNL,
        #  then it returns an error "error in ptssup_mkcltsock_afunix()"
        if slots['ranks'][0]['node'].lower().startswith('lassen'):
            rs_str = ''
        else:
            rs_str = 'cpu_index_using: physical\n'

        rank_id = 0
        for rank in slots['ranks']:

            gpu_maps = list(rank['gpu_map'])
            for map_set in rank['core_map']:
                cores = ','.join(str(core) for core in map_set)
                rs_str += 'rank: %d: {' % rank_id
                rs_str += ' host: %s;'  % str(rank['node_id'])
                rs_str += ' cpu: {%s}'  % cores
                if gpu_maps:
                    gpus = [str(gpu_map[0]) for gpu_map in gpu_maps]
                    rs_str += '; gpu: {%s}' % ','.join(gpus)
                rs_str  += '}\n'
                rank_id += 1

        rs_name = '%s/%s.rs' % (sandbox, uid)
        with open(rs_name, 'w') as fout:
            fout.write(rs_str)

        return rs_name


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        uid          = task['uid']
        slots        = task['slots']
        td           = task['description']
        task_sandbox = task['task_sandbox_path']

        assert(slots), 'missing slots for %s' % uid

        self._log.debug('prep %s', uid)

        # from https://www.olcf.ornl.gov/ \
        #             wp-content/uploads/2018/11/multi-gpu-workshop.pdf
        #
        # CUDA with    MPI, use jsrun --smpiargs="-gpu"
        # CUDA without MPI, use jsrun --smpiargs="off"
        #
        # We only set this for CUDA tasks
        if 'cuda' in td.get('gpu_thread_type', '').lower():
            if 'mpi' in td.get('gpu_process_type', '').lower():
                smpiargs = '--smpiargs="-gpu"'
            else:
                smpiargs = '--smpiargs="off"'
        else:
            smpiargs = ''

        rs_fname = self._create_resource_set_file(slots=slots, uid=uid,
                                                  sandbox=task_sandbox)

        ret = '%s --erf_input %s %s %s' % (self._command, rs_fname,
                                               smpiargs, exec_path)
        return ret


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: does JSRUN set a rank env?
        ret  = 'test -z "$MPI_RANK"  || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK" || export RP_RANK=$PMIX_RANK\n'

        return ret


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

