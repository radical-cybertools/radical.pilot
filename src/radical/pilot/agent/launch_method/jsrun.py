
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class JSRUN(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self.launch_command = ru.which('jsrun')
        assert(self.launch_command)


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        return "echo $PMIX_RANK"


    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, log, profiler):

        if 'session.lassen' in cfg['sid'].lower():
            # correctness of GPU IDs is based on env var CUDA_VISIBLE_DEVICES
            # which value is taken from `gpu_map`, it is set at class
            # `radical.pilot.agent.scheduler.base.AgentSchedulingComponent`
            # (method `_handle_cuda`)
            #
            # *) since the launching happens at the login node Lassen@LLNL
            #    thus the session name can be used to identify the machine
            #
            # FIXME: the `cvd_id_mode` setting should eventually move into the
            #        resource config.
            lm_info = {'cvd_id_mode': 'physical'}
        else:
            lm_info = {'cvd_id_mode': 'logical'}
        return lm_info


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

        # `cpu_index_using: physical` causes the following issue
        #    "error in ptssup_mkcltsock_afunix()"
        rs_str = 'cpu_index_using: logical\n'
        rank = 0
        for node in slots['nodes']:

            gpu_maps = list(node['gpu_map'])
            for map_set in node['core_map']:
                cores = ','.join(str(core) for core in map_set)
                rs_str += 'rank: %d: {'  % rank
                rs_str += ' host: %s;'  % str(node['uid'])
                rs_str += ' cpu: {%s}'  % cores
                if gpu_maps:
                    gpus = [str(gpu_map[0]) for gpu_map in gpu_maps]
                    rs_str += '; gpu: {%s}' % ','.join(gpus)
                rs_str += '}\n'
                rank   += 1

        rs_name = '%s/%s.rs' % (sandbox, uid)
        with open(rs_name, 'w') as fout:
            fout.write(rs_str)

        return rs_name


    # --------------------------------------------------------------------------
    #
    def construct_command(self, t, launch_script_hop):

        uid          = t['uid']
        slots        = t['slots']
        td           = t['description']
        task_exec    = td['executable']
        task_args    = td.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)
        task_sandbox = t['task_sandbox_path']

        assert(slots), 'missing slots for %s' % uid

        self._log.debug('prep %s', uid)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
      # task_env   = td.get('environment') or dict()
      # env_list   = self.EXPORT_ENV_VARIABLES + task_env.keys()
      # env_string = ' '.join(['-E "%s"' % var for var in env_list])
      #
      # # jsrun fails if an -E export is not set
      # for var in env_list:
      #     if var not in os.environ:
      #         os.environ[var] = ''

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

      # flags = '-n%d -a1 ' % (task_procs)
        command = '%s --erf_input %s %s %s %s' % (self.launch_command, rs_fname,
                                                  smpiargs, env_string,
                                                  task_command)

      # with open('./commands.log', 'a') as fout:
      #     fout.write('%s\n' % command)

        return command, None


# ------------------------------------------------------------------------------

