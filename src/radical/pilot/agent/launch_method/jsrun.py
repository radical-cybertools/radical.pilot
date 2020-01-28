
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

    # --------------------------------------------------------------------------
    #
    def _create_resource_set_file(self, slots, uid, sandbox):
        """
        This method takes as input a CU slots and creates the necessary
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

            The slots that the unit will be placed. A slot has the following
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

        uid     : unit ID (string)
        sandbox : unit sandbox (string)
        mpi     : MPI or not (bool, default: False)

        """

        rs_str = 'cpu_index_using: physical\n'
        rank = 0
        for node in slots['nodes']:

            gpu_maps = list(node['gpu_map'])
            for map_set in node['core_map']:
                cores = ','.join(str(core) for core in map_set)
                rs_str += 'rank: %d: {'  % rank
                rs_str += ' host: %s;'  % str(node['uid'])
                rs_str += ' cpu: {%s}'  % cores
                if gpu_maps:
                    gpus  = ','.join(str(gpu) for gpu in gpu_maps.pop(0))
                    rs_str += '; gpu: {%s}' % gpus
                rs_str += '}\n'
                rank   += 1

        rs_name = '%s/%s.rs' % (sandbox, uid)
        with open(rs_name, 'w') as fout:
            fout.write(rs_str)

        return rs_name


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        uid          = cu['uid']
        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_args    = cud.get('arguments')   or list()
        task_argstr  = self._create_arg_string(task_args)
        task_sandbox = cu['unit_sandbox_path']

        assert(slots), 'missing slots for %s' % uid

        self._log.debug('prep %s', uid)

        if task_argstr: task_command = "%s %s" % (task_exec, task_argstr)
        else          : task_command = task_exec

        env_string = ''
      # task_env   = cud.get('environment') or dict()
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
        if 'cuda' in cud.get('gpu_thread_type', '').lower():
            if 'mpi' in cud.get('gpu_process_type', '').lower():
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

