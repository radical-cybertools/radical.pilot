
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import radical.utils as ru

from .base import LaunchMethod

# maximum length of command line arguments we support. Beyond that we use
# a hostfile to specify the process layout.
ARG_MAX = 4096


# ------------------------------------------------------------------------------
#
# aprun: job launcher for Cray systems (alps-run)
# TODO : ensure that only one concurrent aprun per node is executed!
#
class APRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, lm_cfg, rm_info, log, prof):

        self._command: str = ''

        LaunchMethod.__init__(self, name, lm_cfg, rm_info, log, prof)


    # --------------------------------------------------------------------------
    #
    def _init_from_scratch(self, env, env_sh):

        lm_info = {'env'    : env,
                   'env_sh' : env_sh,
                   'command': ru.which('aprun')}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _init_from_info(self, lm_info):

        self._env         = lm_info['env']
        self._env_sh      = lm_info['env_sh']
        self._command     = lm_info['command']

        assert self._command

        self._mpi_version = lm_info['mpi_version']
        self._mpi_flavor  = lm_info['mpi_flavor']


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        pass


    # --------------------------------------------------------------------------
    #
    def can_launch(self, task):

        if not task['description']['executable']:
            return False, 'no executable'

        return True, ''


    # --------------------------------------------------------------------------
    #
    def get_launcher_env(self):

        return ['. $RP_PILOT_SANDBOX/%s' % self._env_sh]


    # --------------------------------------------------------------------------
    #
    def get_launch_cmds(self, task, exec_path):

        slots = task['slots']

        # we get something like the following from the scheduler:
        #
        #     t = { ...
        #       'cpu_processes'    : 4,
        #       'cpu_process_type' : 'mpi',
        #       'cpu_threads'      : 2,
        #       'gpu_processes     : 2,
        #       'slots':
        #       {   # 'ranks': [{'node_name': node_name,
        #           #            'node_id'  : node_id,
        #           #            'core_map' : [core_map],
        #           #            'gpu_map'  : [gpu_map],
        #           #            'lfs'      : lfs}],
        #         'ranks'         : [{  'node_name': node_1,
        #                               'node_id'  : node_id_1,
        #                               'core_map' : [[0, 2], [4, 6]],
        #                               'gpu_map'  : [[0]],
        #                               'lfs'      : 1024},
        #                            {  'node_name': node_2,
        #                               'node_id'  : node_id_2,
        #                               'core_map' : [[1, 3], [5, 7]],
        #                               'gpu_map'  : [[0]],
        #                               'lfs'      : 1024}
        #                            ],
        #         'cores_per_node': 8,
        #         'gpus_per_node' : 1,
        #         'lm_info'       : { ... }
        #       }
        #     }
        #
        # The 'ranks' entry here defines what nodes and cores we should ask
        # aprun to populate with processes.
        #
        # The relevant aprun documentation is at (search for `-cc` and `-L`):
        # http://docs.cray.com/books/S-2496-4101/html-S-2496-4101/cnl_apps.html
        #
        #   -L node_list    : candidate nodes to constrain application placement
        #   -n pes          : number of PEs to place
        # # -N pes_per_node : number of PEs to place per node
        #   -d depth        : number of CPUs for each PE and its threads
        #   -cc cpu_list    : bind processing elements (PEs) to CPUs.
        #
        # (CPUs here mostly means cores)
        #
        # Example:
        #     aprun -L node_1 -n 1 -N 1 -d 3 -cc 0,1,2       cmd : \
        #           -L node_2 -n 1 -N 1 -d 3 -cc 0,1,2       cmd : \
        #           -L node_3 -n 2 -N 2 -d 3 -cc 0,1,2:3,4,5 cmd :
        #
        # Each node can only be used *once* in that way for any individual
        # aprun command.  This means that the depth must be uniform for that
        # node, over *both* cpu and gpu processes.  This limits the mixability
        # of cpu and gpu processes for tasks started via aprun.
        #
        # Below we sift through the task slots and create a slot list which
        # basically defines sets of cores (-cc) for each node (-L).  Those sets
        # need to have the same size per node (the depth -d).  The number of
        # sets defines the number of procs to start (-n/-N).
        #
        # If the list of arguments for aprun becomes too long, we create
        # a temporary hostfile instead, and reference it from the aprun command
        # line.  POSIX specifies 4k characters, and we don't bother to check for
        # larger sizes, since at that point command lines become unwieldy from
        # a debugging perspective anyway.
        #
        # FIXME: this is not yet implemented, since aprun does not seem to
        #        support placement files just yet.
        #
        # Because of that FIXME above, but also in general to keep the command
        # line managable, we collapse the aprun node spcs for all nodes with the
        # same layout:
        #
        #   original:
        #     aprun -L node_1 -n 1  -N 1 -d 3 -cc 0,1,2       cmd : \
        #           -L node_2 -n 1  -N 1 -d 3 -cc 0,1,2       cmd : \
        #           -L node_3 -n 2  -N 2 -d 3 -cc 0,1,2:3,4,5 cmd :
        #
        #   collapsed:
        #     aprun -L node_1,node_2 -n 2 -N 1 -d 3 -cc 0,1,2       cmd : \
        #           -L node_3        -n 2 -N 2 -d 3 -cc 0,1,2:3,4,5 cmd :
        #
        # Note that the `-n` argument needs to be adjusted accordingly.
        #
        nodes = dict()
        for rank in slots['ranks']:

            node_id = rank['node_id']
            if node_id not in nodes:
                # keep all cpu and gpu slots, record depths
                nodes[node_id] = {'cpu' : list(),
                                  'gpu' : list()}

            # add all cpu and gpu process slots to the node list.
            for cpu_slot in rank['core_map']:
                nodes[node_id]['cpu'].append(cpu_slot)

        # create a node_spec for each node, which contains the aprun options for
        # that node to start the number of application processes on the given
        # core.
        node_specs = dict()
        for node_id in nodes:

            cpu_slots = nodes[node_id]['cpu']
          # gpu_slots = nodes[node_id]['gpu']

            assert(cpu_slots)

            # make sure all process slots have the same depth
            if cpu_slots:
                depths = set()
                for cpu_slot in cpu_slots:
                    depths.add(len(cpu_slot))
                assert(len(depths) == 1), 'aprun implies uniform depths: %s' % depths
                depth = list(depths)[0]
            else:
                depth = 1

            # derive core pinning for each node (gpu's go to core `0`
            core_specs = list()
            for cpu_slot in cpu_slots:
                core_specs.append(','.join([str(core) for core in cpu_slot]))
            pin_specs  =  ':'.join(core_specs)

            # count toal cpu / gpu processes
            nprocs = len(cpu_slots)

            # create the unique part of the node spec, and keep spec info like
            # this:
            #
            #   '-d 2 -cc 0,1:2,3' : {'nprocs' : [2, 2],                 # set
            #                         'nodes'  : ['node_1', 'node_2']},  # list
            #   ...
            #
            # Note that for the same spec string, we should *always* have the
            # same matching value for `nprocs`.
            spec_key = '-d %d -cc %s' % (depth, pin_specs)
            if spec_key not in node_specs:
                node_specs[spec_key] = {'nprocs': set(),   # number of processes
                                        'nodes' : list()}  # nodes for this spec

            node_specs[spec_key]['nprocs'].add(nprocs)
            node_specs[spec_key]['nodes' ].append(node_id)

        # Now that we have the node specs, and also know what nodes to apply
        # them to, we can construct the aprun command:
        cmd = self._command
        for node_spec, info in list(node_specs.items()):

            # nprocs must be uniform
            nprocs_list = list(info['nprocs'])
            nprocs      = nprocs_list[0]
            assert(len(nprocs_list) == 1), nprocs_list

            cmd += ' -n %d -N %s -L %s %s %s :' % \
                             (nprocs * len(info['nodes']), nprocs,
                              ','.join(info['nodes']), node_spec, exec_path)

        # remove trailing colon from above
        cmd = cmd[:-1]
        self._log.debug('aprun cmd: %s', cmd)

        return cmd.rstrip()


    # --------------------------------------------------------------------------
    #
    def get_rank_cmd(self):

        # FIXME: do we have an APLS_RANK?

        ret  = 'test -z "$MPI_RANK"  || export RP_RANK=$MPI_RANK\n'
        ret += 'test -z "$PMIX_RANK" || export RP_RANK=$PMIX_RANK\n'

        return ret


    # --------------------------------------------------------------------------
    #
    def get_rank_exec(self, task, rank_id, rank):

        td           = task['description']
        task_exec    = td['executable']
        task_args    = td['arguments']
        task_argstr  = self._create_arg_string(task_args)
        command      = '%s %s' % (task_exec, task_argstr)

        return command.rstrip()


# ------------------------------------------------------------------------------

