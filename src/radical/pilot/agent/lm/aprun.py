
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import pprint
import radical.utils as ru

from .base import LaunchMethod

# maximum length of command line arguments we support. Beyond that we use
# a hostfile to specify the process layout.
ARG_MAX = 4096


# ==============================================================================
#
class APRun(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):
        # aprun: job launcher for Cray systems
        self.launch_command = ru.which('aprun')

        # TODO: ensure that only one concurrent aprun per node is executed!


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        cud        = cu['description']
        slots      = cu['slots']
        executable = cud['executable']
        args       = cud['arguments']
        argstr     = self._create_arg_string(args)

        if argstr: cmd = "%s %s" % (executable, argstr)
        else     : cmd = executable

        # we get something like the following from the scheduler:
        #
        #     cu = { ...
        #       'cpu_processes'    : 4,
        #       'cpu_process_type' : 'mpi',
        #       'cpu_threads'      : 2,
        #       'gpu_processes     : 2,
        #       'slots':
        #       {   # 'nodes': [{'name': node_name,
        #           #            'uid': node_uid,
        #           #            'core_map': [core_map],
        #           #            'gpu_map': [gpu_map],
        #           #            'lfs': lfs}],
        #         'nodes'         : [{  'name': node_1,
        #                               'uid': node_uid_1,
        #                               'core_map': [[0, 2], [4, 6]],
        #                               'gpu_map': [[0]],
        #                               'lfs': 1024},
        #                            {  'name': node_2,
        #                               'uid': node_uid_2,
        #                               'core_map': [[1, 3], [5, 7]],
        #                               'gpu_map': [[0]],
        #                               'lfs': 1024}
        #                            ],
        #         'cores_per_node': 8,
        #         'gpus_per_node' : 1,
        #         'lm_info'       : { ... }
        #       }
        #     }
        #
        # The 'nodes' entry here defines what nodes and cores we should ask
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
        # of cpu and gpu processes for units started via aprun.
        #
        # Below we sift through the unit slots and create a slot list which
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
        for node in slots['nodes']:

            node_id = node['uid']
            if node_id not in nodes:
                # keep all cpu and gpu slots, record depths
                nodes[node_id] = {'cpu' : list(),
                                  'gpu' : list()}

            # add all cpu and gpu process slots to the node list.
            for cpu_slot in node['core_map']: nodes[node_id]['cpu'].append(cpu_slot)
            for gpu_slot in node['gpu_map']: nodes[node_id]['gpu'].append(gpu_slot)


        self._log.debug('aprun slots: %s', pprint.pformat(slots))
        self._log.debug('      nodes: %s', pprint.pformat(nodes))

        # create a node_spec for each node, which contains the aprun options for
        # that node to start the number of application processes on the given
        # core.
        node_specs = dict()
        for node_id in nodes:

            cpu_slots = nodes[node_id]['cpu']
            gpu_slots = nodes[node_id]['gpu']

            self._log.debug('cpu_slots: %s', pprint.pformat(cpu_slots))
            self._log.debug('gpu_slots: %s', pprint.pformat(gpu_slots))

            assert(cpu_slots or gpu_slots)

            # make sure all process slots have the same depth
            if cpu_slots:
                depths = set()
                for cpu_slot in cpu_slots:
                    depths.add(len(cpu_slot))
                assert(len(depths) == 1), 'aprun implies uniform depths: %s' % depths
                depth = list(depths)[0]
            else:
                depth = 1

            # ensure that depth is `1` if gpu processes are requested
            if gpu_slots:
                assert(1 == depth), 'aprun implies depth==1 for gpu procs: %s' % depth

            # derive core pinning for each node (gpu's go to core `0`
            core_specs = list()
            for cpu_slot in cpu_slots:
                core_specs.append(','.join([str(core) for core in cpu_slot]))
            for gpu_slot in gpu_slots:
                core_specs.append('0')
            pin_specs  =  ':'.join(core_specs)

            # count toal cpu / gpu processes
            nprocs = len(cpu_slots) + len(gpu_slots)

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
        aprun_command = self.launch_command
        for node_spec,info in node_specs.iteritems():

            # nprocs must be uniform
            nprocs_list = list(info['nprocs'])
            nprocs      = nprocs_list[0]
            assert(len(nprocs_list) == 1), nprocs_list

            aprun_command += ' -n %d -N %s -L %s %s %s :' % \
                             (nprocs * len(info['nodes']), nprocs,
                              ','.join(info['nodes']), node_spec, cmd)

        # remove trailing colon from above
        aprun_command = aprun_command[:-1]
        self._log.debug('aprun cmd: %s', aprun_command)

        return aprun_command, None


# ------------------------------------------------------------------------------

