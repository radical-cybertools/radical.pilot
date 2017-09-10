
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import pprint
import radical.utils as ru

from .base import LaunchMethod


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
        self.launch_command= ru.which('aprun')

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
        #       {                 # [[nodename, [node_uid], [core indexes],   [gpu idx]]]
        #         'nodes'         : [[node_1,   node_uid_1, [[0, 2], [4, 6]], [[0]    ]],
        #                            [node_2,   node_uid_2, [[1, 3], [5, 7]], [[0]    ]]],
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
        #   -N pes_per_node : number of PEs to place per node
        #   -d depth        : number of CPUs for each PE and its threads
        #   -cc cpu_list    : bind processing elements (PEs) to CPUs.
        #
        # (CPUs here mostly means cores)
        #
        # Example:
        #     aprun -L node_1 -N 1 -d 3 -cc 0,1,2       cmd : \
        #           -L node_2 -N 2 -d 3 -cc 0,1,2:3,4,5 cmd :
        #
        # Each node can only be used *once* in that way for any individual
        # aprun command.  This means that the depth must be uniform for that
        # node, over *both* cpu and gpu processes.  This limits the mixability
        # of cpu and gpu processes for units started via aprun.
        #
        # Below we sift through the unit slots and create a slot list which
        # basically defines sets of cores (-cc) for each node (-L).  Those sets
        # need to have the same size per node (the depth -d).  The number of
        # sets defines the number of procs to start (-N).
        nodes = dict()
        for node in slots['nodes']:

            node_id = node[1]
            if node_id not in nodes:
                # keep all cpu and gpu slots, record depths
                nodes[node_id] = {'cpu' : list(),
                                  'gpu' : list())

            # add all cpu and gpu process slots to the node list.
            for cpu_slot in node[2]: nodes[node_id]['cpu'].append(cpu_clot)
            for gpu_slot in node[3]: nodes[node_id]['gpu'].append(gpu_clot)


        self._log.debug('aprun slots: %s', pprint.pformat(slots))
        self._log.debug('      nodes: %s', pprint.pformat(nodes))

        # create a node_spec for each node, which contains the aprun options for
        # that node to start the number of application processes on the given
        # core.
        node_specs = list()
        for node_id in nodes:

            cpu_slots = nodes[node_id]['cpu']
            gpu_slots = nodes[node_id]['gpu']

            # make sure all process slots have the same depth
            depths = set()
            for cpu_slot in cpu_slots:
                depths.add(len(cpu_slot))
            assert(len(depths) == 1), 'aprun implies uniform process depths'

            # ensure that depth is `1` if gpu processes are requested
            if gpu_slots:
                assert(1 == depths[0]), 'aprun implies depth==1 for gpu procs'

            # derive core pinning for each node (gpu's go to core `0`
            core_specs = list()
            for cpu_slot in cpu_slots:
                core_specs.append(','.join([str(core) for core in cpu_slot]))
            for gpu_slot in gpu_slots:
                core_specs.append('0')
            pin_specs  =  ':'.join(core_specs)

            # count toal cpu / gpu processes
            nprocs = len(cpu_slots) + len(gpu_slots)

            # add the node spec for this node
            node_specs.append(' -L %s -N %d -d %d -cc %s %s'
                              % (node_id, nprocs, depths[0], pin_specs, cmd))


        node_layout   = ' : '.join(node_specs)
        aprun_command = "%s %s" % (self.launch_command, node_layout)
        self._log.debug('aprun cmd: %s', aprun_command)

        return aprun_command, None


# ------------------------------------------------------------------------------

