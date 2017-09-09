
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

        # relevant aprun documentation (search for `-cc` and `-L`):
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
            node_name = node[0]
            if node_name not in nodes:
                nodes[node_name] = list()

            cpu_slots = node[2]
            gpu_slots = node[3]

            if cpu_slots: nodes[node_name].append(cpu_slots)
            if gpu_slots: nodes[node_name].append(gpu_slots)

        self._log.debug('aprun slots: %s', pprint.pformat(slots))
        self._log.debug('      nodes: %s', pprint.pformat(nodes))

        # create a node_spec for each node, which contains the aprun options for
        # that node to start the number of application processes on the given
        # core, and if needed also the GPU processes,  For the latter, we don't
        # really care about thread numbers, but create one process on core 0 for
        # each requested GPU process.
        node_specs = list()
        for node_name,core_sets in nodes.iteritems():

            # some sanity checks:
            #  - make sure all process slots have the same depth
            #  - make sure that cores are not oversubscribed
            nprocs = len(slots)
            depths = set()
            cores  = set()
            ncores = 0

            for core_set in core_sets:
                ncores +=  len(core_set)
                depths.add(len(core_set))
                [cores.add(core) for core in core_set]

            assert(len(depths) == 1)    , 'inconsistent depths : %s' % depths
            assert(ncores == len(cores)), 'oversubscribed cores: %s' % cores

            # derive core pinning for each node
            core_specs = [','.join([str(core) for core     in core_set])
                                              for core_set in core_sets]
            pin_specs  =  ':'.join(core_specs)

            # add the node spec for this node
            node_specs.append(' -L %s -N %d -d %d -cc %s %s'
                              % (node_name, nprocs, depths[0], pin_specs, cmd))


        node_layout   = ' : '.join(node_specs)
        aprun_command = "%s %s" % (self.launch_command, node_layout)
        self._log.debug('aprun cmd: %s', aprun_command)

        return aprun_command, None


# ------------------------------------------------------------------------------

