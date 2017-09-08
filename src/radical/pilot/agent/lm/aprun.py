
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

        self._log.debug('aprun slots: %s', pprint.pformat(slots))

        # relevant aprun documentation (search for `-cc` and `-L`):
        # http://docs.cray.com/books/S-2496-4101/html-S-2496-4101/cnl_apps.html
        #
        #     aprun -L node_1 -N 1 -d 3 cmd : \
        #           -L node_2 -N 2 -d 3 cmd :
        #
        # Each node can only be used *once* in that spec for any individual
        # aprun command.  This means that tpp must be uniform for that node 
        #
        # Pin the procs and its threads to specific cores:
        #
        #     aprun -L node_1 -N 1 -d 3 -cc 0,1,2       cmd : \
        #           -L node_2 -N 2 -d 3 -cc 0,1,2:3,4,5 cmd
        #
        # create a nodes list out of slots.  This host list will result in one
        # `-L` argument each.  It contains sets of cores - for each set, one
        # process will be placed (`-N`).  All sets need to be of the same
        # length (`-d`).
        nodes = dict()
        for node in slots['nodes']:
            node_name = node[0]
            if node_name not in nodes:
                nodes[node_name] = list()  # list of process slots to fill
            cpu_slots = node[2]
            gpu_slots = node[3]

            # Since we don't really distingush gpu and cpu processes in aprun,
            # we treat *all* gpu entries as `[0]`, meaning that a single process
            # will be pinned to core 0.
            if gpu_slots:
                gpu_slots = [0]

            if cpu_slots:
                nodes[node_name].extend(cpu_slots)
            if gpu_slots:
                nodes[node_name].extend(gpu_slots)

        self._log.debug('aprun nodes: %s', pprint.pformat(nodes))
      # self._log.debug('slots: %s', pprint.pformat(slots))
      # self._log.debug('nodes: %s', pprint.pformat(nodes))

        node_specs = list()
        for node_name, slots in nodes.iteritems():

            # make sure all process slots have the same depth
            nprocs = len(slots)
            depths = set()
            for slot in slots:
                depth = len(slot)
                depths.add(depth)
          #     self._log.debug('slot %s - %s', slot, depth)
          # self._log.debug('depths %s', depths)
            assert(len(depths) == 1), 'inconsistent process depths: %s' % depths

            # make usre that cores are not oversubscribed
            used_cores  = set()
            used_ncores = 0
            for slot in slots:
                for core in slot:
                    used_cores.add(core)
                    used_ncores += 1
            assert(used_ncores == len(used_cores)), \
                   'oversubscribed cores : %s' % used_cores

            # convert int slots into strings for the join ops below
            slots = [[str(core) for core in slot] for slot in slots]

            # get core pinning for each slot (keep track of used cores)
            cores = [','.join(slot) for slot in slots]
            pins  =  ':'.join(cores)

            # add the node spec for this node
            node_specs.append(' -L %s -N %d -d %d -cc %s %s'
                              % (node_name, nprocs, depth, pins, cmd))


        node_layout   = ' : '.join(node_specs)
        aprun_command = "%s %s" % (self.launch_command, node_layout)
        self._log.debug('aprun cmd: %s', aprun_command)

        return aprun_command, None


# ------------------------------------------------------------------------------

