
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


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
        cores      = cud['cores']
        mpi        = cud['mpi']
        args       = cud.get('arguments') or []
        argstr     = self._create_arg_string(args)

        if argstr: command = "%s %s" % (executable, argstr)
        else     : command = executable

        if mpi   : pes = cores
        else     : pes = 1

        # the `slots` struct will tell us on how many (and which) nodes and
        # cores we can start the CU.  We kind of want that information turned
        # around: how many executables need to be spawned on each of the nodes
        # in that list?  We do expect a uniform layout for the resulting set, in
        # the sense that we expect the same number of cores to be allocated for
        # each process instance.  We do not expect the same number of processes
        # per core, and we do not expect homogeneous nodes.  GPU process and
        # core requests are mapped to core_0, and expected to not really use
        # that core 
        #
        # TODO (GPU): is that the right thing to do?  Could not find any advise
        #             on GPU placement via aprun.  Possibly needs a BW ticket.
        #
        # We build the following data structure:
        #
        #   nodes = {'node_1' : { 'cpu_procs' : 2,
        #                         'cpu_tpp'   : 4,
        #                         'gpu_procs' : 1, 
        #                         'gpu_tpp'   : 1},
        #           {'node_2' : { 'cpu_procs' : 4,
        #                         'cpu_tpp'   : 8,
        #                         'gpu_procs' : 1, 
        #                         'gpu_tpp'   : 1},
        #             ...
        #           }
        #
        # We will raise an error if `threads_per_process` has different values
        # for the same node.  We expect the node name to be an identifier
        # useable for aprun's node specification.  No consistency check is done
        # on the process and thread placement - we trust the scheduler in its
        # infinite wisdom.
        #
        nodes = dict()
        for slot_node in slots['nodes']:

            node_name = slot_node[0]
            if node_name not in nodes:
                nodes[node_name] = {'cpu_procs' : 0,
                                    'cpu_tpp'   : set(), 
                                    'gpu_procs' : 0, 
                                    'gpu_tpp'   : set()}  # unused

            for cpu_slot in slot_node[1]:
                nodes[node_name]['cpu_procs'] += 1
                nodes[node_name]['cpu_tpp'].add(len(cpu_slot))

            for gpu_slot in slot_node[1]:
                nodes[node_name]['gpu_procs'] += 1
                nodes[node_name]['gpu_tpp'].add(len(gpu_slot))


        # sanity check
        for node in nodes:

            if len(node['cpu_tpp'] != 1):
                self._log.debug(pprint.pformat(slots))
                self._log.debug(pprint.pformat(nodes))
                raise ValueError('inconsistent threads_per_process count')


        # build apirun command.  For each node, we create a new '-nx exex_n'
        # section, then concat them all with ':' for a complete layout
        # description.  GPU procs area added 

        aprun_command = "%s -n %d %s" % (self.launch_command, pes, command)

        return aprun_command, None


# ------------------------------------------------------------------------------

