

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"

# ------------------------------------------------------------------------------
#
# This file contains methods to support the transition of the non-gpu aware
# scheduling data structures to a gpu aware version.  It is used to allow
# combine new AgentScheduling implementations with 'old' LaunchMethods in the
# AgentExecutingComponent. The LMs have to make sure to end up with a version of
# the slot structure they can handle.
#
#     slots = { ...
#       'cpu_processes'    : 4,
#       'cpu_process_type' : 'mpi',
#       'cpu_threads'      : 2,
#       'gpu_processes     : 2,
#       'slots' :
#       {                 # [[nodename, [node_uid], [core indexes],   [gpu idx]]]
#         'nodes'         : [[node_1,   node_uid_1, [[0, 2], [4, 6]], [[0]    ]],
#                            [node_2,   node_uid_2, [[1, 3], [5, 7]], [[0]    ]]],
#         'cores_per_node': 8,
#         'gpus_per_node' : 1,
#         'lm_info'       : { ... }
#       }
#     }
#
def slots2opaque(slots, nodes):
    '''
    Translate the new `slots` structure as documented in the agent scheduler's
    base class into the old `opaque_slots` structure as documented below.
    '''

    # old opaque_slots structure:
    #
    # task_slots = [self.nodes[x]['name'] for x in offsets]
    # opaque_slots = {
    #     'task_slots'   : task_slots,         # [node:core]
    #     'task_offsets' : offsets,            # [core] (global)
    #     'lm_info'      : self._lrms_lm_info  # unchanged
    # }
    #
    task_slots    = list()
    task_offsets  = list()

    for entry in slots:
        for cpu_set in entry[2]:
            task_slots.append('%s:%d' % (entry[1], cpu_set[0]))

        for gpu_set in entry[3]:
            task_slots.append('%s:%d' % (entry[1], gpu_set[0]))

    task_offsets = _slots2offset(task_slots, nodes)

    return {'task_slots'   : task_slots,
            'task_offsets' : task_offsets,
            'lm_info'      : slots['lrms_lm_info']}


# --------------------------------------------------------------------------
#
# Convert a set of slots into an index into the global slots list
#
def _slots2offset(task_slots, nodes):
    '''
    Back-translate the opaque_slots derived above into global core offsets
    '''

    # Note: rhis assumes all hosts have the same number of cores

    global_idx     = 0
    global_offsets = list()

    for node in nodes:

        cores_per_node = len(node['cores'])

        for ts in task_slots:

            node_name, core_idx = ts.split(':', 1)

            if node_name == node['uid']:
                global_offsets.append(global_idx + core_idx)

        global_idx += cores_per_node

    return min(global_offsets)


# ------------------------------------------------------------------------------

