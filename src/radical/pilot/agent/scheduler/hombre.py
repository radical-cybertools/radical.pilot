
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"


import copy

import radical.utils as ru

from .base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
class Hombre(AgentSchedulingComponent):
    '''
    HOMBRE: HOMogeneous Bag-of-task REsource allocator.  Don't kill me...

    The scheduler implementation manages a set of resources (cores).  When it
    gets the first scheduling request, it will assume that to be representative
    *for all future sccheduling requests*, ie. it assumes that all tasks are
    homogeneous.  It thus splits the resources into equal sized chunks and
    assigns them to the incoming requests.  The chunking is fast, no decisions
    need to be made, the scheduler really just hands out pre-defined chunks,
    which is relatively quick - that's the point, to trade generality with
    performance.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.nodes = None

        AgentSchedulingComponent.__init__(self, cfg, session)

        # homogeneous workloads are always uniform
        self._uniform_wl = True


    # --------------------------------------------------------------------------
    #
    # FIXME: this should not be overloaded here, but in the base class
    #
    def finalize_child(self):

        # make sure that parent finalizers are called
        super(Hombre, self).finalize_child()


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # * oversubscribe:
        #   Cray's aprun for example does not allow us to oversubscribe CPU
        #   cores on a node, so we can't, say, run n CPU processes on an n-core
        #   node, and than add one additional process for a GPU application.  If
        #   `oversubscribe` is set to False (which is the default for now),
        #   we'll prevent that behavior by allocating one additional CPU core
        #   for each set of requested GPU processes.
        self._oversubscribe = self._cfg.get('oversubscribe', True)

        if not self._oversubscribe:
            raise ValueError('HOMBRE needs oversubscription enabled')

        # NOTE: We delay the actual configuration until we received the first
        #       unit to schedule - at that point we can slice and dice the
        #       resources into suitable static slots.
        self._configured = False

        self.free = list()     # declare for early debug output


    # --------------------------------------------------------------------------
    #
    def _delayed_configure(self, cud):

        if self._configured:
            return

        self.chunk  = {'cpu_processes'    : cud['cpu_processes'   ],
                       'cpu_process_type' : cud['cpu_process_type'],
                       'cpu_threads'      : cud['cpu_threads'     ],
                       'cpu_thread_type'  : cud['cpu_thread_type' ],

                       'gpu_processes'    : cud['gpu_processes'   ],
                       'gpu_process_type' : cud['gpu_process_type'],
                       'gpu_threads'      : cud['gpu_threads'     ],
                       'gpu_thread_type'  : cud['gpu_thread_type' ],
                       }

        self.cpn     = self._rm_cores_per_node
        self.gpn     = self._rm_gpus_per_node

        self.free    = list()     # list of free chunks
        self.lock    = ru.Lock()  # lock for the above list

        cores_needed = cud['cpu_processes'] * cud['cpu_threads']
        gpus_needed  = cud['gpu_processes']

        # check if we need single or multi-node chunks
        if  cud['cpu_process_type'] != 'MPI' and \
            cud['gpu_process_type'] != 'MPI' :

            # single node task - check if it fits
            if cores_needed > self.cpn or \
               gpus_needed  > self.gpn:
                raise ValueError('unit does not fit on node')


        # ---------------------------------------------------------------------
        # create as many equal sized chunks from the available nodes as
        # possible, and put them into the `free` list.  The actual scheduling
        # algorithm will blindly pick chunks from that list whenever a new CUD
        # arrives.
        cblock   = cud['cpu_threads']
        ncblocks = cud['cpu_processes']
        cblocks  = list()
        cidx     = 0

        while cidx + cblock <= self.cpn:
            cblocks.append(list(range(cidx,cidx + cblock)))
            cidx += cblock

        gblock   = 1
        ngblocks = cud['gpu_processes']
        gblocks  = list()
        gidx     = 0
        while gidx + gblock <= self.gpn:
            gblocks.append(list(range(gidx,gidx + gblock)))
            gidx += gblock

        self._log.debug('core blocks %s', cblocks)
        self._log.debug('gpu  blocks %s', gblocks)

        for node in self.nodes:
            node['cblocks'] = copy.deepcopy(cblocks)
            node['gblocks'] = copy.deepcopy(gblocks)

        # ----------------------------------------------------------------------
        def next_slot(slot=None):
            if slot:
                del(slot['ncblocks'])
                del(slot['ngblocks'])
                self.free.append(slot)
            return {'nodes'         : list(),
                    'cores_per_node': self.cpn,
                    'gpus_per_node' : self.gpn,
                    'lm_info'       : self._rm_lm_info,
                    'ncblocks'      : 0,
                    'ngblocks'      : 0}
        # ---------------------------------------------------------------------
        nidx   = 0
        nnodes = len(self.nodes)
        slot   = next_slot()
        while nidx < nnodes:


            if  slot['ncblocks'] == ncblocks and \
                slot['ngblocks'] == ngblocks :
                slot = next_slot(slot)

            node  = self.nodes[nidx]
            nuid  = node['uid']
            nname = node['name']
            ok    = True

            while slot['ncblocks'] < ncblocks:
                if node['cblocks']:
                    cblock = node['cblocks'].pop(0)
                    slot['nodes'].append({'name'    : nname,
                                          'uid'     : nuid,
                                          'core_map': [cblock],
                                          'gpu_map' : []})
                    slot['ncblocks'] += 1
                else:
                    ok = False
                    break

            while slot['ngblocks'] < ngblocks:
                if node['gblocks']:

                    # move the process onto core `0` (oversubscribed)
                    # enabled)
                    gblock = node['gblocks'].pop(0)
                    slot['nodes'].append({'name'    : nname,
                                          'uid'     : nuid,
                                          'core_map': [[0]],
                                          'gpu_map' : [gblock]})
                    slot['ngblocks'] += 1
                else:
                    ok = False
                    break

            if ok:
                self.free.append(slot)
                slot = next_slot()
                continue

            nidx += 1


        if  slot['ncblocks'] == ncblocks and \
            slot['ngblocks'] == ngblocks :
            self.free.append(slot)

        if not self.free:
            raise RuntimeError('configuration cannot be used for this workload')

        # run this method only once
        self._configured = True


    # --------------------------------------------------------------------------
    #
    def _allocate_slot(self, cud):
        '''
        This is the main method of this implementation, and is triggered when
        a unit needs to be mapped to a set of cores / gpus.
        '''

      # self._log.debug('=> allocate [%d]', len(self.free))
        self._delayed_configure(cud)

        # ensure that all CUDs require the same amount of reources
        for k,v in list(self.chunk.items()):
            if cud[k] != v:
                raise ValueError('hetbre?  %d != %d' % (v, cud[k]))

      # self._log.debug('find new slot')
        slots = self._find_slots(cud)
        if slots:
            self._log.debug('allocate slot %s', slots['nodes'])
        else:
            self._log.debug('allocate slot %s', slots)

      # self._log.debug('<= allocate [%d]', len(self.free))


        return slots


    # --------------------------------------------------------------------------
    #
    def _release_slot(self, slots):
        '''
        This method is called when previously aquired resources are not needed
        anymore.  `slots` are the resource slots as previously returned by
        `_allocate_slots()`.
        '''

      # self._log.debug('=> release  [%d]', len(self.free))
        self._log.debug('release  slot %s', slots['nodes'])
        with self.lock:
            self.free.append(slots)
      # self._log.debug('<= release  [%d]', len(self.free))


    # --------------------------------------------------------------------------
    #
    def _find_slots(self, cores_requested):

        # check if we have free chunks laying around - return one
        with self.lock:
            if self.free:
                return self.free.pop()
            else:
                return None


# ------------------------------------------------------------------------------

