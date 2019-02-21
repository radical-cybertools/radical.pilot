#!/usr/bin/env python

import os
import sys
import math
import time
import random

import radical.pilot as rp
import radical.utils as ru



# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tid = sys.argv[1]
    tc  = ru.read_json(tid)
    session = None
    try:
        print 'TEST   %-15s: %s' % (tid, ' '.join(["%s : %-8s" % (str(k), tc[k])
                                    for k in sorted(tc.keys())]))

        session = rp.Session()
        pmgr    = rp.PilotManager(session=session)
        pcores  = 42 * tc['pnodes']
        pd_init = {'resource'      : 'ornl.summit', 
                   'runtime'       : 30,
                   'exit_on_error' : True,
                   'project'       : 'BIP178',
                   'queue'         : 'batch',
                   'access_schema' : 'local',
                   'cores'         : pcores
                  }

        pdesc = rp.ComputePilotDescription(pd_init)
        pilot = pmgr.submit_pilots(pdesc)
        umgr  = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        u_size  = tc['uprocs'] * tc['uthreads']
        n_units = tc.get('nunits')
        if not n_units:
            n_units = int(math.floor(pcores / u_size) * tc['gens'])

        cuds = list()
        for i in range(0, n_units):

            cud = rp.ComputeUnitDescription()
            cud.executable       = '/bin/sleep'
            cud.arguments        = tc['utime']

            uprocs = tc['uprocs']
            if isinstance(uprocs, list):
                uprocs_val = random.randint(uprocs[0], uprocs[1])
            else:
                uprocs_val = uprocs

            cud.cpu_processes    = uprocs_val
            cud.cpu_threads      = tc['uthreads']
            cud.cpu_process_type = tc['uptype']
            cud.cpu_thread_type  = tc['uttype']
            cud.gpu_processes    = tc['ugpus']
            cud.gpu_process_type = tc['uptype']
            cuds.append(cud)

        units = umgr.submit_units(cuds)
        umgr.wait_units()

        states = dict()
        for u in units:
            if u.state not in states: states[u.state]  = 1
            else                    : states[u.state] += 1

        print 'RESULT %-15s: %s' % (tid, ' '.join(["%s : %-8s" % (str(k), states[k])
                                    for k in sorted(states.keys())]))

    finally:
        if session:
            session.close(download=True)


# ------------------------------------------------------------------------------
