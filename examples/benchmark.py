#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

import radical.pilot as rp
import radical.utils as ru

#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    pilot_size   = int(sys.argv[1])
    unit_num     = int(sys.argv[2])
    unit_size    = int(sys.argv[3])
    walltime     = int(sys.argv[4])

    resource     = 'ornl.titan_orte'
    project      = 'CSC230'
    queue        = 'debug'
    schema       = 'local'


    session  = rp.Session()
    print "session id: %s" % session.uid

    try:
        pmgr  = rp.PilotManager(session=session)
        pd    = {'resource'      : resource,
                 'runtime'       : walltime,
                 'exit_on_error' : True,
                 'project'       : project,
                 'queue'         : queue,
                 'access_schema' : schema,
                 'cores'         : pilot_size + 1*16  # add node for sub-agent
                }
        pilot = pmgr.submit_pilots(rp.ComputePilotDescription(pd))
        umgr  = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        prep  = '/lustre/atlas/world-shared/csc230/rp_benchmark_prep/'
        files = 'md.tpr'.split()
        sds   = list()
        for f in files:
            sds.append({'source': 'file://localhost/%s/%s' % (prep, f),
                        'target': f,
                        'action': rp.LINK})

        cuds = list()
        for i in range(0, unit_num):

            cud = rp.ComputeUnitDescription()
            cud.executable       = 'gmx_mpi'
          # cud.arguments        = 'mdrun -deffnm md -ntomp 4 -pin on -pinoffset 0'.split()
            cud.arguments        = 'mdrun -deffnm md'.split()
            cud.environment      = {'PMI_NO_FORK' : 'True'}
            cud.input_staging    = sds
          # cud.cpu_processes    = unit_size
          # cud.cpu_process_type = rp.MPI
            cud.cores            = unit_size
            cud.mpi              = True
            cuds.append(cud)

        umgr.submit_units(cuds)
        umgr.wait_units()

    except Exception as e:
        print 'exception %s' % repr(e)
        ru.print_exception_trace()
        raise
   
    except (KeyboardInterrupt, SystemExit) as e:
        print 'interrupt %s' % repr(e)
        ru.print_exception_trace()
 
    finally:
        session.close(download=True)


#-------------------------------------------------------------------------------

