#!/usr/bin/env python

__copyright__ = "Copyright 2013-2015, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import radical.pilot as rp
import radical.utils as ru

dh = ru.DebugHelper ()

RUNTIME  =    60
SLEEP    =    30
PILOTS   =     1
UNITS    =     3
SCHED    = rp.SCHED_PILOTDATA
#SCHED    = rp.umgr.scheduler.SCHEDULER_DIRECT

resources = {
    'osg.xsede-virt-clust' : {
        'project'  : 'TG-CCR140028',
        'queue'    : None,
        'schema'   : 'ssh'
    },
    'osg.connect' : {
        'project'  : 'RADICAL',
        'queue'    : None,
        'schema'   : 'ssh'
    }
}
#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    # Hello HTC :-)
    #if state == rp.FAILED:
    #    sys.exit (1)


#------------------------------------------------------------------------------
#
CNT = 0
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        CNT += 1
        print "[Callback]: # %6d" % CNT

    # Hello HTC :-)
    #if state == rp.FAILED:
    #    print "stderr: %s" % unit.stderr
    #    sys.exit(2)


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size: %s." % wait_queue_size


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        resource = sys.argv[1]
    else:
        resource = 'local.localhost'

    print 'running on %s' % resource

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        umgr = rp.UnitManager(session=session, scheduler=SCHED)
        umgr.register_callback(unit_state_cb,      rp.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)

        dpds = []
        #for SE in [
        for SE in [
            #'MIT_CMS',
            #'LUCILLE',
            # Preferred SEs
            'cinvestav',
            "GLOW",
            "SPRACE",
            # "SWT2_CPB", # DEAD
            "Nebraska",
            "UCSDT2",
            "UTA_SWT2"
        ]:
            dpdesc = rp.DataPilotDescription()
            dpdesc.resource = 'osg.%s' % SE
            dpds.append(dpdesc)

        data_pilots = pmgr.submit_data_pilots(dpds)

        duds = []
        for size in [1, 10]:
            dud = rp.DataUnitDescription()
            dud.name = "%dM" % size
            dud.file_urls = ["data/%dM" % size]
            dud.size = size
            # dud.selection = rp.SELECTION_PREFERRED
            dud.selection = rp.SELECTION_FAST
            # dud.selection = rp.SELECTION_RELIABLE
            duds.append(dud)

        data_units = umgr.submit_data_units(duds, data_pilots=data_pilots, existing=True)
        for du in data_units:
            print "data unit: %s available on data pilots: %s" % (du.uid, du.pilot_ids)

        cuds = []
        for unit_count in range(0, UNITS):
            cud = rp.ComputeUnitDescription()
            #cud.name = "Test"
            cud.executable     = "/bin/sh"
            #cud.pre_exec       = ["source /cvmfs/oasis.opensciencegrid.org/osg-software/osg-wn-client/3.2/current/el6-x86_64/setup.sh"]
            #cud.arguments      = ["-c", "echo $HOSTNAME:$OSG_HOSTNAME && sleep %d" % SLEEP]
            #cud.arguments      = ["-c", "cat TESTFILE && rev TESTFILE > OUTPUT"]
            cud.arguments      = ["-c", "cksum *M > OUTPUT"]
            # cud.input_staging  = [
                #"srm://cit-se.ultralight.org:8443/srm/v2/server?SFN=/mnt/hadoop/osg/osg/marksant/data/1M",
                # "srm://cit-se.ultralight.org:8443/srm/v2/server?SFN=/mnt/hadoop/osg/osg/marksant/data/10M",
                # "srm://cit-se.ultralight.org:8443/srm/v2/server?SFN=/mnt/hadoop/osg/osg/marksant/data/100M"
            # ]
            #cud.output_staging  = ["OUTPUT > srm://cit-se.ultralight.org:8443/srm/v2/server?SFN=/mnt/hadoop/osg/osg/marksant/OUTPUT"]
            cud.input_data     = [du.uid for du in data_units]
            cud.cores          = 1
            cuds.append(cud)

        units = umgr.submit_units(cuds)

        cpdesc = rp.ComputePilotDescription()
        cpdesc.resource        = resource
        cpdesc.cores           = 1
        cpdesc.project         = resources[resource]['project']
        cpdesc.queue           = resources[resource]['queue']
        cpdesc.runtime         = RUNTIME
        cpdesc.cleanup         = False
        cpdesc.access_schema   = resources[resource]['schema']
        # cpdesc.candidate_hosts = [
        #                          #'~(HAS_CVMFS_oasis_opensciencegrid_org =?= TRUE)'
        #                          '!CIT_CMS_T2', # Takes too long to bootstrap
        #                          '!FIU_HPCOSG_CE',  # zeromq build fails
        #                          '!FLTECH', # gcc: error trying to exec 'cc1plus': execvp: No such file or directory
        #                          '!GridUNESP_CENTRAL', # On hold immediately.
        #                          '!MIT_CMS', # gcc: error trying to exec 'cc1plus': execvp: No such file or directory
        #                           # '!MWT2', # No ssh
        #                          '!Nebraska',  # zeromq build fails
        #                           # '!NPX', # No ssh
        #                           '!NUMEP-OSG',  # OASIS source failure
        #                           '!OU_OSCER_ATLAS', # /cvmfs/oasis.opensciencegrid.org/osg/modules/lmod/current/init/bash: No such file or directory
        #                           '!SMU_ManeFrame_CE',  # No mongodb connectivity
        #                           # '!SPRACE', # failing
        #                           # '!SMU_HPC', # Failed to start up, no real reason, revisit
        #                           '!SU-OG',  # No compiler
        #                           '!SU-OG-CE',  #
        #                           '!SU-OG-CE1',  #
        #                            '!UCSDT2', # cc not found / Failing because of format character ...
        #                            "!UFlorida-HPC",  # No oasis modules
        #                         ]
        cpdesc.candidate_hosts = [
            # Sites with a preferred sE
            "cinvestav",
            #"CIT_CMS_T2", too long to run
            "Crane", # Other site
            "GLOW",
            #"MIT_CMS", # see above
            #"Nebraska", # zeromq build fails
            # "SPRACE", No cc1plus
            "SWT2_CPB",
            "Tusker", # Other site
            # "UCSDT2",
            "UTA_SWT2"
        ]

        # TODO: bulk submit pilots here
        for p in range(PILOTS):
            pilot = pmgr.submit_pilots(cpdesc)
            umgr.add_pilots(pilot)

        print "all data pilots: %s" % pmgr.list_data_pilots()
        print "all data units: %s" % umgr.list_data_units()

        umgr.wait_units() # timeout=10)

        for cu in units:
            print "* Task %s state %s, exit code: %s, stdout: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.stdout, cu.start_time, cu.stop_time)

      # os.system ("radicalpilot-stats -m stat,plot -s %s > %s.stat" % (session.uid, session_name))


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close (cleanup=False)

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------

