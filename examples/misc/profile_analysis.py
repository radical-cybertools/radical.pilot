#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys

import radical.pilot as rp
import radical.utils as ru

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug 
# set if you want to see what happens behind the scences!


RUNTIME  =    20  # how long to run the pilot
CORES    =    64  # how many cores to use for one pilot
UNITS    =   128  # how many units to create
SLEEP    =     0  # how long each unit sleeps
SCHED    = rp.SCHED_DIRECT_SUBMISSION


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we use a reporter class for nicer output
    report = ru.Reporter("Getting Started")

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:
        report.info('read configs')
        resources = ru.read_json('%s/config.json', os.path.dirname(__file__))
        report.ok('\\ok\n')

        report.header('submit pilots')

        # prepare some input files for the compute units
        os.system ('hostname > file1.dat')
        os.system ('date     > file2.dat')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        pdescs = list()
        for resource in sys.argv[1:]:
            pdesc = rp.ComputePilotDescription()
            pdesc.resource      = resource
            pdesc.cores         = CORES
            pdesc.project       = resources[resource]['project']
            pdesc.queue         = resources[resource]['queue']
            pdesc.runtime       = RUNTIME
            pdesc.cleanup       = False
            pdesc.access_schema = resources[resource]['schema']
            pdescs.append(pdesc)

        # Launch the pilot.
        pilots = pmgr.submit_pilots(pdescs)
    
        report.header('submit units')

        session.close (cleanup=False)

    # fetch profiles and convert into inspectable data frames
    if session:
        session_id = session.uid


    : 

    import radical.pilot.utils as rpu
    # we have a session
    profiles   = rpu.fetch_profiles(sid=session_id, tgt='/tmp/')
    profile    = rpu.combine_profiles (profiles)
    frame      = rpu.prof2frame(profile)
    sf, pf, uf = rpu.split_frame(frame)

  # print len(sf)
  # print len(pf)
  # print len(uf)
  # 
  # print sf[0:10]
  # print pf[0:10]
  # print uf[0:10]
    rpu.add_info(uf)
    rpu.add_states(uf)
    adv = uf[uf['event'].isin(['advance'])]
    print len(adv)
  # print uf[uf['uid'] == 'unit.000001']
  # print list(pf['event'])

    rpu.add_frequency(adv, 'f_exe', 0.5, {'state' : 'Executing', 'event' : 'advance'})
    print adv[['time', 'f_exe']].dropna(subset=['f_exe'])

    s_frame, p_frame, u_frame = rpu.get_session_frames(session_id)
    print str(u_frame)

    for f in profiles:
        os.unlink(f)


#-------------------------------------------------------------------------------

