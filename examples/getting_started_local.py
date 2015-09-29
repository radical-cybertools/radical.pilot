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


UNITS = 10  # how many units to create


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

        report.header('submit pilots')

        # prepare some input files for the compute units
        os.system ('hostname > file1.dat')
        os.system ('date     > file2.dat')
    
        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)
    
        # Define a 4-core local pilot that runs for 10 minutes and cleans up
        # after itself.
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "local.localhost"
        pdesc.runtime  =  10 # minutes
        pdesc.cores    =   8 # number of cores to use 
                             # (make sure you have that many!)
        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)
    
        report.header('submit units')

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(session=session, scheduler=rp.SCHED_DIRECT)
        umgr.add_pilots(pilot)
    
        # Create a workload of ComputeUnits (tasks). Each compute unit
        # uses /bin/cat to concatenate two input files, file1.dat and
        # file2.dat. The output is written to STDOUT. cu.environment is
        # used to demonstrate how to set environment variables within a
        # ComputeUnit - it's not strictly necessary for this example. As
        # a shell script, the ComputeUnits would look something like this:
        #
        #    export INPUT1=file1.dat
        #    export INPUT2=file2.dat
        #    /bin/cat $INPUT1 $INPUT2
        #
        cuds = []
        report.info('create %d unit description(s)' % UNITS)
        for unit_count in range(0, UNITS):
            cud = rp.ComputeUnitDescription()
            cud.name          = "unit_%03d" % unit_count
            cud.executable    = "/bin/date"
            cud.pre_exec      = ["sleep 1"]
            cud.post_exec     = ["sleep 1"]
          # cud.arguments     = ["1"]
            cud.cores         = 1
            cuds.append(cud)
            report.progress('.')
        report.ok('\\ok\n')
    
        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)
    
        report.header('gather results')

        # Wait for all compute units to reach a terminal state (DONE or FAILED).
        umgr.wait_units()
    
        report.info('\n')
        for unit in units:
            if unit.state == rp.DONE:
                report.ok("  * %s: %s, exit code: %s, stdout: %s\n" \
                % (unit.uid, unit.state, unit.exit_code, unit.stdout.strip()))
            else:
                report.error("  * %s: %s, exit code: %s, stderr: %s\n" \
                % (unit.uid, unit.state, unit.exit_code, unit.stderr.strip()))
    
        # delete the test data files
        os.system ('rm file1.dat')
        os.system ('rm file2.dat')


    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error("caught Exception: %s\n" % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn("need to exit now: %s\n" % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will both clean out the session's database record, and kill
        # all remaining pilots.
        report.header('finalize')
        session.close ()

    report.header()


#-------------------------------------------------------------------------------

