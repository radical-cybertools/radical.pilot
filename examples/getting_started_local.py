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
CORES    =    32  # how many cores to use for one pilot
UNITS    =   128  # how many units to create
SLEEP    =     0  # how long each unit sleeps


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we use a reporter class for nicer output
    report = ru.LogReporter()
    report.title("Getting Started")

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:
        report.info('read configs')
        config = ru.read_json('%s/config.json' % os.path.dirname(__file__))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # prepare some input files for the compute units
        os.system ('hostname > file1.dat')
        os.system ('date     > file2.dat')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        pdescs = list()
        report.info('create pilot descriptions')
        for resource in sys.argv[1:]:
            pd_init = {
                    'resource'      : resource,
                    'cores'         : CORES,
                    'runtime'       : RUNTIME,
                    'project'       : config[resource]['project'],
                    'queue'         : config[resource]['queue'],
                    'access_schema' : config[resource]['schema']
                    }
            pdescs.append(rp.ComputePilotDescription(pd_init))
        report.ok('>>ok\n')

        # Launch the pilot.
        pilots = pmgr.submit_pilots(pdescs)

        # use different schedulers, depending on number of pilots
        report.info('select scheduler')
        if len(pilots) == 1: SCHED = rp.SCHED_DIRECT
        else               : SCHED = rp.SCHED_BACKFILLING
        report.ok('>>%s\n' % SCHED)
    
        report.info('stage data to pilot')
        input_sd_pilot = {
                'source': 'file:///etc/passwd',
                'target': 'staging:///f1',
                'action': rp.TRANSFER
                }
        for pilot in pilots:
            pilot.stage_in (input_sd_pilot)
            report.progress()
        report.ok('>>ok\n')

        report.header('submit units')

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(session=session, scheduler=SCHED)
        umgr.add_pilots(pilots)

      # input_sd_umgr   = {'source':'/etc/group',        'target': 'f2',                'action': rp.TRANSFER}
      # input_sd_agent  = {'source':'staging:///f1',     'target': 'f1',                'action': rp.COPY}
      # output_sd_agent = {'source':'f1',                'target': 'staging:///f1.bak', 'action': rp.COPY}
      # output_sd_umgr  = {'source':'f2',                'target': 'f2.bak',            'action': rp.TRANSFER}

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
        report.info('create %d unit description(s)\n\t' % UNITS)
        cuds = list()
        for i in range(0, UNITS):
            cud = rp.ComputeUnitDescription()
            cud.name          = "unit_%03d" % i
            if i == 10:
                cud.executable    = "/bin/data"
            else:
                cud.executable    = "/bin/date"
            cud.arguments     = ["-u"]
            cud.pre_exec      = ["sleep a"]
            cud.post_exec     = ["sleep 1"]
            cud.cores         = 1
          # cud.input_staging  = [ input_sd_umgr,  input_sd_agent]
          # cud.output_staging = [output_sd_umgr, output_sd_agent]
            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

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
                report.plain("  * %s: %s, exit code: %3s, stdout: %s" \
                        % (unit.uid, unit.state[:4], 
                            unit.exit_code, unit.stdout.strip()[:26]))
                report.ok(">>ok\n")
            else:
                report.plain("  * %s: %s, exit code: %3s, stderr: %s" \
                        % (unit.uid, unit.state[:4], 
                           unit.exit_code, unit.stderr.strip()[-26:]))
                report.error(">>err\n")
    
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
        # not.  This will kill all remaining pilots, but leave the database
        # entries alone.
        report.header('finalize')
        session.close (terminate=True, cleanup=False)

    report.header()


#-------------------------------------------------------------------------------

