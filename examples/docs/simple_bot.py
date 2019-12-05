#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import radical.pilot as rp
import radical.utils as ru

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

""" DESCRIPTION: Tutorial 1: A Simple Workload consisting of a Bag-of-Tasks
"""

# READ: The RADICAL-Pilot documentation:
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if
# you want to see what happens behind the scences!


#
if __name__ == "__main__":

    RESOURCE_LABEL = 'local.localhost'
    PILOT_CORES    =  2
    BAG_SIZE       = 10  # The number of units
    CU_CORES       =  1  # The cores each CU will take.
    QUEUE          = None
    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Getting Started (RP version %s)' % rp.version)

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' in the 'finally' clause.
    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        #
        # Change the resource below if you want to run on a remote resource.
        # You also might have to set the 'project' to your allocation ID if
        # your remote resource does compute time accounting.
        #
        # A list of preconfigured resources can be found at:
        # http://radicalpilot.readthedocs.org/en/latest/ \
        #        machconf.html#preconfigured-resources
        #
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = RESOURCE_LABEL  # this is a "label", not a hostname
        pdesc.cores    =  PILOT_CORES
        pdesc.runtime  = 30    # minutes
        pdesc.cleanup  = True  # clean pilot sandbox and database entries
        pdesc.queue = QUEUE

        # submit the pilot.
        report.header("Submitting Compute Pilot to Pilot Manager ...")
        pilot = pmgr.submit_pilots(pdesc)

        # create a UnitManager which schedules ComputeUnits over pilots.
        report.header("Initializing Unit Manager ...")
        umgr = rp.UnitManager (session=session)


        # Add the created ComputePilot to the UnitManager.
        report.ok('>>ok\n')

        umgr.add_pilots(pilot)

        report.info('Create %d Unit Description(s)\n\t' % BAG_SIZE)

        # create CU descriptions
        cudesc_list = []
        for i in range(BAG_SIZE):

            # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.executable  = "/bin/echo"
            cudesc.arguments   = ['I am CU number $CU_NO']
            cudesc.environment = {'CU_NO': i}
            cudesc.cores       = 1
            # -------- END USER DEFINED CU DESCRIPTION --------- #

            cudesc_list.append(cudesc)
            report.progress()
        report.ok('>>>ok\n')
        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        report.header("Submit Compute Units to Unit Manager ...")
        cu_set = umgr.submit_units (cudesc_list)

        report.header("Waiting for CUs to complete ...")
        umgr.wait_units()

        for unit in cu_set:
            print("* CU %s, state %s, exit code: %s, stdout: %s"
                % (unit.uid, unit.state, unit.exit_code, unit.stdout.strip()))


    except Exception as e:
        # Something unexpected happened in the pilot code above
        print("caught Exception: %s" % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print("need to exit now: %s" % e)

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        report.header("closing session")
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots.
    report.header()


# ------------------------------------------------------------------------------

