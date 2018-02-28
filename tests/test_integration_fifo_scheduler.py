#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

verbose  = os.environ.get('RADICAL_PILOT_VERBOSE', 'REPORT')
os.environ['RADICAL_PILOT_VERBOSE'] = verbose

import radical.pilot as rp
import radical.utils as ru
from random import randint, shuffle

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    # we use a reporter class for nicer output
    report = ru.LogReporter(name='radical.pilot', level=verbose)
    report.title('Getting Started (RP version %s)' % rp.version)

    # use the resource specified as argument, fall back to localhost
    if   len(sys.argv)  > 2: report.exit('Usage:\t%s [resource]\n\n' % sys.argv[0])
    elif len(sys.argv) == 2: resource = sys.argv[1]
    else                   : resource = 'local.localhost'

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        # read the config used for resource details
        report.info('read config')
        config = ru.read_json('%s/config.json' % os.path.dirname(os.path.abspath(__file__)))
        report.ok('>>ok\n')

        report.header('submit pilots')

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
                'resource'      : resource,
                'runtime'       : 15,  # pilot runtime (min)
                'exit_on_error' : True,
                'project'       : config[resource]['project'],
                'queue'         : config[resource]['queue'],
                'access_schema' : config[resource]['schema'],
                'cores'         : 2,
                }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = pmgr.submit_pilots(pdesc)


        report.header('submit units')

        # Register the ComputePilot in a UnitManager object.
        umgr = rp.UnitManager(session=session)
        umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.

        n = 128  # number of units to run
        report.info('create %d unit description(s)\n\t' % n)

        cuds = list()
        for i in range(0, n):

            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.name = i
            cud.executable = '/bin/date'
            cud.cores = randint(1,2)        #Heterogeneous cuds
            cuds.append(cud)
            report.progress()
        report.ok('>>ok\n')

        shuffle(cuds)       # inplace

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
        report.header('gather results')
        umgr.wait_units()
    
        report.info('\n')

        # Test if cuds and cus are in the same order. If this is true,
        # the time reported by the cus (in their output) should be in 
        # ascending order. 
        # This test assumes that the unit launched by the FIFO scheduler
        # actually executes first. No system delays.
        assert [unit.name for unit in units] == [cud.name for cud in cuds]
        times = [unit.stdout.strip()[:35] for unit in units]
        assert times == sorted(times)


        #for unit in units:
            #report.plain('  * %s: %s, exit: %3s, out: %s\n' \
            #        % (unit.uid, unit.state[:4],
            #            unit.exit_code, unit.stdout.strip()[:35]))
            
            

        # get some more details for one unit:
        #unit_dict = units[0].as_dict()
        #report.plain("unit workdir : %s\n" % unit_dict['unit_sandbox'])
        #report.plain("pilot id     : %s\n" % unit_dict['pilot'])
        #report.plain("exit code    : %s\n" % unit_dict['exit_code'])
    

    except Exception as e:
        # Something unexpected happened in the pilot code above
        report.error('caught Exception: %s\n' % e)
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        report.warn('exit requested\n')

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.  This will kill all remaining pilots.
        report.header('finalize')
        session.close()

    report.header()


#-------------------------------------------------------------------------------

