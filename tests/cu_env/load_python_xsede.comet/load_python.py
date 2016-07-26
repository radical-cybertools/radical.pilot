#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__   = 'MIT'

import os
import sys

os.environ['RADICAL_PILOT_VERBOSE'] = 'REPORT'

import radical.pilot as rp
import radical.utils as ru


# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------


#------------------------------------------------------------------------------
#
if __name__ == '__main__':

	# we use a reporter class for nicer output
	report = ru.LogReporter(name='radical.pilot')
	report.title('Getting Started (RP version %s)' % rp.version)

	resource = 'xsede.comet'

	# Create a new session. No need to try/except this: if session creation
	# fails, there is not much we can do anyways...
	session = rp.Session()

	# all other pilot code is now tried/excepted.  If an exception is caught, we
	# can rely on the session object to exist and be valid, and we can thus tear
	# the whole RP stack down via a 'session.close()' call in the 'finally'
	# clause...
	try:

		report.header('submit pilots')

		# Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
		pmgr = rp.PilotManager(session=session)

		# Define an [n]-core local pilot that runs for [x] minutes
		# Here we use a dict to initialize the description object
		pd_init = {
				'resource'      : resource,
				'cores'         : 24,  # pilot size
				'runtime'       : 15,  # pilot runtime (min)
				'exit_on_error' : True,
				'project'       : "TG-MCB090174",
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

		n = 1   # number of units to run
		report.info('create %d unit description(s)\n\t' % n)

		cuds = list()
		for i in range(0, n):

			# create a new CU description, and fill it.
			# Here we don't use dict initialization.
			cud = rp.ComputeUnitDescription()
			cud.pre_exec = ["module load python"]
			cud.executable = 'which python'
			#cud.arguments = ['Hello', 'World', 'Test' 'Suite', '!']
			cuds.append(cud)
			report.progress()
		report.ok('>>ok\n')

		# Submit the previously created ComputeUnit descriptions to the
		# PilotManager. This will trigger the selected scheduler to start
		# assigning ComputeUnits to the ComputePilots.
		cus = umgr.submit_units(cuds)

		# Wait for all compute units to reach a final state (DONE, CANCELED or FAILED).
		report.header('gather results')
		umgr.wait_units()
	
		# Get output of "module load python" from comet (login node)
		import subprocess
		prog = subprocess.Popen(["ssh", "comet.sdsc.xsede.org", "module load python; which python"], stdout=subprocess.PIPE)
		out = prog.communicate()[0]

		for unit in cus:
			assert str(unit.stdout).rstrip(' \t\n\r') == out.rstrip(' \t\n\r')


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

