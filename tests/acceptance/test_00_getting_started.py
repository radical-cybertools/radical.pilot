#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__ = 'MIT'

import os
import unittest

# Set-up logging
os.environ['RADICAL_VERBOSE'] = 'ERROR'
os.environ['RADICAL_PILOT_VRBOSE'] = 'ERROR'
os.environ['RADICAL_LOG_TGT'] = '0'

import radical.pilot as rp # noqa
import radical.utils as ru # noqa

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------

#######################################
#            TestProjectUser          #
#######################################


class GettingStarted(unittest.TestCase):
    """Implements the '00_getting_started.py' example in unittest"""

    @classmethod
    def setUpClass(cls):
        """ Getting the resources is slow, to avoid calling it for each
        test use setUpClass() and store the result as class variable
        """
        super(GettingStarted, cls).setUpClass()

        # Set-up the resource, hard-coding 'localhost' for now...
        cls.resource = 'local.localhost'

        # Create a new session. No need to try/except this: if session creation
        # fails, there is not much we can do anyways...
        cls.session = rp.Session()
        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        cls.pmgr = rp.PilotManager(session=cls.session)
        # Create a UnitManager object.
        cls.umgr = rp.UnitManager(session=cls.session)

        # Read in configuration
        cls.config = ru.read_json('%s/config.json' %
                                  os.path.dirname(os.path.abspath(__file__)))

        # Number of Compute Units (CUs)
        cls.n = 128   # number of units to run

    def test_getting_started(self):
        """  """
        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        pd_init = {
            'resource': self.resource,
            'runtime': 15,  # pilot runtime (min)
            'exit_on_error': True,
            'project': self.config[self.resource]['project'],
            'queue': self.config[self.resource]['queue'],
            'access_schema': self.config[self.resource]['schema'],
            'cores': self.config[self.resource]['cores'],
        }
        pdesc = rp.ComputePilotDescription(pd_init)

        # Launch the pilot.
        pilot = self.pmgr.submit_pilots(pdesc)

        self.umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.
        cuds = list()
        for i in range(0, self.n):
            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/date'
            cuds.append(cud)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        self.umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or
        # FAILED).
        self.umgr.wait_units()

        # Verify that 100% of the pilots came back with 'DONE' status
        done_units = 0
        for description in self.umgr.get_units():
            if description.state == "DONE":
                done_units += 1
        self.assertEquals(
            (float(done_units) / float(self.n)), 1.0,
            "Only {0}% of CUs were DONE."
            .format(str((float(done_units) / float(self.n)) * 100.00)))

    @classmethod
    def tearDownClass(cls):
        # Close pilot session
        cls.session.close(cleanup=False)


if __name__ == '__main__':
    unittest.main()
