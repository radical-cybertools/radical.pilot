#!/usr/bin/env python

__copyright__ = 'Copyright 2013-2014, http://radical.rutgers.edu'
__license__ = 'MIT'

import os
import unittest
import radical.pilot as rp  # noqa
import radical.utils as ru  # noqa

# ------------------------------------------------------------------------------
#
# READ the RADICAL-Pilot documentation: http://radicalpilot.readthedocs.org/
#
# ------------------------------------------------------------------------------

#######################################
#            TestProjectUser          #
#######################################


class AcceptanceTests(unittest.TestCase):
    """Implements the '00_getting_started.py' example in unittest"""

    @classmethod
    def setUpClass(cls):
        """Initialize tests, just creates instance variables needed."""
        super(AcceptanceTests, cls).setUpClass()

        # Set-up the resource, hard-coding 'localhost' for now...
        cls.resource = None

        # Create a new session. No need to try/except this: if session creation
        # fails, there is not much we can do anyways...
        cls.session = None
        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        cls.pmgr = None
        # Create a UnitManager object.
        cls.umgr = None

        # Read in configuration
        cls.config = ru.read_json('%s/config.json' %
                                  os.path.dirname(os.path.abspath(__file__)))

        # Number of Compute Units (CUs)
        cls.n = 128   # number of units to run

    def setUp(self):
        """ Getting the resources is slow, to avoid calling it for each
        test use setUpClass() and store the result as class variable
        """
        # Set-up the resource, hard-coding 'localhost' for now...
        self.resource = os.getenv(
            'RADICAL_PILOT_RESOURCE',
            default='local.localhost'
        )
        # Create a new session. No need to try/except this: if session creation
        # fails, there is not much we can do anyways...
        self.session = rp.Session()
        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        self.pmgr = rp.PilotManager(session=self.session)
        # Create a UnitManager object.
        self.umgr = rp.UnitManager(session=self.session)

        # Define an [n]-core local pilot that runs for [x] minutes
        # Here we use a dict to initialize the description object
        self.pd_init = {
            'resource': self.resource,
            'runtime': 15,  # pilot runtime (min)
            'exit_on_error': True,
            'project': self.config[self.resource]['project'],
            'queue': self.config[self.resource]['queue'],
            'access_schema': self.config[self.resource]['schema'],
            'cores': self.config[self.resource]['cores'],
        }

    def test_00_getting_started(self):
        """Test a standard pilot run"""

        # Create description object from template description
        pilot_desc = rp.ComputePilotDescription(self.pd_init)

        # Launch the pilot.
        pilot = self.pmgr.submit_pilots(pilot_desc)

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
        units = self.umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or
        # FAILED).
        self.umgr.wait_units()

        # Verify that 100% of the units came back with 'DONE' status
        done_units = 0
        for description in units:
            if description.state == "DONE":
                done_units += 1
        self.assertEquals(
            (float(done_units) / float(self.n)), 1.0,
            "Only {0}% of CUs were DONE."
            .format(str((float(done_units) / float(self.n)) * 100.00)))

    def test_01_unit_details(self):
        """Test unit details, units has all details accessible via api
        """

        # Detail keys to be checked in unit dictionary
        expected_detail_keys = [
            'type',
            'umgr',
            'uid',
            'name',
            'state',
            'exit_code',
            'stdout',
            'stderr',
            'pilot',
            'sandbox',
            'description',
        ]

        # Create description object from template description
        pilot_desc = rp.ComputePilotDescription(self.pd_init)

        # Launch the pilot.
        pilot = self.pmgr.submit_pilots(pilot_desc)

        self.umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.
        cuds = list()
        for i in range(1, self.n + 1):
            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            cud.executable = '/bin/date'
            cuds.append(cud)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = self.umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or
        # FAILED).
        self.umgr.wait_units()

        # Not asserting for 100% completion, that is not the idea here...

        # Check that all items in the dictionary
        # match the expected keys and that all
        # values are *not NONE*
        for unit in units:
            unit_dict = unit.as_dict()
            for key, val in unit_dict.iteritems():
                self.assertIn(key, expected_detail_keys)
                self.assertIsNotNone(
                    val, msg="'{0}' unexpectedly None".format(key))

    def test_02_failing_units(self):
        """Test failing units, about ~50% of the units will fail"""

        # Create description object from template description
        pilot_desc = rp.ComputePilotDescription(self.pd_init)

        # Launch the pilot.
        pilot = self.pmgr.submit_pilots(pilot_desc)

        self.umgr.add_pilots(pilot)

        # Create a workload of ComputeUnits.
        # Each compute unit runs '/bin/date'.
        # About ~50% of them will fail
        cuds = list()
        for i in range(1, self.n + 1):
            # create a new CU description, and fill it.
            # Here we don't use dict initialization.
            cud = rp.ComputeUnitDescription()
            if i % 2:
                cud.executable = '/bin/date'
            else:
                # trigger an error now and then
                cud.executable = '/bin/data'  # does not exist
            cuds.append(cud)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        units = self.umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or
        # FAILED).
        self.umgr.wait_units()

        # Verify that >= 50% of the units came back with 'DONE' status
        # TODO: better checks for failures...
        done_units = 0
        for description in units:
            if description.state == "DONE":
                done_units += 1
        self.assertGreaterEqual(
            (float(done_units) / float(self.n)), 0.50,
            "Only {0}% of CUs were DONE."
            .format(str((float(done_units) / float(self.n)) * 100.00)))

    def test_03_multiple_pilots(self):
        """Test multiple pilots"""

        # Have to hard-code list of resources
        # TODO: get real list of resources
        resources = ['local.localhost']

        # Create multiple pilot descriptions, one for each resource
        pilot_descriptions = list()
        resource_count = len(resources)
        for resource in resources:
            pd_init = {
                'resource': resource,
                'runtime': 15,  # pilot runtime (min)
                'exit_on_error': True,
                'project': self.config[resource]['project'],
                'queue': self.config[resource]['queue'],
                'access_schema': self.config[resource]['schema'],
                'cores': self.config[resource]['cores'],
            }
            pilot_descriptions.append(rp.ComputePilotDescription(pd_init))

        # Launch the pilot.
        pilot = self.pmgr.submit_pilots(pilot_descriptions)
        pilot_count = len(pilot)

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
        units = self.umgr.submit_units(cuds)

        # Wait for all compute units to reach a final state (DONE, CANCELED or
        # FAILED).
        self.umgr.wait_units()

        # Verify that 100% of the units came back with 'DONE' status
        done_units = 0
        for description in units:
            if description.state == "DONE":
                done_units += 1
        self.assertEquals(
            (float(done_units) / float(self.n)), 1.0,
            "Only {0}% of CUs were DONE."
            .format(str((float(done_units) / float(self.n)) * 100.00)))

        # Finally assert that the number of requested vs submitted pilots are
        # the same
        self.assertEquals(resource_count, pilot_count)

    def tearDown(self):
        # Close pilot session
        self.session.close(cleanup=True)


if __name__ == '__main__':
    unittest.main(verbosity=2, failfast=True, catchbreak=True)
