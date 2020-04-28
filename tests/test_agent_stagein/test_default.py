# pylint: disable=protected-access, unused-argument
import os
import glob
import radical.utils as ru
from radical.pilot.agent.staging_input.default import Default
from unittest import TestCase, mock

os.environ['RP_UNIT_TESTING'] = 'TRUE'


class TestDefault(TestCase):

    # ------------------------------------------------------------------------------
    # 
    def setUp(self):
        ret = list()
        for fin in glob.glob('tests/test_agent_stagein/test_cases/unit.*.json'):
            tc     = ru.read_json(fin)
            unit   = tc['unit'   ]
            result = tc['results']
            if result:
                ret.append([unit, result])

        return ret


    # ------------------------------------------------------------------------------
    #
    def tearDown(self):

        pass

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Default, '__init__',   return_value=None)
    @mock.patch.object(Default, '_handle_unit', return_value=None)
    @mock.patch.object(Default, 'advance', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_work(self, mocked_init, mocked_raise_on, mocked_handle_unit,
                  mocked_advance):

        tests = self.setUp()
        component = Default(cfg=None, session=None)
        component._log = ru.Logger('dummy')

        for test in tests:
            staging_units, no_staging_units = component._work([test[0]])
            self.assertEqual(staging_units, test[1][0])
            self.assertEqual(no_staging_units, test[1][1])
