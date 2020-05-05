# pylint: disable=protected-access, unused-argument
import glob
import radical.utils as ru
from radical.pilot.agent.staging_input.default import Default
from unittest import TestCase, mock


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
    @mock.patch('radical.utils.raise_on')
    def test_work(self, mocked_init, mocked_raise_on):

        global_things = []
        global_state = []
        # ------------------------------------------------------------------------------
        #
        def _advance_side_effect(things, state, publish, push):
            nonlocal global_things
            nonlocal global_state
            global_things.append(things)
            global_state.append(state)


        # ------------------------------------------------------------------------------
        #
        def _handle_unit_side_effect(unit, actionables):
            _advance_side_effect(unit, actionables, False, False)


        tests = self.setUp()
        component = Default(cfg=None, session=None)
        component._handle_unit = mock.MagicMock(side_effect=_handle_unit_side_effect)
        component.advance = mock.MagicMock(side_effect=_advance_side_effect)
        component._log = ru.Logger('dummy')

        for test in tests:
            global_things = []
            global_state = []
            component._work([test[0]])
            self.assertEqual(global_things, test[1][0])
            self.assertEqual(global_state, test[1][1])
