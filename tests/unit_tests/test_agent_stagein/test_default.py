# pylint: disable=protected-access, no-value-for-parameter, unused-argument

import glob
import os

import radical.utils as ru

from unittest import TestCase, mock

from radical.pilot.agent.staging_input.default import Default

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class StageInTC(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        cls._test_cases = []
        for f in glob.glob('%s/test_cases/task.*.json' % base):
            c = ru.read_json(f)
            if c.get('results'):
                cls._test_cases.append([c['task'], c['results']])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Default, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_work(self, mocked_init, mocked_raise_on):

        global_things = []
        global_state = []

        # ----------------------------------------------------------------------
        #
        def _advance_side_effect(things, state, publish, push):
            nonlocal global_things
            nonlocal global_state
            global_things.append(things)
            global_state.append(state)

        # ----------------------------------------------------------------------
        #
        def _handle_task_side_effect(task, actionables):
            _advance_side_effect(task, actionables, False, False)

        component = Default(cfg=None, session=None)
        component._handle_task = mock.MagicMock(
                                       side_effect=_handle_task_side_effect)
        component.advance = mock.MagicMock(side_effect=_advance_side_effect)
        component._log = ru.Logger('dummy')

        for test in self._test_cases:
            global_things = []
            global_state = []
            component._work([test[0]])
            self.assertEqual(global_things, test[1][0])
            self.assertEqual(global_state, test[1][1])


if __name__ == '__main__':

    tc = StageInTC()
    tc.test_work()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
