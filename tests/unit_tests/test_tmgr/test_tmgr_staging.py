# pylint: disable=unused-argument

import glob
import os

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot import states as rps
from radical.pilot.utils import Component

from radical.pilot.tmgr.staging_input.default import Default as StageInDefault
from radical.pilot.tmgr import Input

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TMGRStagingTC(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        cls._test_cases = []
        for f in glob.glob('%s/test_cases/task.*.json' % base):
            cls._test_cases.append(ru.read_json(f))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Component, '__init__', return_value=None)
    def test_si_create(self, mocked_component_init):

        with self.assertRaises(TypeError):
            # only base class can call `create` method
            StageInDefault.create(cfg={}, session=None)

        with self.assertRaises(ValueError):
            # unknown staging component
            Input.create(cfg={'tmgr_staging_input_component': 'unknown'},
                         session=None)

        tmgr_si = Input.create(cfg={'owner': 'test_uid'}, session=None)
        self.assertIsInstance(tmgr_si, StageInDefault)
        self.assertTrue(tmgr_si.uid.startswith('test_uid.staging.input'))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(StageInDefault, '__init__', return_value=None)
    def test_si_work(self, mocked_si_init):

        tmgr_si = StageInDefault(cfg={}, session=None)

        def _mocked_advance(things, state, publish, push):
            nonlocal global_things
            nonlocal global_state
            global_things.append(things)
            global_state.append(state)

        tmgr_si.advance = mock.MagicMock(side_effect=_mocked_advance)

        # test tasks which got failure in `_handle_task`

        def _mocked_handle_task(task, actionables):
            raise Exception('Forced exception')

        tmgr_si._handle_task = mock.MagicMock(side_effect=_mocked_handle_task)

        for tc in self._test_cases:

            global_things = []
            global_state  = []

            if not tc.get('task'):
                continue

            tmgr_si.work(dict(tc['task']))

            for tasks in global_things:
                # there were only one task per call
                self.assertEqual(tasks[0]['control'], 'tmgr')
            # advanced is called 2 times for the provided inputs
            self.assertEqual(len(global_things), 2)
            self.assertEqual(global_state, [rps.TMGR_STAGING_INPUT, rps.FAILED])

# ------------------------------------------------------------------------------

