# pylint: disable=protected-access,unused-argument

import threading as mt

from unittest import mock, TestCase

from radical.pilot.pilot_manager import PilotManager


# ------------------------------------------------------------------------------
#
class PMGRTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PilotManager, '__init__', return_value=None)
    @mock.patch.object(PilotManager, 'wait_pilots', return_value=None)
    @mock.patch.object(PilotManager, 'publish', return_value=None)
    def test_cancel_pilots(self, mocked_publish, mocked_wait_pilots, mocked_init):

        pmgr = PilotManager(session=None)
        pmgr._uid         = 'pmgr.0000'
        pmgr._pilots_lock = mt.RLock()
        pmgr._log         = mock.Mock()
        pmgr._session     = mock.Mock()
        pmgr._pilots      = {'pilot.0000': mock.Mock()}

        with self.assertRaises(ValueError):
            pmgr.cancel_pilots('unknown_pilot_uid')

        pmgr.cancel_pilots()

        self.assertTrue(mocked_wait_pilots.called)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PilotManager, '__init__', return_value=None)
    @mock.patch.object(PilotManager, 'publish', return_value=None)
    @mock.patch.object(PilotManager, 'wait_pilots', return_value=None)
    def test_kill_pilots(self, mocked_wait_pilots, mock_publish, mocked_init):

        pmgr = PilotManager(session=None)
        pmgr._uid         = 'pmgr.0001'
        pmgr._pilots_lock = mt.RLock()
        pmgr._log         = mock.Mock()
        pmgr._session     = mock.Mock()
        pmgr._pilots      = {'pilot.0001': mock.Mock()}

        with self.assertRaises(ValueError):
            pmgr.kill_pilots('unknown_pilot_uid')

        pmgr.kill_pilots()

        self.assertTrue(mock_publish.called)
        self.assertTrue(mocked_wait_pilots.called)

        args, kwargs = mock_publish.call_args_list[0]
        self.assertEqual('control_pubsub', args[0])
        self.assertEqual({'cmd': 'kill_pilots',
                          'arg': {'pmgr': 'pmgr.0001',
                                  'uids': ['pilot.0001']}}, args[1])

# ------------------------------------------------------------------------------

