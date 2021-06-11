# pylint: disable=unused-argument, protected-access

__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.executing.base import AgentExecutingComponent
from radical.pilot.agent.executing.popen import Popen


# ------------------------------------------------------------------------------
#
class TestBaseExecuting(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    def test_create(self, mocked_popen_init):

        class NewExecuting(AgentExecutingComponent):
            pass

        with self.assertRaises(TypeError):
            # method `create` is allowed to be called by the base class only
            NewExecuting.create(cfg=None, session=None)

        spawners = [
            {'spawner': 'POPEN'},
            {'spawner': 'UNKNOWN'}
        ]

        for spawner in spawners:
            try:
                AgentExecutingComponent.create(cfg=spawner, session=None)
            except:
                # in case of spawner is not presented in `rpa.executing.base`
                with self.assertRaises(RuntimeError):
                    raise

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentExecutingComponent, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.launch_method.base.LaunchMethod')
    @mock.patch('radical.utils.Logger')
    def test_find_launcher(self, mocked_logger, mocked_lm, mocked_init):

        exec_base = AgentExecutingComponent(cfg=None, session=None)

        exec_base._launch_order = []
        self.assertIsNone(exec_base.find_launcher(task=None))

        lm = mocked_lm.return_value
        lm.can_launch.return_value = True

        exec_base._log          = mocked_logger()
        exec_base._launchers    = {'launcher_name': lm}
        exec_base._launch_order = exec_base._launchers.keys()
        self.assertIs(exec_base.find_launcher(task=None), lm)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentExecutingComponent, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.executing.base.rpa.LaunchMethod')
    def test_initialize(self, mocked_lm, mocked_init):

        ec = AgentExecutingComponent(cfg=None, session=None)
        ec._cfg = ru.Munch(from_dict={
            'sid'             : 'sid.0000',
            'rm_info'         : {'lfs_per_node': {'path': '/tmp'}},
            'resource_sandbox': '',
            'session_sandbox' : '',
            'pilot_sandbox'   : '',
            'resource'        : 'resource_config_label',
            'resource_cfg'    : {'order': [],
                                 'launch_methods': {'SRUN': {}}}
        })
        ec._log               = ec._prof               = mock.Mock()
        ec.work               = ec.command_cb          = mock.Mock()
        ec.register_input     = ec.register_output     = mock.Mock()
        ec.register_publisher = ec.register_subscriber = mock.Mock()

        mocked_lm.create.return_value = mocked_lm
        ec.initialize()

        self.assertEqual(ec._launch_order, ['SRUN'])
        self.assertEqual(ec._launchers['SRUN'], mocked_lm)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestBaseExecuting()
    tc.test_create()
    tc.test_find_launcher()
    tc.test_initialize()


# ------------------------------------------------------------------------------
