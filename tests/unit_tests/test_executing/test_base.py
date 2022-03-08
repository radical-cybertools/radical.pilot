#!/usr/bin/env python3

# pylint: disable=unused-argument,protected-access,no-value-for-parameter

__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.executing        import AgentExecutingComponent
from radical.pilot.agent.executing.popen  import Popen
from radical.pilot.agent.resource_manager import ResourceManager


# ------------------------------------------------------------------------------
#
class TestBaseExecuting(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    def test_create(self, mocked_popen_init):

        class NewExecuting(AgentExecutingComponent):

            def command_cb(self, topic, msg):
                pass

            def work(self, tasks):
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
                with self.assertRaises(ValueError):
                    raise

        # Popen was initialized
        self.assertTrue(mocked_popen_init.called)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentExecutingComponent, '__init__', return_value=None)
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    def test_initialize(self, mocked_rm, mocked_init):

        ec = AgentExecutingComponent(cfg=None, session=None)
        ec._cfg = ru.TypedDict(from_dict={
            'sid'             : 'sid.0000',
            'resource_manager': 'FORK',
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

        mocked_rm.create.return_value = mocked_rm
        ec.initialize()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestBaseExecuting()
    tc.test_create()
    tc.test_initialize()


# ------------------------------------------------------------------------------
