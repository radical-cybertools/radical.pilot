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

            def control_cb(self, topic, msg):
                pass

            def cancel_task(self, task):
                pass

            def work(self, tasks):
                pass

        with self.assertRaises(TypeError):
            # method `create` is allowed to be called by the base class only
            NewExecuting.create(cfg=None, session=None)

        spawners = ['POPEN', 'UNKNOWN']


        for spawner in spawners:
            session = ru.Config(cfg={
                'rcfg': {'agent_spawner' : spawner}})
            try:
                AgentExecutingComponent.create(cfg=spawner, session=session)
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

        ec._session     = mock.Mock()
        ec._session.uid = 'sid.0000'
        ec._session.cfg = ru.TypedDict(from_dict={
            'resource'        : 'resource_config_label',
            'resource_sandbox': '',
            'session_sandbox' : '',
            'pilot_sandbox'   : ''
        })
        ec._session.rcfg = ru.TypedDict(from_dict={
            'resource_manager': 'FORK',
            'agent_spawner'   : 'POPEN'})

        ec._term               = mock.Mock()
        ec._log                = mock.Mock()
        ec._prof               = mock.Mock()
        ec.work                = mock.Mock()
        ec.control_cb          = mock.Mock()
        ec.register_input      = mock.Mock()
        ec.register_output     = mock.Mock()
        ec.register_publisher  = mock.Mock()
        ec.register_subscriber = mock.Mock()

        mocked_rm.create.return_value = mocked_rm
        ec.initialize()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestBaseExecuting()
    tc.test_create()
    tc.test_initialize()


# ------------------------------------------------------------------------------
