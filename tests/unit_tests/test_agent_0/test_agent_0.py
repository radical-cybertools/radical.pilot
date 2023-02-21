# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os
import tempfile
import threading

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot                        import TaskDescription
from radical.pilot.agent                  import Agent_0
from radical.pilot.agent.resource_manager import RMInfo


# ------------------------------------------------------------------------------
#
class TestComponent(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    def test_check_control(self, mocked_init):

        global_control = []

        def _publish_effect(publish_type, cmd):
            nonlocal global_control
            global_control.append((publish_type, cmd))

        def _prepenv_effect(env_id, spec):
            return (env_id, spec)

        agent_cmp = Agent_0(ru.Config(), None)

        agent_cmp._log         = mock.Mock()
        agent_cmp.publish      = mock.MagicMock(side_effect=_publish_effect)
        agent_cmp._prepare_env = mock.MagicMock(side_effect=_prepenv_effect)

        msg = {'cmd': 'test',
               'arg': {'uid': 'rpc.0000',
                       'rpc': 'bye'}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control, [])

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0001',
                       'rpc': 'bye'}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control, [])

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0002',
                       'rpc': 'hello'}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertIn(global_control[0], [('control_pubsub',
                                           {'cmd': 'rpc_res',
                                            'arg': {'uid': 'rpc.0002',
                                                    'err': "KeyError('arg')",
                                                    'out': None,
                                                    'ret': 1}
                                           }),
                                          ('control_pubsub',
                                           {'cmd': 'rpc_res',
                                            'arg': {'uid': 'rpc.0002',
                                                    'err': "KeyError('arg',)",
                                                    'out': None,
                                                    'ret': 1}
                                           })])

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0003',
                       'rpc': 'hello',
                       'arg': ['World']}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control[1], ('control_pubsub',
                                             {'cmd': 'rpc_res',
                                              'arg': {'uid': 'rpc.0003',
                                                      'err': None,
                                                      'out': 'hello World',
                                                      'ret': 0}
                                             }))

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0004',
                       'rpc': 'prepare_env',
                       'arg': {'env_name': 'radical',
                               'env_spec': 'spec'}
                      }
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control[2], ('control_pubsub',
                                             {'cmd': 'rpc_res',
                                              'arg': {'uid': 'rpc.0004',
                                                      'err': None,
                                                      'out': ('radical',
                                                              'spec'),
                                                      'ret': 0}
                                             }))


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    @mock.patch('radical.utils.env_prep')
    @mock.patch('radical.utils.sh_callout_bg')
    def test_start_sub_agents(self, mocked_run_sh_callout, mocked_ru_env_prep,
                              mocked_init):

        agent_0 = Agent_0(ru.Config(), None)
        agent_0._pwd = tempfile.gettempdir()
        agent_0._log = mock.Mock()
        agent_0._cfg = ru.Config(from_dict={
            'agents': {
                'agent_1': {'target'    : 'node',
                            'components': {'agent_executing': {'count': 1}}}
            }
        })

        agent_0._rm = mock.Mock()

        # number of available agent nodes less than number of agents in config
        agent_0._rm.info = RMInfo({'agent_node_list': []})
        with self.assertRaises(AssertionError):
            agent_0._start_sub_agents()

        agent_0._rm.info = RMInfo({
            'agent_node_list' : [{'node_id': '1', 'node_name': 'n.0000'}],
            'cores_per_node'  : 10,
            'threads_per_core': 2})

        # no launcher for agent task(s)
        agent_0._rm.find_launcher.return_value = None
        with self.assertRaises(RuntimeError):
            agent_0._start_sub_agents()

        def check_agent_task(agent_task, *args, **kwargs):
            agent_td = TaskDescription(agent_task['description'])
            # ensure that task description is correct
            agent_td.verify()
            # check "cores_per_rank" attribute
            self.assertEqual(agent_td.cores_per_rank,
                             agent_0._rm.info.cores_per_node)
            return ''

        launcher = mock.Mock()
        launcher.get_launcher_env.return_value = []
        launcher.get_launch_cmds = check_agent_task
        agent_0._rm.find_launcher.return_value = launcher

        agent_files = glob.glob('%s/agent_1.*.sh' % agent_0._pwd)
        self.assertEqual(len(agent_files), 0)

        agent_0._start_sub_agents()

        agent_files = glob.glob('%s/agent_1.*.sh' % agent_0._pwd)
        self.assertEqual(len(agent_files), 2)
        for agent_file in agent_files:
            os.unlink(agent_file)

        # incorrect config setup for agent ('target' is in ['local', 'node'])
        agent_0._cfg['agents']['agent_1']['target'] = 'incorrect_target'
        with self.assertRaises(ValueError):
            agent_0._start_sub_agents()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    def test_start_services(self, mocked_init):

        advanced_services = list()

        def local_advance(services, publish, push):

            nonlocal advanced_services
            advanced_services = services

        agent_0 = Agent_0()
        agent_0.advance = local_advance
        agent_0._log = mock.Mock()
        agent_0._service_task_ids = list()

        agent_0.services_event = mock.Mock()
        test_data = [dict({'cores_per_rank': '3', 'executable':'/bin/ls'})]
        agent_0._cfg = ru.Config(from_dict={'pid':12, 'pilot_sandbox':'/'})
        agent_0._cfg.services = test_data

        agent_0.services_event.wait = mock.Mock(return_value=True)
        agent_0._start_services()
        self.assertTrue(advanced_services[0]['uid'].startswith('service.'))
        self.assertTrue(test_data[0].items() <=
                        advanced_services[0]['description'].items())

        agent_0.services_event.wait = mock.Mock(return_value=False)
        with self.assertRaises(RuntimeError):
            agent_0._start_services()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    def test_state_cb_of_services(self, mocked_init):

        agent_0 = Agent_0(ru.Config(), None)
        agent_0._service_task_ids = ['101','102']
        agent_0._running_services = []

        agent_0._log = mock.Mock()

        agent_0.services_event = threading.Event()

        topic = 'test_topic'
        msg   = {'cmd': 'update',
                 'arg': []}

        agent_0._state_cb_of_services(topic, msg)
        msg['arg'].append({'uid'  : '101',
                          'state': 'AGENT_EXECUTING'})
        agent_0._state_cb_of_services(topic, msg)

        msg['arg'].append({'uid': '102',
                            'state': 'AGENT_EXECUTING'})
        agent_0._state_cb_of_services(topic, msg)
        self.assertTrue(agent_0.services_event.is_set())


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestComponent()
    tc.test_check_control()
    tc.test_start_sub_agents()
    tc.test_start_services()


# ------------------------------------------------------------------------------
