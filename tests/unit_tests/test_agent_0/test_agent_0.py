# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os
import tempfile

from unittest import mock, TestCase

from radical.pilot                        import TaskDescription
from radical.pilot.agent.resource_manager import RMInfo

from radical.pilot.agent                  import Agent_0


# ------------------------------------------------------------------------------
#
class TestComponent(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    def test_check_control(self, mocked_init):

        global_control = []

        def _publish_side_effect(publish_type, cmd):
            nonlocal global_control
            global_control.append((publish_type, cmd))

        def _prepare_env_side_effect(env_id, spec):
            return (env_id, spec)

        agent_cmp = Agent_0(None, None)
        agent_cmp._log = mock.Mock()
        agent_cmp.publish = mock.MagicMock(side_effect=_publish_side_effect)
        agent_cmp._prepare_env = mock.MagicMock(
                                           side_effect=_prepare_env_side_effect)

        msg = {'cmd': 'test',
               'arg': {'uid': 'rpc.0000',
                       'rpc': 'bye'}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control, [])

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0000',
                       'rpc': 'bye'}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control, [])

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0000',
                       'rpc': 'hello'}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertIn(global_control[0], [('control_pubsub',
                                           {'cmd': 'rpc_res',
                                            'arg': {'uid': 'rpc.0000',
                                                    'err': "KeyError('arg')",
                                                    'out': None,
                                                    'ret': 1}
                                           }),
                                          ('control_pubsub',
                                           {'cmd': 'rpc_res',
                                            'arg': {'uid': 'rpc.0000',
                                                    'err': "KeyError('arg',)",
                                                    'out': None,
                                                    'ret': 1}
                                           })])

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0000',
                       'rpc': 'hello',
                       'arg': ['World']}
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control[1], ('control_pubsub',
                                             {'cmd': 'rpc_res',
                                              'arg': {'uid': 'rpc.0000',
                                                      'err': None,
                                                      'out': 'hello World',
                                                      'ret': 0}
                                             }))

        msg = {'cmd': 'rpc_req',
               'arg': {'uid': 'rpc.0000',
                       'rpc': 'prepare_env',
                       'arg': {'env_name': 'radical',
                               'env_spec': 'spec'}
                      }
              }
        self.assertTrue(agent_cmp._check_control(None, msg))
        self.assertEqual(global_control[2], ('control_pubsub',
                                             {'cmd': 'rpc_res',
                                              'arg': {'uid': 'rpc.0000',
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

        agent_0 = Agent_0(None, None)
        agent_0._pwd = tempfile.gettempdir()
        agent_0._log = mock.Mock()
        agent_0._cfg = {
            'agents': {
                'agent_1': {'target'    : 'node',
                            'components': {'agent_executing': {'count': 1}}}
            }
        }

        agent_0._rm = mock.Mock()

        # number of available agent nodes less than number of agents in config
        agent_0._rm.info = RMInfo({'agent_node_list': []})
        with self.assertRaises(AssertionError):
            agent_0._start_sub_agents()

        agent_0._rm.info = RMInfo({
            'agent_node_list' : [{'node_id': '1', 'node_name': 'n.0000'}],
            'cores_per_node'  : 10,
            'threads_per_core': 1})

        # no launcher for agent task(s)
        agent_0._rm.find_launcher.return_value = None
        with self.assertRaises(RuntimeError):
            agent_0._start_sub_agents()

        def check_agent_task(agent_task, *args, **kwargs):
            agent_td = TaskDescription(agent_task['description'])
            # ensure that task description is correct
            agent_td.verify()
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

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestComponent()
    tc.test_check_control()
    tc.test_start_sub_agents()


# ------------------------------------------------------------------------------
