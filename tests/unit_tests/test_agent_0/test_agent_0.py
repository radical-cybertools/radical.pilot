# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os
import shutil
import tempfile

import threading as mt

from unittest import mock, TestCase

from radical.pilot.messages import RPCRequestMessage, RPCResultMessage

import radical.utils as ru
import radical.pilot as rp

from radical.pilot.agent                  import Agent_0
from radical.pilot.agent.resource_manager import RMInfo


# ------------------------------------------------------------------------------
#
class TestComponent(TestCase):

    _cleanup_files = []

    def _init_primary_side_effect(self):

        self._log  = mock.MagicMock()
        self._prof = mock.MagicMock()
        self._rep  = mock.MagicMock()
        self._reg  = mock.MagicMock()


    # --------------------------------------------------------------------------
    #
    @classmethod
    @mock.patch.object(rp.Session, '_init_primary',
                       side_effect=_init_primary_side_effect,
                       autospec=True)
    def setUpClass(cls, *args, **kwargs) -> None:

        cls._session = rp.Session()
        cls._cleanup_files.append(cls._session.uid)

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls) -> None:

        for p in cls._cleanup_files:
            if p is None:
                continue
            for f in glob.glob(p):
                if os.path.isdir(f):
                    try:
                        shutil.rmtree(f)
                    except OSError as e:
                        print('[ERROR] %s - %s' % (e.filename, e.strerror))
                else:
                    os.unlink(f)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    def test_check_control_cb(self, mocked_init):

        global_control = []

        def _publish_effect(publish_type, cmd):
            nonlocal global_control
            global_control.append((publish_type, cmd))

        def _prepenv_effect(env_name, env_spec):
            return env_name, env_spec

        agent_cmp = Agent_0()

        agent_cmp._log         = mock.Mock()
        agent_cmp._prof        = mock.Mock()
        agent_cmp._pid         = 'pilot.0000'
        agent_cmp.publish      = mock.MagicMock(side_effect=_publish_effect)
        agent_cmp._prepare_env = mock.MagicMock(side_effect=_prepenv_effect)

        agent_cmp._rpc_handlers = {'prepare_env': (agent_cmp._prepare_env, None)}

        msg = {'cmd': 'test',
               'arg': {'uid': 'rpc.0000',
                       'rpc': 'bye'}
              }
        self.assertIsNone(agent_cmp._control_cb(None, msg))
        self.assertEqual(global_control, [])

        msg = RPCRequestMessage({'cmd': 'bye', 'kwargs': {'uid': 'rpc.0001'}})
        self.assertIsNone(agent_cmp._control_cb(None, msg))
        self.assertEqual(global_control, [])

        msg = RPCRequestMessage({'cmd'   : 'prepare_env',
                                 'uid'   : 'rpc.0004',
                                 'kwargs': {'env_name': 'radical',
                                            'env_spec': 'spec'}})
        self.assertIsNone(agent_cmp._control_cb(None, msg))
        self.assertEqual(global_control[0],
                         ('control_pubsub',
                          RPCResultMessage({'uid': 'rpc.0004',
                                            'val': ('radical', 'spec')})))


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    @mock.patch('radical.utils.env_prep')
    @mock.patch('radical.utils.sh_callout_bg')
    def test_start_sub_agents(self, mocked_run_sh_callout, mocked_ru_env_prep,
                                    mocked_init):

        agent_0 = Agent_0()

        agent_0._pwd = tempfile.gettempdir()
        agent_0._log = mock.Mock()
        agent_0._sid = 'rp.session.0'

        agent_0._session     = mock.Mock()
        agent_0._session.cfg = ru.Config(from_dict={
            'agents': {
                'agent_1': {'target'    : 'node',
                            'components': {'agent_executing': {'count': 1}}}
            },
            'reg_addr': 'tcp://location'
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
            agent_td = rp.TaskDescription(agent_task['description'])
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
        self.assertEqual(0, len(agent_files))

        agent_0._start_sub_agents()

        agent_files = glob.glob('%s/agent_1.*.sh' % agent_0._pwd)
        self.assertEqual(2, len(agent_files))
        for agent_file in agent_files:
            os.unlink(agent_file)

        # incorrect config setup for agent ('target' is in ['local', 'node'])
        agent_0._session.cfg['agents']['agent_1']['target'] = 'incorrect_target'
        with self.assertRaises(ValueError):
            agent_0._start_sub_agents()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    def test_start_services(self, mocked_init):

        advanced_services = list()

        def local_advance(things, publish, push):
            nonlocal advanced_services
            advanced_services = things

        agent_0 = Agent_0()
        agent_0._uid                   = 'agent_0'
        agent_0._cb_lock               = mt.RLock()
        agent_0._threads               = dict()
        agent_0._log                   = mock.Mock()
        agent_0._service_uids_launched = list()
        agent_0._services_setup        = mock.Mock()

        agent_0.advance = local_advance

        agent_0._session = self._session
        agent_0._session._cfg = ru.Config(from_dict={'pid'          : 12,
                                                     'pilot_sandbox': '/',
                                                     'services'     : []})

        agent_0._session._cfg.services = [{}]
        with self.assertRaises(ValueError):
            # no executable provided
            agent_0._start_services()

        agent_0._session._cfg.services = [{'executable': 'test',
                                           'ranks'     : 'zero'}]
        with self.assertRaises(TypeError):
            # type mismatch
            agent_0._start_services()

        services = [{'executable'    : '/bin/ls',
                     'cores_per_rank': '3',
                     'metadata'      : {}}]
        agent_0._session._cfg.services = services

        agent_0._services_setup.wait = mock.Mock(return_value=True)
        agent_0._start_services()

        self.assertTrue(advanced_services[0]['uid'].startswith('service.'))
        self.assertEqual(advanced_services[0]['type'], 'service_task')

        service_td = advanced_services[0]['description']
        self.assertEqual(service_td['executable'], services[0]['executable'])
        self.assertEqual(service_td['ranks'], 1)
        self.assertEqual(service_td['mode'], rp.AGENT_SERVICE)

        agent_0._services_setup.wait = mock.Mock(return_value=False)
        with self.assertRaises(RuntimeError):
            agent_0._start_services()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Agent_0, '__init__', return_value=None)
    def test_ctrl_service_up(self, mocked_init):

        agent_0 = Agent_0()
        agent_0._cfg                   = ru.Config()
        agent_0._session               = self._session
        agent_0._service_uids_launched = ['101', '102']
        agent_0._service_uids_running  = []

        agent_0._pid  = 'pilot_test.0000'
        agent_0._log  = mock.Mock()
        agent_0._prof = mock.Mock()

        agent_0._services_setup = mt.Event()

        topic = 'test_topic'
        msg   = {'cmd': 'service_up',
                 'arg': {}}

        msg['arg']['uid'] = '101'
        agent_0._control_cb(topic, msg)
        self.assertFalse(agent_0._services_setup.is_set())

        msg['arg']['uid'] = '102'
        agent_0._control_cb(topic, msg)
        self.assertTrue(agent_0._services_setup.is_set())


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestComponent()
    tc.test_check_control_cb()
    tc.test_start_sub_agents()
    tc.test_start_services()
    tc.test_ctrl_service_up()


# ------------------------------------------------------------------------------
