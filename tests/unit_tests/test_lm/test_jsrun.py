# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import glob

from unittest import mock, TestCase

import radical.utils as ru

from .test_common import setUp
from radical.pilot.agent.launch_method.jsrun import JSRUN


class TestJSRun(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:
        cls._sbox = os.getcwd()

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls) -> None:
        rs_files = glob.glob('%s/*.rs' % cls._sbox)
        for rs_file in rs_files:
            try   : os.remove(rs_file)
            except: pass

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/jsrun')
    def test_init_from_scratch(self, mocked_which, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)

        lm_info = lm_jsrun._init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_jsrun.sh',
                   'command': '/usr/bin/jsrun'}
        lm_jsrun._init_from_info(lm_info)
        self.assertEqual(lm_jsrun._env,     lm_info['env'])
        self.assertEqual(lm_jsrun._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_jsrun._command, lm_info['command'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_jsrun._init_from_info(lm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)
        self.assertTrue(lm_jsrun.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_jsrun.can_launch(
            task={'description': {'executable': None}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)
        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_jsrun.sh',
                   'command': '/usr/bin/jsrun'}
        lm_jsrun._init_from_info(lm_info)
        lm_env = lm_jsrun.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    def test_create_resource_set_file(self, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)

        test_cases = setUp('lm', 'jsrun')
        for test_case in test_cases:

            if test_case[1] in ['AssertionError']:
                continue

            uid       = test_case[0]['uid']
            slots     = test_case[0]['slots']
            rs_layout = test_case[2]

            rs_file = lm_jsrun._create_resource_set_file(
                slots=slots, uid=uid, sandbox=self._sbox)
            with ru.ru_open(rs_file) as rs_layout_file:
                self.assertEqual(rs_layout_file.readlines(), rs_layout)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @mock.patch.object(JSRUN, '_create_resource_set_file')
    @mock.patch('radical.utils.Logger')
    def test_get_launch_rank_cmds(self, mocked_logger, mocked_rs_file,
                                  mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)
        lm_jsrun._log     = mocked_logger
        lm_jsrun._command = 'jsrun'

        test_cases = setUp('lm', 'jsrun')
        for test_case in test_cases:

            task   = test_case[0]
            result = test_case[1]

            if result == 'AssertionError':
                with self.assertRaises(AssertionError):
                    lm_jsrun.get_launch_cmds(task, '')

            else:
                rs_layout_file = test_case[3]
                lm_jsrun._create_resource_set_file.return_value = rs_layout_file

                command = lm_jsrun.get_launch_cmds(task, '')
                self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

                command = lm_jsrun.get_rank_exec(task, None, None)
                self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestJSRun()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_create_resource_set_file()
    tc.test_get_launch_rank_cmds()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
