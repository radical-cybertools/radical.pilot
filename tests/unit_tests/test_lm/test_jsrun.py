# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import glob

from unittest import mock, TestCase

import radical.utils as ru
import radical.pilot as rp

from .test_common import setUp
from radical.pilot.agent.launch_method.jsrun import JSRUN, LaunchMethod

JSRUN._in_pytest = True


# ------------------------------------------------------------------------------
#
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
    @mock.patch.object(LaunchMethod, '__init__', return_value=None)
    def test_init(self, mocked_lm_init):

        lm_jsrun = JSRUN('', {}, None, None, None)

        # default initial values
        self.assertEqual(lm_jsrun._command, '')
        self.assertFalse(lm_jsrun._erf)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/jsrun')
    def test_init_from_scratch(self, mocked_which, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)

        lm_jsrun.name = 'JSRUN'
        lm_info = lm_jsrun.init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())
        self.assertFalse(lm_info['erf'])

        lm_jsrun.name = 'JSRUN_ERF'
        lm_info = lm_jsrun.init_from_scratch({}, '')
        self.assertTrue(lm_info['erf'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_jsrun.sh',
                   'command': '/usr/bin/jsrun',
                   'erf'    : True}
        lm_jsrun.init_from_info(lm_info)
        self.assertEqual(lm_jsrun._env,     lm_info['env'])
        self.assertEqual(lm_jsrun._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_jsrun._command, lm_info['command'])
        self.assertEqual(lm_jsrun._erf,     lm_info['erf'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_jsrun.init_from_info(lm_info)

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
                   'command': '/usr/bin/jsrun',
                   'erf'    : False}
        lm_jsrun.init_from_info(lm_info)
        lm_env = lm_jsrun.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    def test_create_resource_set_file(self, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)
        lm_jsrun._erf = True

        test_cases = setUp('lm', 'jsrun_erf')
        for test_case in test_cases:

            uid    = test_case[0]['uid']
            slots  = test_case[0]['slots']
            result = test_case[1]

            if result == 'AssertionError':
                with self.assertRaises(AssertionError):
                    lm_jsrun._create_resource_set_file(
                        slots=slots, uid=uid, sandbox=self._sbox)

            else:
                rs_layout = test_case[2]
                slots     = rp.utils.convert_slots_to_old(slots)
                for slot in slots:
                    slot['version'] = 1

                rs_file   = lm_jsrun._create_resource_set_file(
                    slots=slots, uid=uid, sandbox=self._sbox)
                with ru.ru_open(rs_file) as rs_layout_file:
                    lines = rs_layout_file.readlines()
                    self.assertEqual(lines, rs_layout)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @mock.patch.object(JSRUN, '_create_resource_set_file')
    @mock.patch('radical.utils.Logger')
    def test_get_launch_rank_cmds(self, mocked_logger, mocked_rs_file,
                                  mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)
        lm_jsrun._command = 'jsrun'
        lm_jsrun._log     = mocked_logger

        lm_jsrun._erf = True

        for lm_name in ['jsrun', 'jsrun_erf']:

            lm_jsrun._erf = bool('_erf' in lm_name)

            test_cases = setUp('lm', lm_name)
            for test_case in test_cases:
                task   = test_case[0]
                result = test_case[1]

                lm_jsrun._rm_info = {
                    'gpus_per_node'   : 1,
                    'threads_per_core': 1
                }

                if result == 'AssertionError':
                    if not lm_jsrun._erf:
                        with self.assertRaises(AssertionError):
                            lm_jsrun.get_launch_cmds(task, '')

                else:
                    if len(test_case) > 2:
                        lm_jsrun._create_resource_set_file.return_value = \
                            test_case[3]  # resource set file name


                    command = lm_jsrun.get_launch_cmds(task, '')
                    print()
                    print(task['uid'])
                    print(command)
                    print(result['launch_cmd'])
                    self.assertEqual(command, result['launch_cmd'], task['uid'])

                    command = lm_jsrun.get_exec(task)
                    self.assertEqual(command, result['rank_exec'], task['uid'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    def test_get_rank_cmd(self, mocked_init):

        lm_jsrun = JSRUN('', {}, None, None, None)

        command = lm_jsrun.get_rank_cmd()
        self.assertIn('$PMIX_RANK', command)


# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestJSRun()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_create_resource_set_file()
    tc.test_get_launch_rank_cmds()
    tc.test_get_rank_cmd()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
