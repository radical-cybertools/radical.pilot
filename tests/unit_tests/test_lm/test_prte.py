# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.prte import PRTE


# ------------------------------------------------------------------------------
#
class TestPRTE(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__', return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    @mock.patch.object(PRTE, '_configure', return_value={})
    @mock.patch('radical.utils.which', return_value='/usr/bin/prun')
    def test_init_from_scratch(self, mocked_which, mocked_configure,
                               mocked_term, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)

        lm_info = lm_prte._init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())
        self.assertEqual(lm_info['details'], mocked_configure())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__', return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_init_from_info(self, mocked_term, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_prte.sh',
                   'command': '/usr/bin/prun',
                   'details': {'dvm_list': {'id000': {'dvm_uri': 'uri000',
                                                      'nodes': ['0', '1']}},
                               'version_info': {'name': 'PRTE',
                                                'version': '1.1.1'},
                               'cvd_id_mode': 'physical'}}

        lm_prte._init_from_info(lm_info)
        self.assertEqual(lm_prte._env,     lm_info['env'])
        self.assertEqual(lm_prte._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_prte._command, lm_info['command'])
        self.assertEqual(lm_prte._details, lm_info['details'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_prte._init_from_info(lm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__', return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_can_launch(self, mocked_term, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)
        self.assertTrue(lm_prte.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_prte.can_launch(
            task={'description': {'executable': None}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__', return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_get_launcher_env(self, mocked_term, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_prte.sh',
                   'command': '/usr/bin/prun',
                   'details': {'dvm_list'    : {'id000': {'dvm_uri': 'uri000',
                                                          'nodes': ['0', '1']}},
                               'version_info': {'name'   : 'PRTE',
                                                'version': '1.1.1'},
                               'cvd_id_mode' : 'physical'}}
        lm_prte._init_from_info(lm_info)

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'],
                      lm_prte.get_launcher_env())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__', return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_get_launch_rank_cmds(self, mocked_term, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)
        lm_prte.name     = 'prte'
        lm_prte._command = 'prun'
        lm_prte._details = {'version_info': {'name'   : 'PRTE',
                                             'version': '1.1.1'},
                            'cvd_id_mode' : 'physical'}
        lm_prte._verbose = False

        test_cases = setUp('lm', 'prte')
        for task, result in test_cases:

            lm_prte._details['dvm_list'] = {}

            # FIXME: add test to check partitions
            if task.get('dvm_list'):
                lm_prte._details['dvm_list'].update(task['dvm_list'])

            if result == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    lm_prte.get_launch_cmds(task, '')

            else:
                command = lm_prte.get_launch_cmds(task, '')
                self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

                command = lm_prte.get_rank_exec(task, None, None)
                self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestPRTE()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
