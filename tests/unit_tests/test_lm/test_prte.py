# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager.base import RMInfo

from radical.pilot.agent.launch_method.prte    import DVM_HOSTS_FILE_TPL
from radical.pilot.agent.launch_method.prte    import DVM_URI_FILE_TPL
from radical.pilot.agent.launch_method.prte    import PRTE

from .test_common import setUp


# ------------------------------------------------------------------------------
#
class TestPRTE(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:
        cls._work_dir = os.getcwd()


    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls) -> None:
        # cleanup - remove prrte-files with hosts and dvm-uri
        prrte_files = glob.glob('%s/prrte.*' % cls._work_dir)
        for _file in prrte_files:
            try   : os.unlink(_file)
            except: pass


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    @mock.patch('radical.pilot.agent.launch_method.prte.LaunchMethod')
    def test_init(self, mocked_lm_base, mocked_terminate):

        if 'RADICAL_PILOT_PRUN_VERBOSE' in os.environ:
            del os.environ['RADICAL_PILOT_PRUN_VERBOSE']

        lm_prte = PRTE('', {}, None, None, None)
        self.assertFalse(lm_prte._verbose)

        os.environ['RADICAL_PILOT_PRUN_VERBOSE'] = 'TRUE'
        lm_prte = PRTE('', {}, None, None, None)
        self.assertTrue(lm_prte._verbose)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__',   return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    @mock.patch.object(PRTE, '_configure', return_value={'dvm_list'    : {},
                                                         'version_info': {}})
    @mock.patch('radical.utils.which',     return_value='/usr/bin/prun')
    def test_init_from_scratch(self, mocked_which, mocked_configure,
                                     mocked_terminate, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)

        lm_info = lm_prte.init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())
        self.assertEqual(lm_info['details'], mocked_configure())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__',   return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_init_from_info(self, mocked_terminate, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_prte.sh',
                   'command': '/usr/bin/prun',
                   'details': {'dvm_list': {'0': {'dvm_uri': 'uri000',
                                                  'nodes'  : ['0', '1']}},
                               'version_info': {'name'   : 'PRTE',
                                                'version': '1.1.1'}}}

        lm_prte.init_from_info(lm_info)
        self.assertEqual(lm_prte._env,     lm_info['env'])
        self.assertEqual(lm_prte._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_prte._command, lm_info['command'])
        self.assertEqual(lm_prte._details, lm_info['details'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_prte.init_from_info(lm_info)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__',    return_value=None)
    @mock.patch.object(PRTE, '_terminate',  return_value=None)
    @mock.patch('radical.utils.which',      return_value='any_cmd')
    @mock.patch('radical.utils.sh_callout', return_value=('''
                    PRTE: 2.0v2.0.0rc2-12-g68bd9f2
      PRTE repo revision: v2.0.0rc2-12-g68bd9f2
''', None, None))
    @mock.patch('subprocess.Popen')
    @mock.patch('threading.Thread.start')
    @mock.patch('threading.Event')
    def test_configure(self, mocked_mt_event, mocked_mt_thread, mocked_mp_popen,
                             mocked_sh_callout, mocked_which,
                             mocked_terminate, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)
        lm_prte._log = lm_prte._prof = mock.Mock()
        lm_prte._verbose = True
        lm_prte._rm_info = RMInfo({
            'cores_per_node': 42,
            'node_list'     : [{'name': 'name_0', 'index': 0},
                               {'name': 'name_1', 'index': 1},
                               {'name': 'name_2', 'index': 2},
                               {'name': 'name_3', 'index': 3}]
        })
        lm_prte._lm_cfg = {'pid'      : 11111,
                           'dvm_count': 2}

        # assume that these files were created by `prte` command
        for _dvm_id in range(lm_prte._lm_cfg['dvm_count']):
            dvm_file_info = {'base_path': self._work_dir, 'dvm_id': _dvm_id}
            with ru.ru_open(DVM_URI_FILE_TPL % dvm_file_info, 'w') as fout:
                fout.write('prte-batch0-2387273@0.0;tcp4://1.1.1.1:12345\n')

        prte_cmd = ''

        def mp_popen_side_effect(*args, **kwargs):
            nonlocal prte_cmd
            prte_cmd = ' '.join(args[0])

        mocked_mp_popen.side_effect = mp_popen_side_effect
        mocked_mt_event.wait        = mock.Mock(return_value=True)

        lm_details = lm_prte._configure()

        prte_info = mocked_sh_callout()[0]
        self.assertIn(lm_details['version_info']['version'],        prte_info)
        self.assertIn(lm_details['version_info']['version_detail'], prte_info)

        self.assertEqual(len(lm_details['dvm_list']),
                         lm_prte._lm_cfg['dvm_count'])

        prte_cmd_opts = '--pmixmca ptl_base_max_msg_size 1073741824 ' \
                        '--prtemca routed_radix 2 ' \
                        '--prtemca plm_rsh_num_concurrent 2 ' \
                        '--prtemca plm_base_verbose 5'
        self.assertTrue(prte_cmd.endswith(prte_cmd_opts))

        # check that corresponding files with nodes/hosts were created
        for _dvm_id in range(lm_prte._lm_cfg['dvm_count']):
            self.assertTrue(os.path.isfile(DVM_HOSTS_FILE_TPL % {
                'base_path': self._work_dir, 'dvm_id': _dvm_id}))


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__',   return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_finalize(self, mocked_terminate, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)

        self.assertFalse(mocked_terminate.called)
        lm_prte.finalize()
        self.assertTrue(mocked_terminate.called)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__', return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_get_partition_ids(self, mocked_terminate, mocked_init):

        lm_prte = PRTE('', {}, None, None, None)

        dvm_list = {'0': {'dvm_uri': 'uri000',
                          'nodes'  : ['0', '1']},
                    '1': {'dvm_uri': 'uri001',
                          'nodes'  : ['2', '3']}}

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_prte.sh',
                   'command': '/usr/bin/prun',
                   'details': {'dvm_list'    : dvm_list,
                               'version_info': {'name'   : 'PRTE',
                                                'version': '1.1.1'}}}
        lm_prte.init_from_info(lm_info)
        self.assertEqual(lm_prte.get_partition_ids(), ['0', '1'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PRTE, '__init__',   return_value=None)
    @mock.patch.object(PRTE, '_terminate', return_value=None)
    def test_can_launch(self, mocked_terminate, mocked_init):

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
        lm_prte.init_from_info(lm_info)

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

                command = lm_prte.get_exec(task)
                self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestPRTE()
    tc.test_init()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()
    tc.test_configure()
    tc.test_finalize()
    tc.test_get_partitions()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
