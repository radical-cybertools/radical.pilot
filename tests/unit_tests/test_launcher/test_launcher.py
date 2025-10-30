# pylint: disable=protected-access, no-value-for-parameter, unused-argument

import copy

from unittest import mock, TestCase

import radical.utils as ru
import radical.pilot as rp

from radical.pilot.pmgr.launching.base import PMGRLaunchingComponent


# ------------------------------------------------------------------------------
#
class TestLauncher(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        class Session(object):

            def __init__(self):
                self.uid = 'uid.0'
                self.sid = 'sid.0'
                self.cfg = ru.Config(cfg={})

            def _get_endpoint_fs(self, pilot):
                return ru.Url(pilot['description'].get('endpoint_fs') or '/')

            def _get_resource_sandbox(self, pilot):
                return ru.Url(pilot['description'].get('sandbox') or
                              '/resource/sandbox')

            def _get_session_sandbox(self, pilot):
                return ru.Url(pilot['description'].get('session_sandbox') or
                              '/session/sandbox/%s' % self.uid)

            def _get_pilot_sandbox(self, pilot):
                return ru.Url('/pilot/sandbox/%s' % pilot['uid'])

            def _get_client_sandbox(self):
                return ru.Url('/client/sandbox')

        cls._session = Session()
        cls._configs = ru.Config('radical.pilot.resource', name='*')

        for site in cls._configs:
            for resource in cls._configs[site]:
                cls._configs[site][resource] = \
                        rp.ResourceConfig(cls._configs[site][resource])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PMGRLaunchingComponent, '__init__', return_value=None)
    @mock.patch.object(ru.Config, 'write', return_value=None)
    def test_configure(self, mocked_cfg_write, mocked_init):

        component = PMGRLaunchingComponent(cfg=None, session=None)
        component._uid           = 'pmgr.launching.0000'
        component._cfg           = mock.Mock()
        component._log           = mock.Mock()
        component._session       = self._session

        component._pmgr          = 'pmgr.0'
        component._prof          = ru.Config(cfg={'enabled': False})
        component._sandboxes     = dict()
        component._root_dir      = '/radical_pilot_src'
        component._rm_info       = ru.Config(cfg={'details': None})

        component._rp_version    = rp.version

        resource                 = 'local.localhost'
        rcfg                     = self._configs.local.localhost
        rcfg.verify()

        pilot = {'uid'         : 'pilot.0001',
                 'description' : {'cores'          : 0,
                                  'gpus'           : 0,
                                  'nodes'          : 2,
                                  'backup_nodes'   : 0,
                                  'queue'          : 'default',
                                  'project'        : 'foo',
                                  'job_name'       : None,
                                  'runtime'        : 10,
                                  'app_comm'       : 0,
                                  'cleanup'        : 0,
                                  'memory'         : 0,
                                  'services'       : [],
                                  'prepare_env'    : {},
                                  'enable_ep'      : False,
                                  'reconfig_src'   : None}}

        component._prepare_pilot(resource, rcfg, pilot, {}, '')

        # method `write` for agent config was called
        self.assertTrue(mocked_cfg_write.called)

        jd_dict = pilot['jd_dict']
        self.assertEqual(jd_dict.name, pilot['uid'])
        self.assertEqual(jd_dict.environment['RADICAL_BASE'],
                         str(self._session._get_resource_sandbox(pilot)))

        # default SMT is set to "1"
        self.assertEqual(jd_dict.environment['RADICAL_SMT'], '1')

        # allocated resources - part of the job description and agent's config
        self.assertEqual(jd_dict.total_cpu_count, pilot['cfg']['cores'])
        self.assertEqual(jd_dict.total_gpu_count, pilot['cfg']['gpus'])

        pilot = {'uid'         : 'pilot.0000',
                 'description' : {'cores'          : 10,
                                  'gpus'           : 2,
                                  'nodes'          : 0,
                                  'backup_nodes'   : 0,
                                  'queue'          : 'default',
                                  'project'        : 'foo',
                                  'job_name'       : 'bar',
                                  'runtime'        : 10,
                                  'app_comm'       : 0,
                                  'cleanup'        : 0,
                                  'memory'         : 0,
                                  'services'       : [],
                                  'prepare_env'    : {},
                                  'enable_ep'      : False,
                                  'reconfig_src'   : None}}

        component._prepare_pilot(resource, rcfg, pilot, {}, '')
        self.assertEqual(pilot['jd_dict'].name,
                         pilot['description']['job_name'])

        # test SMT (provided by env variable RADICAL_SMT or within the config's
        #           parameter "system_architecture")

        # `system_architecture` should exist and be a dict
        self.assertIsInstance(pilot['jd_dict'].system_architecture, dict)

        # value for "ornl.summit" is 1 (with MPIRun LM)
        component._prepare_pilot('ornl.summit',
                                 self._configs.ornl.summit,
                                 pilot, {}, '')
        self.assertEqual(pilot['jd_dict'].system_architecture['smt'], 1)
        # value for "ornl.summit_jsrun" is 4 (with JSRun LM)
        component._prepare_pilot('ornl.summit_jsrun',
                                 self._configs.ornl.summit_jsrun,
                                 pilot, {}, '')
        self.assertEqual(pilot['jd_dict'].system_architecture['smt'], 4)

        # set `cores/gpus_per_node` unknown - a.k.a. heterogeneous cluster

        het_rcfg = copy.deepcopy(rcfg)
        het_rcfg.cores_per_node = 0
        het_rcfg.gpus_per_node  = 0

        pilot['description'].update({'cores': 0, 'gpus' : 0, 'nodes': 1})
        with self.assertRaises(RuntimeError):
            component._prepare_pilot(resource, het_rcfg, pilot, {}, '')

        pilot['description'].update({'cores': 24, 'gpus': 2, 'nodes': 0})
        component._prepare_pilot(resource, het_rcfg, pilot, {}, '')
        # allocated resources equal to the requested cores & gpus
        self.assertEqual(pilot['cfg']['cores'], pilot['description']['cores'])
        self.assertEqual(pilot['cfg']['gpus'],  pilot['description']['gpus'])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestLauncher()
    tc.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
