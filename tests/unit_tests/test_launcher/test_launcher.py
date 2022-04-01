# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = "Copyright 2020, http://radical.rutgers.edu"
__license__   = "MIT"

import os

from unittest import TestCase
from unittest import mock

import radical.utils as ru
import radical.pilot as rp

from   radical.pilot.pmgr.launching.base import PMGRLaunchingComponent


# ------------------------------------------------------------------------------
#
class TestLauncher(TestCase):

    # --------------------------------------------------------------------------
    #
    def setUp(self):

        class Session(object):
            def __init__(self):
                self.uid = 'uid.0'
                self.sid = 'sid.0'
                self.cfg = ru.Config(cfg={'dburl': 'db://'})

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


        session = Session()
        configs = ru.Config('radical.pilot.resource', name='*')
        return session, configs

    # --------------------------------------------------------------------------
    #
    def tearDown(self):
        pass

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PMGRLaunchingComponent, '__init__', return_value=None)
    def test_configure(self, mocked_init):

        session, configs = self.setUp()

        component = PMGRLaunchingComponent(cfg=None, session=None)
        component._uid        = 'pmgr.launching.0000'
        component._cfg        = mock.Mock()
        component._log        = ru.Logger('dummy')
        component._rp_version = '0.0'
        component._session    = session

        component._pmgr       = 'pmgr.0'
        component._prof       = ru.Config(cfg = {'enabled': False})
        component._sandboxes  = dict()

        component._mod_dir    = os.path.dirname(os.path.abspath(__file__))
        component._root_dir   = "%s/../../src/radical/pilot/" % component._mod_dir

        component._rp_version    = rp.version
        component._rp_sdist_name = rp.sdist_name
        component._rp_sdist_path = rp.sdist_path

        resource = 'local.localhost'
        rcfg     = configs.local.localhost

        pilot    = {
                        'uid'         : 'pilot.0001',
                        'description' : {'cores'          : 0,
                                         'gpus'           : 0,
                                         'nodes'          : 2,
                                         'queue'          : 'default',
                                         'project'        : 'foo',
                                         'job_name'       : None,
                                         'runtime'        : 10,
                                         'app_comm'       : 0,
                                         'cleanup'        : 0,
                                         'memory'         : 0,
                                         'candidate_hosts': None,
                                         'services'       : []}
                   }
        component._prepare_pilot(resource, rcfg, pilot, {}, '')
        self.assertEqual(pilot['jd_dict'].name, 'pilot.0001')
        self.assertEqual(pilot['jd_dict'].environment['RADICAL_BASE'],
                         str(session._get_resource_sandbox(pilot)))

        pilot    = {
                        'uid'         : 'pilot.0000',
                        'description' : {'cores'          : 10,
                                         'gpus'           : 2,
                                         'nodes'          : 0,
                                         'queue'          : 'default',
                                         'project'        : 'foo',
                                         'job_name'       : None,
                                         'runtime'        : 10,
                                         'app_comm'       : 0,
                                         'cleanup'        : 0,
                                         'memory'         : 0,
                                         'candidate_hosts': None,
                                         'services'       : []}
                   }
        component._prepare_pilot(resource, rcfg, pilot, {}, '')
        self.assertEqual(pilot['jd_dict'].name, 'pilot.0000')
        self.assertEqual(pilot['jd_dict'].environment['RADICAL_BASE'],
                         str(session._get_resource_sandbox(pilot)))

        pilot    = {
                        'uid'         : 'pilot.0000',
                        'description' : {'cores'          : 10,
                                         'gpus'           : 2,
                                         'nodes'          : 0,
                                         'queue'          : 'default',
                                         'project'        : 'foo',
                                         'job_name'       : 'bar',
                                         'runtime'        : 10,
                                         'app_comm'       : 0,
                                         'cleanup'        : 0,
                                         'memory'         : 0,
                                         'candidate_hosts': None,
                                         'services'       : []}
                   }
        component._prepare_pilot(resource, rcfg, pilot, {}, '')
        self.assertEqual(pilot['jd_dict'].name, 'bar')

        # default value is {}
        self.assertEqual(pilot['jd_dict'].system_architecture, {})

        # value for "ornl.summit" is 4
        resource = 'ornl.summit'
        rcfg = configs.ornl.summit
        component._prepare_pilot(resource, rcfg, pilot, {}, '')
        self.assertEqual(pilot['jd_dict'].system_architecture['smt'], 4)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestLauncher()
    tc.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
