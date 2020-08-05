
# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

__copyright__ = "Copyright 2020, http://radical.rutgers.edu"
__license__   = "MIT"

from unittest import TestCase

import os
import radical.utils as ru
import radical.pilot as rp

from   radical.pilot.pmgr.launching.default import Default


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
class TestLauncher(TestCase):

    # ------------------------------------------------------------------------------
    #
    def setUp(self):

        class Session(object):
            def __init__(self):
                self.uid = 'uid.0'
                self.sid = 'sid.0'
                self.cfg = ru.Config(cfg={'dburl': 'db://'})

            def _get_resource_sandbox(self, pilot):
                return ru.Url('/resource/sandbox/%s' % pilot)

            def _get_session_sandbox(self, pilot):
                return ru.Url('/session/sandbox/%s' % pilot)

            def _get_pilot_sandbox(self, pilot):
                return ru.Url('/pilot/sandbox/%s' % pilot)

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
    @mock.patch.object(Default, '__init__', return_value=None)
    def test_configure(self, mocked_init):

        session, configs = self.setUp()

        component = Default(cfg=None, session=None)
        component._cfg        = mock.Mock()
        component._log        = ru.Logger('dummy')
        component._rp_version = '0.0'
        component._session    = session

        component._pmgr       = 'pmgr.0'
        component._prof       = ru.Config(cfg = {'enabled': False})
        component._cache_lock = ru.Lock()
        component._cache      = dict()
        component._sandboxes  = dict()

        component._mod_dir    = os.path.dirname(os.path.abspath(__file__))
        component._root_dir   = "%s/../../src/radical/pilot/" % component._mod_dir
        component._conf_dir   = "%s/configs/" % component._root_dir

        component._rp_version    = rp.version
        component._rp_sdist_name = rp.sdist_name
        component._rp_sdist_path = rp.sdist_path

        resource = 'local.localhost'
        rcfg     = configs.local.localhost

        pilot    = {
                        'uid'         : 'pilot.0000',
                        'description' : {'cores'          : 10,
                                         'gpus'           : 2,
                                         'queue'          : 'default',
                                         'project'        : 'foo',
                                         'job_name'       : None,
                                         'runtime'        : 10,
                                         'app_comm'       : 0,
                                         'cleanup'        : 0,
                                         'memory'         : 0,
                                         'candidate_hosts': None,
                                         }
                   }
        ret = component._prepare_pilot(resource, rcfg, pilot, {})
        assert(ret['jd'].name == 'pilot.0000')

        pilot    = {
                        'uid'         : 'pilot.0000',
                        'description' : {'cores'          : 10,
                                         'gpus'           : 2,
                                         'queue'          : 'default',
                                         'project'        : 'foo',
                                         'job_name'       : 'bar',
                                         'runtime'        : 10,
                                         'app_comm'       : 0,
                                         'cleanup'        : 0,
                                         'memory'         : 0,
                                         'candidate_hosts': None,
                                         }
                   }
        ret = component._prepare_pilot(resource, rcfg, pilot, {})
        assert(ret['jd'].name == 'bar')


# ------------------------------------------------------------------------------

