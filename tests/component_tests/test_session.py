# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2020-2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob
import os
import shutil

import radical.utils as ru

from unittest import TestCase, mock

from radical.pilot.session import Session


# ------------------------------------------------------------------------------
#
class TestSession(TestCase):

    _cleanup_files = []

    # --------------------------------------------------------------------------
    #
    @classmethod
    @mock.patch.object(Session, '_initialize_primary', return_value=None)
    @mock.patch.object(Session, '_get_logger')
    @mock.patch.object(Session, '_get_profiler')
    @mock.patch.object(Session, '_get_reporter')
    def setUpClass(cls, *args, **kwargs) -> None:

        cls._session = Session()
        cls._cleanup_files.append(cls._session.uid)

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls) -> None:

        for p in cls._cleanup_files:
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
    def test_list_resources(self):

        listed_resources = self._session.list_resources()

        self.assertIsInstance(listed_resources, list)
        self.assertIn('local.localhost', listed_resources)

    # --------------------------------------------------------------------------
    #
    def test_get_resource_config(self):

        rcfg_label = 'access.bridges2'

        # schemas are ["ssh", "gsissh"]
        rcfg = self._session.get_resource_config(rcfg_label)
        self.assertEqual(rcfg.job_manager_endpoint,
                         rcfg[rcfg.schemas[0]].job_manager_endpoint)
        new_schema = 'gsissh'
        rcfg = self._session.get_resource_config(rcfg_label, schema=new_schema)
        self.assertEqual(rcfg.job_manager_endpoint,
                         rcfg[new_schema].job_manager_endpoint)

        # check exceptions

        with self.assertRaises(RuntimeError):
            self._session.get_resource_config(resource='wrong_domain.host')

        with self.assertRaises(RuntimeError):
            self._session.get_resource_config(resource='local.wrong_host')

        with self.assertRaises(RuntimeError):
            self._session.get_resource_config(
                resource='local.localhost', schema='wrong_schema')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Session, '_initialize_primary', return_value=None)
    @mock.patch.object(Session, '_get_logger')
    @mock.patch.object(Session, '_get_profiler')
    @mock.patch.object(Session, '_get_reporter')
    @mock.patch('radical.pilot.session.ru.Config')
    def test_resource_schema_alias(self, mocked_config, *args, **kwargs):

        mocked_config.return_value = ru.TypedDict({
            'local': {
                'test': {
                    'schemas'           : ['schema_origin',
                                           'schema_alias',
                                           'schema_alias_alias'],
                    'schema_origin'     : {'param_0': 'value_0'},
                    'schema_alias'      : 'schema_origin',
                    'schema_alias_alias': 'schema_alias'
                }
            }
        })

        s_alias = Session()

        self.assertEqual(
            s_alias._rcfgs.local.test.schema_origin,
            s_alias._rcfgs.local.test.schema_alias)
        self.assertEqual(
            s_alias._rcfgs.local.test.schema_origin,
            s_alias._rcfgs.local.test.schema_alias_alias)
        self.assertEqual(
            s_alias.get_resource_config('local.test', 'schema_origin'),
            s_alias.get_resource_config('local.test', 'schema_alias_alias'))

        self._cleanup_files.append(s_alias.uid)

        with self.assertRaises(KeyError):
            # schema alias refers to unknown schema
            mocked_config.return_value = ru.TypedDict({
                'local': {
                    'test': {
                        'schemas'           : ['schema_alias_error'],
                        'schema_alias_error': 'unknown_schema'
                    }
                }
            })
            Session()

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Session, 'created', return_value=0)
    @mock.patch.object(Session, 'closed', return_value=0)
    def test_close(self, mocked_closed, mocked_created):

        # check default values
        self.assertFalse(self._session._close_options.cleanup)
        self.assertFalse(self._session._close_options.download)
        self.assertTrue(self._session._close_options.terminate)

        # only `True` values are targeted

        self._session._closed = False
        self._session.close(cleanup=True)
        self.assertTrue(self._session._close_options.cleanup)

        self._session._closed = False
        self._session.fetch_json     = mock.Mock()
        self._session.fetch_profiles = mock.Mock()
        self._session.fetch_logfiles = mock.Mock()
        self._session.close(download=True)
        self._session.fetch_json.assert_called()
        self._session.fetch_profiles.assert_called()
        self._session.fetch_logfiles.assert_called()

        self._session._closed = False
        self._session.close(cleanup=True, terminate=True)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestSession()
    tc.test_list_resources()
    tc.test_get_resource_config()
    tc.test_resource_schema_alias()


# ------------------------------------------------------------------------------

