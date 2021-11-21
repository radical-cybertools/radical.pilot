# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2020-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob
import os
import shutil

from unittest import TestCase, mock

from radical.pilot.session import Session

TEST_CASES_PATH = '%s/test_cases' % os.path.dirname(__file__)


# ------------------------------------------------------------------------------
#
class TestSession(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    @mock.patch.object(Session, '_initialize_primary', return_value=None)
    @mock.patch.object(Session, '_get_logger')
    @mock.patch.object(Session, '_get_profiler')
    @mock.patch.object(Session, '_get_reporter')
    def setUpClass(cls, *args, **kwargs):
        cls._session = Session()

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls):
        for d in glob.glob('./rp.session.*'):
            if os.path.isdir(d):
                try:
                    shutil.rmtree(d)
                except OSError as e:
                    print('[ERROR] %s - %s' % (e.filename, e.strerror))

    # --------------------------------------------------------------------------
    #
    def test_list_resources(self):

        listed_resources = self._session.list_resources()

        self.assertIsInstance(listed_resources, list)
        self.assertIn('local.localhost', listed_resources)

    # --------------------------------------------------------------------------
    #
    def test_get_resource_config(self):

        rcfg_label = 'xsede.bridges2'

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
        with self.assertRaises(ValueError):
            self._session.close(cleanup=True, terminate=False)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestSession()
    tc.test_list_resources()
    tc.test_get_resource_config()


# ------------------------------------------------------------------------------

