# pylint: disable=protected-access, unused-argument

__copyright__ = 'Copyright 2020, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob
import os
import shutil

from unittest import TestCase, mock

import radical.pilot as rp

TEST_CASES_PATH = '%s/test_cases' % os.path.dirname(__file__)


# ------------------------------------------------------------------------------
#
class TestSession(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    @mock.patch.object(rp.Session, '_initialize_primary', return_value=None)
    def setUpClass(cls, *args, **kwargs):
        cls._session = rp.Session()

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

        rcfg_label = 'xsede.comet_ssh'

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


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestSession()
    tc.test_list_resources()
    tc.test_get_resource_config()


# ------------------------------------------------------------------------------

