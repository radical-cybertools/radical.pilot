# pylint: disable=protected-access, unused-argument

__copyright__ = 'Copyright 2020, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob
import os
import shutil

from unittest import TestCase, mock

import radical.pilot as rp
import radical.utils as ru

TEST_CASES_PATH = 'tests/test_components/test_cases'


# ------------------------------------------------------------------------------
#
class SessionTestClass(TestCase):

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
    def test_add_resource_config(self):

        cfg_path = os.path.abspath('%s/user_cfg.json' % TEST_CASES_PATH)

        # add resource config by providing config file path
        self._session.add_resource_config(resource_config=cfg_path)

        # add resource config by providing Config instance
        user_cfg = ru.Config(path=cfg_path)
        user_cfg.mpi_launch_method = 'MPIRUN'
        user_cfg.cores_per_node    = 1024
        # label with "<domain>.<host>"
        user_cfg.label = 'local.mpirun_cfg'
        self._session.add_resource_config(resource_config=user_cfg)
        # no label, thus default label is assigned
        user_cfg.label = None
        self._session.add_resource_config(resource_config=user_cfg)

        # check exception(s)
        user_cfg.label = 'tmp_cfg'
        with self.assertRaises(ValueError):
            self._session.add_resource_config(resource_config=user_cfg)

        # retrieve resource labels and test them
        listed_resources = self._session.list_resources()

        self.assertIn('user_cfg.user_resource', listed_resources)
        self.assertIn('local.mpirun_cfg', listed_resources)
        self.assertIn(rp.RESOURCE_CONFIG_LABEL_DEFAULT, listed_resources)

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
