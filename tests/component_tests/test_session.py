#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2020-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import glob
import os
import shutil
import tempfile

import radical.utils as ru

from unittest import TestCase, mock

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class TestSession(TestCase):

    _cleanup_files = []

    # --------------------------------------------------------------------------
    #
    @classmethod
    @mock.patch.object(rp.Session, '_get_logger')
    @mock.patch.object(rp.Session, '_get_profiler')
    @mock.patch.object(rp.Session, '_get_reporter')
    def setUpClass(cls, *args, **kwargs) -> None:

        def init_primary(self):
            self._reg = mock.Mock()
            self._init_cfg_from_scratch()

        with mock.patch.object(rp.Session, '_init_primary', new=init_primary):
            cls._session = rp.Session(uid='rp.session.cls_test')
            cls._cleanup_files.append(cls._session.uid)

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls) -> None:

        for p in cls._cleanup_files:
            if not p:
                continue
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

        default_schema = rcfg.default_schema
        self.assertEqual(rcfg.job_manager_endpoint,
                         rcfg.schemas[default_schema].job_manager_endpoint)
        new_schema = 'gsissh'
        rcfg = self._session.get_resource_config(rcfg_label, schema=new_schema)
        self.assertEqual(rcfg.job_manager_endpoint,
                         rcfg.schemas[new_schema].job_manager_endpoint)

        # check exceptions

        with self.assertRaises(RuntimeError):
            self._session.get_resource_config(resource='wrong_domain.host')

        with self.assertRaises(RuntimeError):
            self._session.get_resource_config(resource='local.wrong_host')

        with self.assertRaises(RuntimeError):
            self._session.get_resource_config(
                resource='local.localhost', schema='wrong_schema')

        # check running from batch

        from radical.pilot.resource_config import ENDPOINTS_DEFAULT
        saved_batch_id = os.getenv('SLURM_JOB_ID')

        # resource manager is Slurm

        os.environ['SLURM_JOB_ID'] = '12345'
        rcfg = self._session.get_resource_config(rcfg_label)
        for e_key, e_value in ENDPOINTS_DEFAULT.items():
            self.assertEqual(rcfg[e_key], e_value)

        del os.environ['SLURM_JOB_ID']
        rcfg = self._session.get_resource_config(rcfg_label)
        for e_key, e_value in ENDPOINTS_DEFAULT.items():
            self.assertNotEqual(rcfg[e_key], e_value)

        if saved_batch_id is not None:
            os.environ['SLURM_JOB_ID'] = saved_batch_id


    # --------------------------------------------------------------------------
    #
    def test_close(self):

        class Dummy():
            def put(self, *args, **kwargs):
                pass

        # check default values
        self.assertFalse(self._session._close_options.download)
        self.assertTrue(self._session._close_options.terminate)

        self._session._ctrl_pub    = Dummy()
        self._session._hb          = mock.Mock()
        self._session._hb_pubsub   = mock.Mock()
        self._session._reg_service = mock.Mock()

        # only `True` values are targeted
        self._session.close(download=True)
        self._session.close(terminate=True)

    # --------------------------------------------------------------------------
    #
    def test_get_resource_sandbox(self):

        pilot = {'uid'        : 'pilot.0000',
                 'description': {}}

        with self.assertRaises(ValueError):
            # `PilotDescription.resource` is not provided
            self._session._get_resource_sandbox(pilot=pilot)

        # check `default_remote_workdir` handling

        # ORNL: split `project` by "_"
        pilot['description'].update({'resource': 'ornl.summit',
                                     'project' : 'PROJNAME_machine'})
        self.assertIn('/projname/',
                      self._session._get_resource_sandbox(pilot).path)
        self._session._cache['resource_sandbox'] = {}

        # ORNL: no any splitting
        pilot['description'].update({'resource': 'ornl.summit',
                                     'project' : 'PROJNAME'})
        self.assertIn('/projname/',
                      self._session._get_resource_sandbox(pilot).path)
        self._session._cache['resource_sandbox'] = {}

        # NCSA: split `project` by "-"
        pilot['description'].update({'resource': 'ncsa.delta',
                                     'project' : 'bbka-delta-cpu'})
        self.assertIn('/bbka/',
                      self._session._get_resource_sandbox(pilot).path)
        self._session._cache['resource_sandbox'] = {}

        # NCSA: no splitting
        pilot['description'].update({'resource': 'ncsa.delta',
                                     'project' : 'bbka_wrongsplitter'})
        self.assertNotIn('/bbka/',
                         self._session._get_resource_sandbox(pilot).path)
        self._session._cache['resource_sandbox'] = {}

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(rp.Session, '_get_reporter')
    @mock.patch('os.getcwd', return_value=tempfile.mkdtemp())
    def test_paths(self, mocked_getcwd, mocked_reporter):

        work_dir = mocked_getcwd()
        self._cleanup_files.append(work_dir)

        def init_primary(session):
            session._reg = mock.Mock()
            session._init_cfg_from_scratch()

        s_uid = 'test.session.0000'
        path  = '/tmp/rp_tests_%s' % os.getuid()
        ru.rec_makedir(path)

        with mock.patch.object(rp.Session, '_init_primary', new=init_primary):
            s0 = rp.Session(uid=s_uid, cfg={'base': ''})

        self.assertEqual(s0.uid, s_uid)
        self.assertEqual(s0.base, work_dir)

        for path_key in ['base', 'path', 'client_sandbox']:
            with mock.patch.object(rp.Session, '_init_primary', new=init_primary):

                s = rp.Session(uid=s_uid, cfg={path_key: path, 'base': path})
                self.assertEqual(s.cfg[path_key], path)

                self._cleanup_files.append(s.path)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestSession()
    tc.setUpClass()
    tc.test_list_resources()
    tc.test_get_resource_config()
    tc.test_get_resource_sandbox()

# ------------------------------------------------------------------------------

