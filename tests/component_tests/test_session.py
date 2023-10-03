#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2020-2023, The RADICAL-Cybertools Team'
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
    @mock.patch.object(Session, '_get_logger')
    @mock.patch.object(Session, '_get_profiler')
    @mock.patch.object(Session, '_get_reporter')
    def setUpClass(cls, *args, **kwargs) -> None:

        def init_primary(self):
            self._reg = mock.Mock()
            self._init_cfg_from_scratch()

        with mock.patch.object(Session, '_init_primary', new=init_primary):
            cls._session = Session()
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

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Session, '_get_logger')
    @mock.patch.object(Session, '_get_profiler')
    @mock.patch.object(Session, '_get_reporter')
    def test_resource_schema_alias(self, *args, **kwargs):

        base_dir = os.path.join(os.path.expanduser('~'), '.radical')
        self._cleanup_files.append(base_dir)

        user_cfg_dir = os.path.join(base_dir, 'pilot', 'configs')
        ru.rec_makedir(user_cfg_dir)

        facility_cfg = {
            'test': {
                'default_schema'    : 'schema_origin',
                'schemas'           : {
                    'schema_origin'     : {'job_manager_hop': 'value_0'},
                    'schema_alias'      : 'schema_origin',
                    'schema_alias_alias': 'schema_alias'
                }
            }
        }
        ru.write_json(facility_cfg, '%s/resource_facility.json' % user_cfg_dir)

        def init_primary(self):
            self._reg = mock.Mock()
            self._init_cfg_from_scratch()

        with mock.patch.object(Session, '_init_primary', new=init_primary):
            s_alias = Session()
        self._cleanup_files.append(s_alias.uid)

        self.assertEqual(
            s_alias._rcfgs.facility.test.schema_origin,
            s_alias._rcfgs.facility.test.schema_alias)
        self.assertEqual(
            s_alias._rcfgs.facility.test.schema_origin,
            s_alias._rcfgs.facility.test.schema_alias_alias)
        self.assertEqual(
            s_alias.get_resource_config('facility.test', 'schema_origin'),
            s_alias.get_resource_config('facility.test', 'schema_alias_alias'))

        # schema alias refers to unknown schema
        facility_cfg = {
            'test': {
                'default_schema': 'schema_alias_error',
                'schemas': {
                    'schemas': ['schema_alias_error'],
                    'schema_alias_error': 'unknown_schema'
                }
            }
        }
        ru.write_json(facility_cfg, '%s/resource_facility.json' % user_cfg_dir)
        with self.assertRaises(KeyError):
            with mock.patch.object(Session, '_init_primary', new=init_primary):
                Session()

    # --------------------------------------------------------------------------
    #
    def test_close(self):

        class Dummy():
            def put(*args, **kwargs):
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


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestSession()
    tc.setUpClass()
    tc.test_list_resources()
    tc.test_get_resource_config()
    tc.test_resource_schema_alias()
    tc.test_get_resource_sandbox()

# ------------------------------------------------------------------------------

