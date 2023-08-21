#!/usr/bin/env python3

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


    def se_init(self):

        self._rep   = mock.Mock()
        self._reg   = mock.Mock()
        self._log   = mock.Mock()
        self._prof  = mock.Mock()

        self._rcfgs = ru.Config('radical.pilot.resource', name='*',
                                expand=False)

        for site in self._rcfgs:
            for rcfg in self._rcfgs[site].values():
                for schema in rcfg.get('schemas', []):
                    while isinstance(rcfg.get(schema), str):
                        tgt = rcfg[schema]
                        rcfg[schema] = rcfg[tgt]


    # --------------------------------------------------------------------------
    #
    @classmethod
    @mock.patch.object(Session, '_init_primary', side_effect=se_init,
                       autospec=True)
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
    @mock.patch.object(Session, '_init_primary', side_effect=se_init,
                       autospec=True)
    @mock.patch.object(Session, '_get_logger')
    @mock.patch.object(Session, '_get_profiler')
    @mock.patch.object(Session, '_get_reporter')
    @mock.patch('radical.pilot.session.ru.Config')
    def test_resource_schema_alias(self, mocked_config, *args, **kwargs):

        mocked_config.return_value = ru.TypedDict({
            'local': {
                'test': {
                    'default_schema'    : 'schema_origin',
                    'schemas'           : {
                        'schema_origin'     : {'param_0': 'value_0'},
                        'schema_alias'      : 'schema_origin',
                        'schema_alias_alias': 'schema_alias'
                    }
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
    def test_close(self):

        class Dummy():
            def put(*args, **kwargs):
                pass

        # check default values
        self.assertFalse(self._session._close_options.download)
        self.assertTrue(self._session._close_options.terminate)

        # only `True` values are targeted

        self._session._ctrl_pub = Dummy()
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

