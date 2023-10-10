#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import glob
import shutil

from unittest import TestCase

import radical.utils        as ru
import radical.pilot.states as rps

import radical.pilot.utils.prof_utils as rpu_prof
import radical.pilot.utils.misc       as rpu_misc

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestUtils(TestCase):

    _cleanup_files = []

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
    def test_expand_sduration(self):

        duration_0 = {'STATE': rps.NEW}
        duration_1 = {'EVENT': 'event_name'}
        duration_2 = {'MSG': 'message_name'}
        duration_3 = {ru.EVENT: 'arbitrary', ru.STATE: 'arbitrary'}
        duration_4 = {'X': 'arbitrary'}
        duration_5 = {'X': 'arbitrary', 'Y': 'arbitrary', 'Z': 'arbitrary'}

        self.assertEqual(
            rpu_prof._expand_sduration(duration_0),
            {ru.EVENT: 'state', ru.STATE: rps.NEW})
        self.assertEqual(
            rpu_prof._expand_sduration(duration_1),
            {ru.EVENT: 'event_name', ru.STATE: None})
        self.assertEqual(
            rpu_prof._expand_sduration(duration_2),
            {ru.EVENT: 'cmd', ru.MSG: 'message_name'})
        self.assertEqual(
            rpu_prof._expand_sduration(duration_3),
            {ru.EVENT: 'arbitrary', ru.STATE: 'arbitrary'})
        with self.assertRaises(Exception):
            rpu_prof._expand_sduration(duration_4)
        with self.assertRaises(Exception):
            rpu_prof._expand_sduration(duration_5)

    # --------------------------------------------------------------------------
    #
    def test_convert_sdurations(self):

        durations_0 = {'name_of_duration': [{'STATE': rps.NEW},
                                            {'EVENT': 'event_name'}]}
        durations_1 = {'name_of_duration': [{'STATE': rps.NEW},
                                            [{'EVENT': 'event_name'},
                                             {'STATE': rps.NEW}]]}
        durations_2 = {'name_of_duration': [{'STATE': rps.NEW},
                                            {'MSG': 'message_name'}]}
        durations_3 = {'name_of_duration': [{ru.EVENT: 'arbitrary',
                                             ru.STATE: 'arbitrary'},
                                            {ru.EVENT: 'arbitrary',
                                             ru.STATE: 'arbitrary'}]}

        self.assertEqual(
            rpu_prof._convert_sdurations(durations_0),
            {'name_of_duration': [{ru.EVENT: 'state',
                                   ru.STATE: rps.NEW},
                                  {ru.EVENT: 'event_name',
                                   ru.STATE: None}]})
        self.assertEqual(
            rpu_prof._convert_sdurations(durations_1),
            {'name_of_duration': [{ru.EVENT: 'state',
                                   ru.STATE: rps.NEW},
                                  [{ru.EVENT: 'event_name',
                                    ru.STATE: None},
                                   {ru.EVENT: 'state',
                                    ru.STATE: rps.NEW}]]})
        self.assertEqual(
            rpu_prof._convert_sdurations(durations_2),
            {'name_of_duration': [{ru.EVENT: 'state',
                                   ru.STATE: rps.NEW},
                                  {ru.EVENT: 'cmd',
                                   ru.MSG: 'message_name'}]})
        self.assertEqual(
            rpu_prof._convert_sdurations(durations_3),
            {'name_of_duration': [{ru.EVENT: 'arbitrary',
                                   ru.STATE: 'arbitrary'},
                                  {ru.EVENT: 'arbitrary',
                                   ru.STATE: 'arbitrary'}]})

    # --------------------------------------------------------------------------
    #
    def test_get_session_json(self):

        with self.assertRaises(AssertionError):
            rpu_prof.get_session_json('unknown_sid', cachedir=None)

        sid = 'rp.session.test_rputils.0001'
        ru.rec_makedir(sid)

        session_json = {
            'pilot': [{'uid': 'pilot.0000'}],
            'task' : [{'uid': 'task.0000', 'pilot': 'pilot.0000'}]
        }
        session_file = os.path.join(os.path.abspath(sid), sid + '.json')
        ru.write_json(session_json, session_file)
        self.assertTrue(os.path.exists(session_file))

        cache = os.path.join(rpu_prof._CACHE_BASEDIR, '%s.json' % sid)

        # no cache file
        self.assertFalse(os.path.exists(cache))
        # if cache dir doesn't exist then set it for "cleanup"
        if not os.path.exists(rpu_prof._CACHE_BASEDIR):
            self._cleanup_files.append(rpu_prof._CACHE_BASEDIR)

        json_data = rpu_prof.get_session_json(sid, cachedir=None)

        # cache file was created
        self.assertTrue(os.path.isfile(cache))
        self.assertEqual(json_data, ru.read_json(cache))

        self.assertEqual(session_json['pilot'][0]['uid'],
                         json_data['pilot'][0]['uid'])
        self.assertIn('task_ids', json_data['pilot'][0])

        # read from cache
        self.assertEqual(json_data, rpu_prof.get_session_json(sid))

        # set dirs and files for cleanup
        self._cleanup_files.extend([sid, cache])

    # --------------------------------------------------------------------------
    #
    def test_get_session_profile(self):

        sid  = 'rp.session.0'
        src  = '%s/test_cases/' % base
        prof = rpu_prof.get_session_profile(sid, src)

        found_bs0_stop = False
        for event in prof[0]:
            if event[1] == 'bootstrap_0_stop':
                found_bs0_stop = True
                break

        assert found_bs0_stop

    # --------------------------------------------------------------------------
    #
    def test_resource_cfg(self):

        # test list of resource configs

        self.assertIsNone(rpu_misc._rcfgs)

        rcfgs = rpu_misc.get_resource_configs()
        self.assertIsInstance(rcfgs, ru.Config)

        self.assertEqual(rcfgs, rpu_misc._rcfgs)
        self.assertIsInstance(rcfgs.as_dict(), dict)

        # test a single resource config

        rcfg_none = rpu_misc.get_resource_config('unknown_site.resource')
        self.assertIsNone(rcfg_none)

        rcfg_none = rpu_misc.get_resource_config('local.unknown_resource')
        self.assertIsNone(rcfg_none)

        rcfg_local = rpu_misc.get_resource_config('local.localhost')
        self.assertEqual(rcfg_local, rcfgs.local.localhost)

        # test resource filesystem URL

        rfs_url = rpu_misc.get_resource_fs_url('local.localhost')
        self.assertIsInstance(rfs_url, ru.Url)
        self.assertEqual(str(rfs_url), rcfg_local.schemas.local.filesystem_endpoint)

        # switched default access schema, which is the first in the list
        rpu_misc._rcfgs.local.localhost.default_schema = 'ssh'
        rfs_url = rpu_misc.get_resource_fs_url('local.localhost')
        self.assertEqual(str(rfs_url), rcfg_local.schemas.ssh.filesystem_endpoint)

        rfs_url = rpu_misc.get_resource_fs_url(resource='access.bridges2',
                                               schema='gsissh')
        self.assertEqual(str(rfs_url),
                         rcfgs.access.bridges2.schemas.gsissh.filesystem_endpoint)

        # test resource job URL

        rj_url = rpu_misc.get_resource_job_url('local.localhost')
        self.assertIsInstance(rj_url, ru.Url)
        schema_default = rpu_misc._rcfgs.local.localhost.default_schema
        self.assertEqual(str(rj_url),
                         rcfg_local.schemas[schema_default].job_manager_endpoint)

        rj_url = rpu_misc.get_resource_job_url(resource='access.bridges2',
                                               schema='gsissh')
        self.assertEqual(str(rj_url),
                         rcfgs.access.bridges2.schemas.gsissh.job_manager_endpoint)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestUtils()
    tc.test_convert_sdurations()
    tc.test_expand_sduration()
    tc.test_get_session_json()
    tc.test_get_session_profile()
    tc.test_resource_cfg()


# ------------------------------------------------------------------------------

