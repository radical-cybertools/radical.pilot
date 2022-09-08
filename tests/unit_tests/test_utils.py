# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os
import shutil

from unittest import TestCase

import radical.utils        as ru
import radical.pilot.states as rps

import radical.pilot.utils.db_utils   as rpu_db
import radical.pilot.utils.prof_utils as rpu_prof
import radical.pilot.utils.misc       as rpu_misc


# ------------------------------------------------------------------------------
#
class TestUtils(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls):
        for p in ['./rp.session.*', '/tmp/rp_cache_*']:
            for d in glob.glob(p):
                if os.path.isdir(d):
                    try:
                        shutil.rmtree(d)
                    except OSError as e:
                        print('[ERROR] %s - %s' % (e.filename, e.strerror))

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
    def test_get_session_docs(self):

        with self.assertRaises(AssertionError):
            rpu_db.get_session_docs('unknown_sid', db=None, cachedir=None)

        sid = 'rp.session.0000'
        ru.rec_makedir(sid)

        session_json = {
            'pilot': [{'uid': 'pilot.0000'}],
            'task' : [{'uid': 'task.0000', 'pilot': 'pilot.0000'}]
        }
        session_file = os.path.join(os.path.abspath(sid), sid + '.json')
        ru.write_json(session_json, session_file)
        self.assertTrue(os.path.exists(session_file))

        cache = os.path.join(rpu_db._CACHE_BASEDIR, '%s.json' % sid)

        # no cache dir and no cache file
        self.assertFalse(os.path.exists(rpu_db._CACHE_BASEDIR))
        self.assertFalse(os.path.exists(cache))

        json_data = rpu_db.get_session_docs(sid, db=None, cachedir=None)

        # cache file was created
        self.assertTrue(os.path.isfile(cache))
        self.assertEqual(json_data, ru.read_json(cache))

        # read from cache
        self.assertEqual(json_data, rpu_db.get_session_docs(sid))

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
        self.assertEqual(str(rfs_url), rcfg_local.local.filesystem_endpoint)

        # switched default access schema, which is the first in the list
        rpu_misc._rcfgs.local.localhost.schemas = ['ssh', 'local']
        rfs_url = rpu_misc.get_resource_fs_url('local.localhost')
        self.assertEqual(str(rfs_url), rcfg_local.ssh.filesystem_endpoint)

        rfs_url = rpu_misc.get_resource_fs_url(resource='xsede.bridges2',
                                               schema='gsissh')
        self.assertEqual(str(rfs_url),
                         rcfgs.xsede.bridges2.gsissh.filesystem_endpoint)

        # test resource job URL

        rj_url = rpu_misc.get_resource_job_url('local.localhost')
        self.assertIsInstance(rj_url, ru.Url)
        schema_default = rpu_misc._rcfgs.local.localhost.schemas[0]
        self.assertEqual(str(rj_url),
                         rcfg_local[schema_default].job_manager_endpoint)

        rj_url = rpu_misc.get_resource_job_url(resource='xsede.bridges2',
                                               schema='gsissh')
        self.assertEqual(str(rj_url),
                         rcfgs.xsede.bridges2.gsissh.job_manager_endpoint)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestUtils()
    tc.test_convert_sdurations()
    tc.test_expand_sduration()
    tc.test_get_session_docs()
    tc.test_resource_cfg()

# ------------------------------------------------------------------------------

