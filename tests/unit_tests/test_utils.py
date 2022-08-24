# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os
import shutil

from unittest import TestCase

import radical.utils        as ru
import radical.pilot.states as rps

import radical.pilot.utils.db_utils   as rpu_db
import radical.pilot.utils.prof_utils as rpu_prof


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


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestUtils()
    tc.test_convert_sdurations()
    tc.test_expand_sduration()
    tc.test_get_session_docs()

# ------------------------------------------------------------------------------

