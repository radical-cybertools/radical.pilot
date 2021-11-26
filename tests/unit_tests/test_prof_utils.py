
from unittest import TestCase

import radical.utils as ru
from radical.pilot import states as s
from radical.pilot.utils.prof_utils import _expand_sduration, _convert_sdurations


class TestDurations(TestCase):

    # ------------------------------------------------------------------------------
    #
    def test_expand_sduration(self):

        duration_0 = {'STATE': s.NEW}
        duration_1 = {'EVENT': 'event_name'}
        duration_2 = {'MSG': 'message_name'}
        duration_3 = {ru.EVENT: 'arbitrary', ru.STATE: 'arbitrary'}
        duration_4 = {'X': 'arbitrary'}
        duration_5 = {'X': 'arbitrary', 'Y': 'arbitrary', 'Z': 'arbitrary'}

        self.assertEqual(_expand_sduration(duration_0),
            {ru.EVENT: 'state', ru.STATE: s.NEW})
        self.assertEqual(_expand_sduration(duration_1),
            {ru.EVENT: 'event_name', ru.STATE: None})
        self.assertEqual(_expand_sduration(duration_2),
            {ru.EVENT: 'cmd', ru.MSG: 'message_name'})
        self.assertEqual(_expand_sduration(duration_3),
            {ru.EVENT: 'arbitrary', ru.STATE: 'arbitrary'})
        with self.assertRaises(Exception):
            _expand_sduration(duration_4)
        with self.assertRaises(Exception):
            _expand_sduration(duration_5)


    # ------------------------------------------------------------------------------
    #
    def test_convert_sdurations(self):

        durations_0 = {'name_of_duration': [{'STATE': s.NEW},
                                            {'EVENT': 'event_name'}]}
        durations_1 = {'name_of_duration': [{'STATE': s.NEW},
                                            [{'EVENT': 'event_name'},
                                             {'STATE': s.NEW}]]}
        durations_2 = {'name_of_duration': [{'STATE': s.NEW},
                                            {'MSG': 'message_name'}]}
        durations_3 = {'name_of_duration': [{ru.EVENT: 'arbitrary',
                                             ru.STATE: 'arbitrary'},
                                            {ru.EVENT: 'arbitrary',
                                             ru.STATE: 'arbitrary'}]}

        self.assertEqual(_convert_sdurations(durations_0),
            {'name_of_duration': [{ru.EVENT: 'state',
                                   ru.STATE: s.NEW},
                                  {ru.EVENT: 'event_name',
                                   ru.STATE: None}]})
        self.assertEqual(_convert_sdurations(durations_1),
            {'name_of_duration': [{ru.EVENT: 'state',
                                   ru.STATE: s.NEW},
                                  [{ru.EVENT: 'event_name',
                                    ru.STATE: None},
                                   {ru.EVENT: 'state',
                                    ru.STATE: s.NEW}]]})
        self.assertEqual(_convert_sdurations(durations_2),
            {'name_of_duration': [{ru.EVENT: 'state',
                                   ru.STATE: s.NEW},
                                  {ru.EVENT: 'cmd',
                                   ru.MSG: 'message_name'}]})
        self.assertEqual(_convert_sdurations(durations_3),
            {'name_of_duration': [{ru.EVENT: 'arbitrary',
                                   ru.STATE: 'arbitrary'},
                                  {ru.EVENT: 'arbitrary',
                                   ru.STATE: 'arbitrary'}]})


if __name__ == '__main__':

    tc = TestDurations()
    tc.test_convert_sdurations()
    tc.test_expand_sduration()
