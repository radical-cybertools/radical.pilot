# ------------------------------------------------------------------------------
#
def test_expand_sduration():
    import radical.utils as ru
    from radical.pilot import states as s
    from radical.pilot.utils.prof_utils import _expand_sduration

    duration_0 = {'STATE': s.NEW}
    duration_1 = {'EVENT': 'event_name'}
    duration_2 = {'MSG': 'message_name'}
    duration_3 = {ru.EVENT: 'arbitrary', ru.STATE: 'arbitrary'}

    assert(_expand_sduration(duration_0) ==
        {ru.EVENT: 'state', ru.STATE: s.NEW})
    assert(_expand_sduration(duration_1) ==
        {ru.EVENT: 'event_name', ru.STATE: None})
    assert(_expand_sduration(duration_2) ==
        {ru.EVENT: 'cmd', ru.MSG: 'message_name'})
    assert(_expand_sduration(duration_3) ==
        {ru.EVENT: 'arbitrary', ru.STATE: 'arbitrary'})

# ------------------------------------------------------------------------------
#
def test__convert_sdurations():
    import radical.utils as ru
    from radical.pilot import states as s
    from radical.pilot.utils.prof_utils import _expand_sduration
    from radical.pilot.utils.prof_utils import _convert_sdurations

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

    assert(_convert_sdurations(durations_0) ==
        {'name_of_duration': [{ru.EVENT: 'state',
                               ru.STATE: s.NEW},
                              {ru.EVENT: 'event_name',
                               ru.STATE: None}]})
    assert(_convert_sdurations(durations_1) ==
        {'name_of_duration': [{ru.EVENT: 'state',
                               ru.STATE: s.NEW},
                              [{ru.EVENT: 'event_name',
                                ru.STATE: None},
                               {ru.EVENT: 'state',
                                ru.STATE: s.NEW}]]})
    assert(_convert_sdurations(durations_2) ==
        {'name_of_duration': [{ru.EVENT: 'state',
                               ru.STATE: s.NEW},
                              {ru.EVENT: 'cmd',
                               ru.MSG: 'message_name'}]})
    assert(_convert_sdurations(durations_3) ==
        {'name_of_duration': [{ru.EVENT: 'arbitrary',
                               ru.STATE: 'arbitrary'},
                              {ru.EVENT: 'arbitrary',
                               ru.STATE: 'arbitrary'}]})
