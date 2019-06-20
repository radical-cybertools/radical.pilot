
# pylint: disable=protected-access, unused-argument


# ------------------------------------------------------------------------------
#
def test_pilot_state_value():
    from radical.pilot.states import _pilot_state_value
    assert(_pilot_state_value('NEW')                    == 0)
    assert(_pilot_state_value('PMGR_LAUNCHING_PENDING') == 1)
    assert(_pilot_state_value('PMGR_LAUNCHING')         == 2)
    assert(_pilot_state_value('PMGR_ACTIVE_PENDING')    == 3)
    assert(_pilot_state_value('PMGR_ACTIVE')            == 4)
    assert(_pilot_state_value('DONE')                   == 5)
    assert(_pilot_state_value('FAILED')                 == 5)
    assert(_pilot_state_value('CANCELED')               == 5)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
def test_pilot_state_collapse():
    from radical.pilot.states import _pilot_state_collapse,_pilot_state_values

    for key in _pilot_state_values.iterkeys():
        assert(_pilot_state_collapse([key]) == key)
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
def test_unit_state_value():
    from radical.pilot.states import _unit_state_value

    assert(_unit_state_value('NEW')                          ==  0)
    assert(_unit_state_value('UMGR_SCHEDULING_PENDING')      ==  1)
    assert(_unit_state_value('UMGR_SCHEDULING')              ==  2)
    assert(_unit_state_value('UMGR_STAGING_INPUT_PENDING')   ==  3)
    assert(_unit_state_value('UMGR_STAGING_INPUT')           ==  4)
    assert(_unit_state_value('AGENT_STAGING_INPUT_PENDING')  ==  5)
    assert(_unit_state_value('AGENT_STAGING_INPUT')          ==  6)
    assert(_unit_state_value('AGENT_SCHEDULING_PENDING')     ==  7)
    assert(_unit_state_value('AGENT_SCHEDULING')             ==  8)
    assert(_unit_state_value('AGENT_EXECUTING_PENDING')      ==  9)
    assert(_unit_state_value('AGENT_EXECUTING')              == 10)
    assert(_unit_state_value('AGENT_STAGING_OUTPUT_PENDING') == 11)
    assert(_unit_state_value('AGENT_STAGING_OUTPUT')         == 12)
    assert(_unit_state_value('UMGR_STAGING_OUTPUT_PENDING')  == 13)
    assert(_unit_state_value('UMGR_STAGING_OUTPUT')          == 14)
    assert(_unit_state_value('DONE')                         == 15)
    assert(_unit_state_value('FAILED')                       == 15)
    assert(_unit_state_value('CANCELED')                     == 15)

# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
def test_unit_state_collapse():
    from radical.pilot.states import _unit_state_collapse, _unit_state_values
    states = ['AGENT_STAGING_OUTPUT',
              'AGENT_EXECUTING_PENDING',
              'UMGR_SCHEDULING',
              'UMGR_STAGING_OUTPUT',
              'UMGR_STAGING_OUTPUT_PENDING',
              'AGENT_SCHEDULING',
              'AGENT_STAGING_OUTPUT_PENDING',
              'AGENT_SCHEDULING_PENDING']
    assert(_unit_state_collapse(states) == 'UMGR_STAGING_OUTPUT')

    states =  ['DONE',
               'UMGR_STAGING_INPUT_PENDING',
               'AGENT_SCHEDULING_PENDING',
               'UMGR_SCHEDULING_PENDING',
               'AGENT_STAGING_OUTPUT_PENDING',
               'AGENT_EXECUTING',
               'AGENT_STAGING_INPUT',
               'FAILED']
    assert(_unit_state_collapse(states) == 'DONE')

    states = ['AGENT_STAGING_OUTPUT_PENDING',
              'CANCELED',
              'UMGR_STAGING_OUTPUT_PENDING',
              'UMGR_STAGING_INPUT',
              'NEW',
              'AGENT_SCHEDULING',
              'AGENT_STAGING_INPUT',
              'UMGR_SCHEDULING']
    assert(_unit_state_collapse(states) == 'CANCELED')
# ------------------------------------------------------------------------------
