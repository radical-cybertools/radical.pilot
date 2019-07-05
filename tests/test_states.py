
# pylint: disable=protected-access, unused-argument


# ------------------------------------------------------------------------------
#
def test_pilot_state_collapse():
    from radical.pilot.states import _pilot_state_collapse

    states = ['NEW',
              'PMGR_LAUNCHING',
              'PMGR_LAUNCHING_PENDING',
              'PMGR_ACTIVE',
              'PMGR_ACTIVE_PENDING']
    assert(_pilot_state_collapse(states) == 'PMGR_ACTIVE')

    states = ['NEW',
              'PMGR_LAUNCHING',
              'DONE',
              'PMGR_ACTIVE',
              'PMGR_ACTIVE_PENDING']
    assert(_pilot_state_collapse(states) == 'DONE')

    states = ['NEW',
              'FAILED',
              'PMGR_LAUNCHING_PENDING',
              'PMGR_ACTIVE',
              'PMGR_ACTIVE_PENDING']
    assert(_pilot_state_collapse(states) == 'FAILED')
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
def test_unit_state_collapse():
    from radical.pilot.states import _unit_state_collapse
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
