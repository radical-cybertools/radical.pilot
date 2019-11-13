
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os


# -----------------------------------------------------------------------------
# common states
NEW      = 'NEW'
DONE     = 'DONE'
FAILED   = 'FAILED'
CANCELED = 'CANCELED'

# shortcut
INITIAL  = [NEW]
FINAL    = [DONE, FAILED, CANCELED]


# ------------------------------------------------------------------------------
#
# pilot states
#
PMGR_LAUNCHING_PENDING = 'PMGR_LAUNCHING_PENDING'
PMGR_LAUNCHING         = 'PMGR_LAUNCHING'
PMGR_ACTIVE_PENDING    = 'PMGR_ACTIVE_PENDING'
PMGR_ACTIVE            = 'PMGR_ACTIVE'

# assign numeric values to states to support state ordering operations
# ONLY final state get the same values.
_pilot_state_values = {
        None                   : -1,
        NEW                    :  0,
        PMGR_LAUNCHING_PENDING :  1,
        PMGR_LAUNCHING         :  2,
        PMGR_ACTIVE_PENDING    :  3,
        PMGR_ACTIVE            :  4,
        DONE                   :  5,
        FAILED                 :  5,
        CANCELED               :  5}
_pilot_state_inv   = {v: k for k, v in _pilot_state_values.items()}
_pilot_state_inv_full = dict()
for st,v in _pilot_state_values.items():
    if v in _pilot_state_inv_full:
        if not isinstance(_pilot_state_inv_full[v], list):
            _pilot_state_inv_full[v] = [_pilot_state_inv_full[v]]
        _pilot_state_inv_full[v].append(st)
    else:
        _pilot_state_inv_full[v] = st


def _pilot_state_value(s):
    return _pilot_state_values[s] 


def _pilot_state_progress(pid, current, target):
    """
    See documentation of 'unit_state_progress' below.
    """

    # first handle final state corrections
    if current == CANCELED:
        if target in [DONE, FAILED, CANCELED]:
            return[target, []]

    # allow to transition from FAILED to DONE (done gets picked up from DB,
    # sometimes after pilot watcher detects demise)
    if current == FAILED:
        if target in [DONE, FAILED]:
            return[target, []]

    if current in FINAL and target != current:
        if target in FINAL:
            raise ValueError('invalid transition for %s: %s -> %s' % (pid, current, target))

    cur = _pilot_state_values[current]
    tgt = _pilot_state_values[target]

    if cur >= tgt:
        # nothing to do, a similar or better progression happened earlier
        return [current, []]

    # dig out all intermediate states, skip current
    passed = list()
    for i in range(cur + 1,tgt):
        passed.append(_pilot_state_inv[i])

    # append target state to trigger notification of transition
    passed.append(target)

    return(target, passed)


def _pilot_state_collapse(states):
    """
    This method takes a list of pilot states and selects the one with the highest
    state value.
    """
    # we first check the final states, as we want to express a preference there.
    # Then we start comparing actual values.
    if DONE     in states: return DONE
    if FAILED   in states: return FAILED
    if CANCELED in states: return CANCELED
    ret = None
    for state in states:
        if _pilot_state_values[state] > _pilot_state_values[ret]:
            ret = state
    return ret


# ------------------------------------------------------------------------------
# 
# unit states
#
UMGR_SCHEDULING_PENDING      = 'UMGR_SCHEDULING_PENDING'
UMGR_SCHEDULING              = 'UMGR_SCHEDULING'
UMGR_STAGING_INPUT_PENDING   = 'UMGR_STAGING_INPUT_PENDING'
UMGR_STAGING_INPUT           = 'UMGR_STAGING_INPUT'
AGENT_STAGING_INPUT_PENDING  = 'AGENT_STAGING_INPUT_PENDING'
AGENT_STAGING_INPUT          = 'AGENT_STAGING_INPUT'
AGENT_SCHEDULING_PENDING     = 'AGENT_SCHEDULING_PENDING'
AGENT_SCHEDULING             = 'AGENT_SCHEDULING'
AGENT_EXECUTING_PENDING      = 'AGENT_EXECUTING_PENDING'
AGENT_EXECUTING              = 'AGENT_EXECUTING'
AGENT_STAGING_OUTPUT_PENDING = 'AGENT_STAGING_OUTPUT_PENDING'
AGENT_STAGING_OUTPUT         = 'AGENT_STAGING_OUTPUT'
UMGR_STAGING_OUTPUT_PENDING  = 'UMGR_STAGING_OUTPUT_PENDING'
UMGR_STAGING_OUTPUT          = 'UMGR_STAGING_OUTPUT'

# assign numeric values to states to support state ordering operations
# ONLY final state get the same values.
_unit_state_values = {
        None                         : -1,
        NEW                          :  0,
        UMGR_SCHEDULING_PENDING      :  1,
        UMGR_SCHEDULING              :  2,
        UMGR_STAGING_INPUT_PENDING   :  3,
        UMGR_STAGING_INPUT           :  4,
        AGENT_STAGING_INPUT_PENDING  :  5,
        AGENT_STAGING_INPUT          :  6,
        AGENT_SCHEDULING_PENDING     :  7,
        AGENT_SCHEDULING             :  8,
        AGENT_EXECUTING_PENDING      :  9,
        AGENT_EXECUTING              : 10,
        AGENT_STAGING_OUTPUT_PENDING : 11,
        AGENT_STAGING_OUTPUT         : 12,
        UMGR_STAGING_OUTPUT_PENDING  : 13,
        UMGR_STAGING_OUTPUT          : 14,
        DONE                         : 15,
        FAILED                       : 15,
        CANCELED                     : 15}
_unit_state_inv = {v: k for k, v in _unit_state_values.items()}
_unit_state_inv_full = dict()
for st,v in _unit_state_values.items():
    if v in _unit_state_inv_full:
        if not isinstance(_unit_state_inv_full[v], list):
            _unit_state_inv_full[v] = [_unit_state_inv_full[v]]
        _unit_state_inv_full[v].append(st)
    else:
        _unit_state_inv_full[v] = st


def _unit_state_value(s):
    return _unit_state_values[s] 


def _unit_state_progress(uid, current, target):
    """
    This method will ensure a unit state progression in sync with the state
    model defined above.  It will return a tuple: [new_state, passed_states]
    where 'new_state' is either 'target' or 'current', depending which comes
    later in the state model, and 'passed_states' is a sorted list of all states
    between 'current' (excluded) and 'new_state' (included).  If a progression
    is not possible because the 'target' state value is equal or smaller that
    the 'current' state, then 'current' is returned, and the list of passed
    states remains empty.  This way, 'passed_states' can be used to invoke
    callbacks for all state transition, where each transition is announced
    exactly once.  

    Note that the call will not allow to transition between states of equal
    values, which in particular applies to final states -- those are truly
    final.  Requesting such transitions will result in silent discard of the
    invalid target state.

    On contradicting final states, DONE and FAILED are preferred over CANCELED,
    but no other transitions are allowed

    unit_state_progress(UMGR_SCHEDULING, NEW)
    --> [UMGR_SCHEDULING, []]

    unit_state_progress(NEW, NEW)
    --> [NEW, []]

    unit_state_progress(NEW, UMGR_SCHEDULING_PENDING)
    --> [UMGR_SCHEDULING_PENDING, [UMGR_SCHEDULING_PENDING]]

    unit_state_progress(NEW, UMGR_SCHEDULING)
    --> [UMGR_SCHEDULING, [UMGR_SCHEDULING_PENDING, UMGR_SCHEDULING]]

    unit_state_progress(DONE, FAILED)
    --> [DONE, []]

    """

    # first handle final state corrections
    if current == CANCELED:
        if target in [DONE, FAILED, CANCELED]:
            return[target, []]

    if current in FINAL:
        if target in FINAL:
            raise ValueError('invalid transition for %s: %s -> %s' % (uid, current, target))

    cur = _unit_state_values[current]
    tgt = _unit_state_values[target]

    if cur >= tgt:
        # nothing to do, a similar or better progression happened earlier
        return [current, []]

    # dig out all intermediate states, skip current
    passed = list()
    for i in range(cur + 1,tgt):
        passed.append(_unit_state_inv[i])

    # append target state to trigger notification of transition
    passed.append(target)

    return(target, passed)


def _unit_state_collapse(states):
    """
    This method takes a list of unit states and selects the one with the highest
    state value.
    """
    # we first check the final states, as we want to express a preference there.
    # Then we start comparing actual values.
    if DONE     in states: return DONE
    if FAILED   in states: return FAILED
    if CANCELED in states: return CANCELED
    ret = None
    for state in states:
        if _unit_state_values[state] > _unit_state_values[ret]:
            ret = state
    return ret


# -----------------------------------------------------------------------------
# backward compatibility
#
# pilot states
if 'RP_ENABLE_OLD_DEFINES' in os.environ:
    CANCELING              = CANCELED
    PENDING                = PMGR_LAUNCHING_PENDING
    PENDING_LAUNCH         = PMGR_LAUNCHING_PENDING
    LAUNCHING              = PMGR_LAUNCHING
    PENDING_ACTIVE         = PMGR_ACTIVE_PENDING
    ACTIVE_PENDING         = PMGR_ACTIVE_PENDING
    ACTIVE                 = PMGR_ACTIVE

    # compute unit states
    UNSCHEDULED            = UMGR_SCHEDULING_PENDING
    SCHEDULING             = UMGR_SCHEDULING
    PENDING_INPUT_STAGING  = UMGR_STAGING_INPUT_PENDING
    STAGING_INPUT          = UMGR_STAGING_INPUT
    ALLOCATING_PENDING     = AGENT_SCHEDULING_PENDING
    ALLOCATING             = AGENT_SCHEDULING
    PENDING_EXECUTION      = AGENT_EXECUTING_PENDING
    EXECUTING_PENDING      = AGENT_EXECUTING_PENDING
    EXECUTING              = AGENT_EXECUTING
    PENDING_OUTPUT_STAGING = UMGR_STAGING_OUTPUT_PENDING
    STAGING_OUTPUT         = UMGR_STAGING_OUTPUT

_legacy_states = {
    'New'                        : NEW, 
    'AllocatingPending'          : UMGR_SCHEDULING_PENDING,
    'Allocating'                 : UMGR_SCHEDULING,
    'PendingInputStaging'        : UMGR_STAGING_INPUT_PENDING,
    'StagingInput'               : UMGR_STAGING_INPUT,
    'AgentStagingInputPending'   : AGENT_STAGING_INPUT_PENDING,
    'AgentStagingInput'          : AGENT_STAGING_INPUT,
    'ExecutingPending'           : AGENT_EXECUTING_PENDING,
    'Executing'                  : AGENT_EXECUTING,
    'AgentStagingOutputPending'  : AGENT_STAGING_OUTPUT_PENDING,
    'AgentStagingOutput'         : AGENT_STAGING_OUTPUT,
    'PendingOutputStaging'       : UMGR_STAGING_OUTPUT_PENDING,
    'StagingOutput'              : UMGR_STAGING_OUTPUT,
    'Done'                       : DONE, 
    'Canceled'                   : CANCELED,
    'CANCELED'                   : CANCELED,
    'Failed'                     : FAILED,
    'FAILED'                     : FAILED
}


# -----------------------------------------------------------------------------

