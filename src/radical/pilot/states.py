
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"


import os as _os


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
_pilot_state_inv   = {_v: _k for _k, _v in _pilot_state_values.items()}
_pilot_state_inv_full = dict()
for _st,_v in _pilot_state_values.items():
    if _v in _pilot_state_inv_full:
        if not isinstance(_pilot_state_inv_full[_v], list):
            _pilot_state_inv_full[_v] = [_pilot_state_inv_full[_v]]
        _pilot_state_inv_full[_v].append(_st)
    else:
        _pilot_state_inv_full[_v] = _st


def _pilot_state_value(s):
    return _pilot_state_values[s]


def _pilot_state_progress(pid, current, target):
    """
    See documentation of 'task_state_progress' below.
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
# task states
#
TMGR_SCHEDULING_PENDING      = 'TMGR_SCHEDULING_PENDING'
TMGR_SCHEDULING              = 'TMGR_SCHEDULING'
TMGR_STAGING_INPUT_PENDING   = 'TMGR_STAGING_INPUT_PENDING'
TMGR_STAGING_INPUT           = 'TMGR_STAGING_INPUT'
AGENT_STAGING_INPUT_PENDING  = 'AGENT_STAGING_INPUT_PENDING'
AGENT_STAGING_INPUT          = 'AGENT_STAGING_INPUT'
AGENT_SCHEDULING_PENDING     = 'AGENT_SCHEDULING_PENDING'
AGENT_SCHEDULING             = 'AGENT_SCHEDULING'
AGENT_EXECUTING_PENDING      = 'AGENT_EXECUTING_PENDING'
AGENT_EXECUTING              = 'AGENT_EXECUTING'
AGENT_STAGING_OUTPUT_PENDING = 'AGENT_STAGING_OUTPUT_PENDING'
AGENT_STAGING_OUTPUT         = 'AGENT_STAGING_OUTPUT'
TMGR_STAGING_OUTPUT_PENDING  = 'TMGR_STAGING_OUTPUT_PENDING'
TMGR_STAGING_OUTPUT          = 'TMGR_STAGING_OUTPUT'

# assign numeric values to states to support state ordering operations
# ONLY final state get the same values.
_task_state_values = {
        None                         : -1,
        NEW                          :  0,
        TMGR_SCHEDULING_PENDING      :  1,
        TMGR_SCHEDULING              :  2,
        TMGR_STAGING_INPUT_PENDING   :  3,
        TMGR_STAGING_INPUT           :  4,
        AGENT_STAGING_INPUT_PENDING  :  5,
        AGENT_STAGING_INPUT          :  6,
        AGENT_SCHEDULING_PENDING     :  7,
        AGENT_SCHEDULING             :  8,
        AGENT_EXECUTING_PENDING      :  9,
        AGENT_EXECUTING              : 10,
        AGENT_STAGING_OUTPUT_PENDING : 11,
        AGENT_STAGING_OUTPUT         : 12,
        TMGR_STAGING_OUTPUT_PENDING  : 13,
        TMGR_STAGING_OUTPUT          : 14,
        DONE                         : 15,
        FAILED                       : 15,
        CANCELED                     : 15}
_task_state_inv = {_v: _k for _k, _v in _task_state_values.items()}
_task_state_inv_full = dict()
for _st,_v in _task_state_values.items():
    if _v in _task_state_inv_full:
        if not isinstance(_task_state_inv_full[_v], list):
            _task_state_inv_full[_v] = [_task_state_inv_full[_v]]
        _task_state_inv_full[_v].append(_st)
    else:
        _task_state_inv_full[_v] = _st


def _task_state_value(s):
    return _task_state_values[s]


def _task_state_progress(uid, current, target):
    """
    This method will ensure a task state progression in sync with the state
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

    task_state_progress(TMGR_SCHEDULING, NEW)
    --> [TMGR_SCHEDULING, []]

    task_state_progress(NEW, NEW)
    --> [NEW, []]

    task_state_progress(NEW, TMGR_SCHEDULING_PENDING)
    --> [TMGR_SCHEDULING_PENDING, [TMGR_SCHEDULING_PENDING]]

    task_state_progress(NEW, TMGR_SCHEDULING)
    --> [TMGR_SCHEDULING, [TMGR_SCHEDULING_PENDING, TMGR_SCHEDULING]]

    task_state_progress(DONE, FAILED)
    --> [DONE, []]

    """

    # first handle final state corrections
    if current == CANCELED:
        if target in [DONE, FAILED, CANCELED]:
            return[target, []]

    if current in FINAL:
        if target in FINAL:
            raise ValueError('invalid transition for %s: %s -> %s' % (uid, current, target))

    cur = _task_state_values[current]
    tgt = _task_state_values[target]

    if cur >= tgt:
        # nothing to do, a similar or better progression happened earlier
        return [current, []]

    # dig out all intermediate states, skip current
    passed = list()
    for i in range(cur + 1,tgt):
        passed.append(_task_state_inv[i])

    # append target state to trigger notification of transition
    passed.append(target)

    return(target, passed)


def _task_state_collapse(states):
    """
    This method takes a list of task states and selects the one with the highest
    state value.
    """
    # we first check the final states, as we want to express a preference there.
    # Then we start comparing actual values.
    if DONE     in states: return DONE
    if FAILED   in states: return FAILED
    if CANCELED in states: return CANCELED
    ret = None
    for state in states:
        if _task_state_values[state] > _task_state_values[ret]:
            ret = state
    return ret


# -----------------------------------------------------------------------------
# backward compatibility
#
# pilot states
if 'RP_ENABLE_OLD_DEFINES' in _os.environ:
    CANCELING              = CANCELED
    PENDING                = PMGR_LAUNCHING_PENDING
    PENDING_LAUNCH         = PMGR_LAUNCHING_PENDING
    LAUNCHING              = PMGR_LAUNCHING
    PENDING_ACTIVE         = PMGR_ACTIVE_PENDING
    ACTIVE_PENDING         = PMGR_ACTIVE_PENDING
    ACTIVE                 = PMGR_ACTIVE

    # task states
    UNSCHEDULED            = TMGR_SCHEDULING_PENDING
    SCHEDULING             = TMGR_SCHEDULING
    PENDING_INPUT_STAGING  = TMGR_STAGING_INPUT_PENDING
    STAGING_INPUT          = TMGR_STAGING_INPUT
    ALLOCATING_PENDING     = AGENT_SCHEDULING_PENDING
    ALLOCATING             = AGENT_SCHEDULING
    PENDING_EXECUTION      = AGENT_EXECUTING_PENDING
    EXECUTING_PENDING      = AGENT_EXECUTING_PENDING
    EXECUTING              = AGENT_EXECUTING
    PENDING_OUTPUT_STAGING = TMGR_STAGING_OUTPUT_PENDING
    STAGING_OUTPUT         = TMGR_STAGING_OUTPUT

_legacy_states = {
    'New'                        : NEW,
    'AllocatingPending'          : TMGR_SCHEDULING_PENDING,
    'Allocating'                 : TMGR_SCHEDULING,
    'PendingInputStaging'        : TMGR_STAGING_INPUT_PENDING,
    'StagingInput'               : TMGR_STAGING_INPUT,
    'AgentStagingInputPending'   : AGENT_STAGING_INPUT_PENDING,
    'AgentStagingInput'          : AGENT_STAGING_INPUT,
    'ExecutingPending'           : AGENT_EXECUTING_PENDING,
    'Executing'                  : AGENT_EXECUTING,
    'AgentStagingOutputPending'  : AGENT_STAGING_OUTPUT_PENDING,
    'AgentStagingOutput'         : AGENT_STAGING_OUTPUT,
    'PendingOutputStaging'       : TMGR_STAGING_OUTPUT_PENDING,
    'StagingOutput'              : TMGR_STAGING_OUTPUT,
    'Done'                       : DONE,
    'Canceled'                   : CANCELED,
    'CANCELED'                   : CANCELED,
    'Failed'                     : FAILED,
    'FAILED'                     : FAILED
}


# backward compatibility

UMGR_SCHEDULING_PENDING      = TMGR_SCHEDULING_PENDING
UMGR_SCHEDULING              = TMGR_SCHEDULING
UMGR_STAGING_INPUT_PENDING   = TMGR_STAGING_INPUT_PENDING
UMGR_STAGING_INPUT           = TMGR_STAGING_INPUT
UMGR_STAGING_OUTPUT_PENDING  = TMGR_STAGING_OUTPUT_PENDING
UMGR_STAGING_OUTPUT          = TMGR_STAGING_OUTPUT


# -----------------------------------------------------------------------------

