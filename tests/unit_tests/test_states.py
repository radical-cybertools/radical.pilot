
# pylint: disable=unused-argument

from unittest import TestCase

from radical.pilot.states import _pilot_state_collapse
from radical.pilot.states import _task_state_collapse


class TestStates(TestCase):

    # ------------------------------------------------------------------------------
    #
    def test_pilot_state_collapse(self):

        states = ['NEW',
                  'PMGR_LAUNCHING',
                  'PMGR_LAUNCHING_PENDING',
                  'PMGR_ACTIVE',
                  'PMGR_ACTIVE_PENDING']
        self.assertEqual(_pilot_state_collapse(states), 'PMGR_ACTIVE')

        states = ['NEW',
                  'PMGR_LAUNCHING',
                  'DONE',
                  'PMGR_ACTIVE',
                  'PMGR_ACTIVE_PENDING']
        self.assertEqual(_pilot_state_collapse(states), 'DONE')

        states = ['NEW',
                  'FAILED',
                  'PMGR_LAUNCHING_PENDING',
                  'PMGR_ACTIVE',
                  'PMGR_ACTIVE_PENDING']
        self.assertEqual(_pilot_state_collapse(states), 'FAILED')


    # ------------------------------------------------------------------------------
    #
    def test_task_state_collapse(self):
        states = ['AGENT_STAGING_OUTPUT',
                  'AGENT_EXECUTING_PENDING',
                  'TMGR_SCHEDULING',
                  'TMGR_STAGING_OUTPUT',
                  'TMGR_STAGING_OUTPUT_PENDING',
                  'AGENT_SCHEDULING',
                  'AGENT_STAGING_OUTPUT_PENDING',
                  'AGENT_SCHEDULING_PENDING']
        self.assertEqual(_task_state_collapse(states), 'TMGR_STAGING_OUTPUT')

        states =  ['DONE',
                   'TMGR_STAGING_INPUT_PENDING',
                   'AGENT_SCHEDULING_PENDING',
                   'TMGR_SCHEDULING_PENDING',
                   'AGENT_STAGING_OUTPUT_PENDING',
                   'AGENT_EXECUTING',
                   'AGENT_STAGING_INPUT',
                   'FAILED']
        self.assertEqual(_task_state_collapse(states), 'DONE')

        states = ['AGENT_STAGING_OUTPUT_PENDING',
                  'CANCELED',
                  'TMGR_STAGING_OUTPUT_PENDING',
                  'TMGR_STAGING_INPUT',
                  'NEW',
                  'AGENT_SCHEDULING',
                  'AGENT_STAGING_INPUT',
                  'TMGR_SCHEDULING']
        self.assertEqual(_task_state_collapse(states), 'CANCELED')


# ------------------------------------------------------------------------------

if __name__ == '__main__':

    tc = TestStates()
    tc.test_task_state_collapse()
    tc.test_pilot_state_collapse()
