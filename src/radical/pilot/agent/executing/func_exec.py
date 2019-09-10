
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import radical.utils as ru

from ...   import states    as rps
from ...   import constants as rpc

from .base import AgentExecutingComponent


# ==============================================================================
#
class FuncExec(AgentExecutingComponent) :

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        AgentExecutingComponent.__init__ (self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def initialize_child(self):
        '''
        register input and output for the agent component pipeline, and
        register input and output for the agent executor network.
        '''

        # agent component bridges
        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.request)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        # agent control pubsub
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)

        # func executor bridges
        self.register_publisher('func_exec_requested')
        self.register_subscriber('func_response', self.response)


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):
        '''
        We don't support any commands, yet
        '''

        self._log.info('command_cb [%s]: %s (ignored)', topic, msg)

      # cmd = msg['cmd']
      # arg = msg['arg']

        return True


    # --------------------------------------------------------------------------
    #
    def request(self, units):
        '''
        pick units from the component pipeline 
        and forward them to the func executors
        '''

        if not isinstance(units, list):
            units = [units]

        self.advance(units, rps.AGENT_EXECUTING, publish=True, push=False)

        # break up bulks so that func executors can work concurrently
        for unit in units:
            unit['state'] = 'func_exec_requested'
            self._req.put(unit)


    # --------------------------------------------------------------------------
    #
    def response(self, units):
        '''
        collect completed units
        and push them back into the agent's component pipeline
        '''

        for unit in units:

            assert(unit['state'] == 'func_exec_responded')

            if unit['func_err']: unit['target_state'] = rps.FAILED
            else               : unit['target_state'] = rps.DONE

        self.advance(units, rps.AGENT_STAGING_OUTPUT_PENDING, 
                     publish=True, push=True)

        return bool(len(units))


# ------------------------------------------------------------------------------

