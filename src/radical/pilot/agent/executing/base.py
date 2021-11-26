
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from ... import states    as rps
from ... import agent     as rpa
from ... import constants as rpc
from ... import utils     as rpu


# ------------------------------------------------------------------------------
# 'enum' for RP's spawner types
EXECUTING_NAME_POPEN   = 'POPEN'
EXECUTING_NAME_FLUX    = 'FLUX'
EXECUTING_NAME_SLEEP   = 'SLEEP'
EXECUTING_NAME_FUNCS   = 'FUNCS'


# ------------------------------------------------------------------------------
#
class AgentExecutingComponent(rpu.Component):
    '''
    Manage the creation of Task processes, and watch them until they are
    completed (one way or the other).  The spawner thus moves the task from
    PendingExecution to Executing, and then to a final state (or PendingStageOut
    of course).
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        rpu.Component.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Spawner
    #
    @classmethod
    def create(cls, cfg, session):

        # Make sure that we are the base-class!
        if cls != AgentExecutingComponent:
            raise TypeError('Factory only available to base class!')

        name = cfg['spawner']

        from .popen    import Popen
        from .flux     import Flux
        from .funcs    import FUNCS
        from .sleep    import Sleep

        impl = {
            EXECUTING_NAME_POPEN  : Popen,
            EXECUTING_NAME_FLUX   : Flux,
            EXECUTING_NAME_SLEEP  : Sleep,
            EXECUTING_NAME_FUNCS  : FUNCS,
        }

        if name not in impl:
            raise ValueError('AgentExecutingComponent %s unknown' % name)

        return impl[name](cfg, session)



    # --------------------------------------------------------------------------
    #
    def initialize(self):

      # self._log.debug('exec base initialize')

        # The spawner/executor needs the ResourceManager information which have
        # been collected during agent startup.
        self._rm = rpa.ResourceManager.create(self._cfg.resource_manager,
                                              self._cfg, self._log, self._prof)

        self._pwd      = os.path.realpath(os.getcwd())
        self.sid       = self._cfg['sid']
        self.resource  = self._cfg['resource']
        self.rsbox     = self._cfg['resource_sandbox']
        self.ssbox     = self._cfg['session_sandbox']
        self.psbox     = self._cfg['pilot_sandbox']
        self.gtod      = '$RP_PILOT_SANDBOX/gtod'
        self.prof      = '$RP_PILOT_SANDBOX/prof'

        # if so configured, let the Task know what to use as tmp dir
        self._task_tmp = self._cfg.get('task_tmp',
                                       os.environ.get('TMP', '/tmp'))


        if self.psbox.startswith(self.ssbox):
            self.psbox = '$RP_SESSION_SANDBOX%s'  % self.psbox[len(self.ssbox):]
        if self.ssbox.startswith(self.rsbox):
            self.ssbox = '$RP_RESOURCE_SANDBOX%s' % self.ssbox[len(self.rsbox):]
        if self.ssbox.endswith(self.sid):
            self.ssbox = '%s$RP_SESSION_ID/'      % self.ssbox[:-len(self.sid)]

        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)

        self.register_output(rps.AGENT_STAGING_OUTPUT_PENDING,
                             rpc.AGENT_STAGING_OUTPUT_QUEUE)

        self.register_publisher (rpc.AGENT_UNSCHEDULE_PUBSUB)
        self.register_subscriber(rpc.CONTROL_PUBSUB, self.command_cb)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        raise NotImplementedError('work is not implemented')


    # --------------------------------------------------------------------------
    #
    def command_cb(self, topic, msg):

        raise NotImplementedError('work is not implemented')


# ------------------------------------------------------------------------------

