
__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

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

        session._log.debug('===== exec init start')

        self._uid = ru.generate_id(cfg['owner'] + '.executing.%(counter)s',
                                   ru.ID_CUSTOM)

        rpu.Component.__init__(self, cfg, session)
        session._log.debug('===== exec init stop')

        # if so configured, let the Task know what to use as tmp dir
        self._task_tmp = cfg.get('task_tmp', os.environ.get('TMP', '/tmp'))


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

        self._log.debug('===== exec base initialize')

        # The spawner/executor needs the ResourceManager information which have
        # been collected during agent startup.
        self._rm = rpa.ResourceManager.create(self._cfg.resource_manager,
                                              self._cfg, self._log, self._prof)

        # The AgentExecutingComponent needs LaunchMethods to construct
        # commands.
        self._launchers    = dict()
        self._launch_order = None

        self._log.debug('===== cfg ', self._cfg)
        launch_methods = self._cfg.resource_cfg.launch_methods
        for name, lm_cfg in launch_methods.items():

            if name == 'order':
                self._launch_order = lm_cfg
                continue

            try:
                self._log.debug('==== prepare lm %s', name)
                lm_cfg['pid']         = self._cfg.pid
                lm_cfg['reg_addr']    = self._cfg.reg_addr
                self._launchers[name] = rpa.LaunchMethod.create(
                    name, lm_cfg, self._rm.info, self._log, self._prof)
            except:
                self._log.exception('skip LM %s' % name)

        if not self._launchers:
            raise RuntimeError('no valid launch methods found')

        if not self._launch_order:
            self._launch_order = list(launch_methods.keys())

        self._pwd      = os.path.realpath(os.getcwd())
        self.sid       = self._cfg['sid']
        self.resource  = self._cfg['resource']
        self.rsbox     = self._cfg['resource_sandbox']
        self.ssbox     = self._cfg['session_sandbox']
        self.psbox     = self._cfg['pilot_sandbox']
        self.gtod      = '$RP_PILOT_SANDBOX/gtod'
        self.prof      = '$RP_PILOT_SANDBOX/prof'

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
    def find_launcher(self, task):

        for name in self._launch_order:
            launcher = self._launchers[name]
            self._log.debug('==== launcher %s: %s', name, launcher)
            if launcher.can_launch(task):
                return self._launchers[name]

        return None


# ------------------------------------------------------------------------------

