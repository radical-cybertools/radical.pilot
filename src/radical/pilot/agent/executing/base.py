
__copyright__ = 'Copyright 2013-2016, http://radical.rutgers.edu'
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
EXECUTING_NAME_SHELL   = 'SHELL'
EXECUTING_NAME_SHELLFS = 'SHELLFS'
EXECUTING_NAME_FLUX    = 'FLUX'
EXECUTING_NAME_SLEEP   = 'SLEEP'
EXECUTING_NAME_FUNCS   = 'FUNCS'


# ------------------------------------------------------------------------------
#
class AgentExecutingComponent(rpu.Component):
    '''
    Manage the creation of CU processes, and watch them until they are completed
    (one way or the other).  The spawner thus moves the unit from
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

        # if so configured, let the CU know what to use as tmp dir
        self._tmp = cfg.get('cu_tmp', os.environ.get('TMP', '/tmp'))


    # --------------------------------------------------------------------------
    #
    # This class-method creates the appropriate sub-class for the Spawner
    #
    @classmethod
    def create(cls, cfg, session):

        name = cfg['spawner']

        # Make sure that we are the base-class!
        if cls != AgentExecutingComponent:
            raise TypeError('Factory only available to base class!')

        from .popen    import Popen
        from .shell    import Shell
        from .shell_fs import ShellFS
        from .flux     import Flux
        from .funcs    import FUNCS
        from .sleep    import Sleep

        try:
            impl = {
                EXECUTING_NAME_POPEN  : Popen,
                EXECUTING_NAME_SHELL  : Shell,
                EXECUTING_NAME_SHELLFS: ShellFS,
                EXECUTING_NAME_FLUX   : Flux,
                EXECUTING_NAME_SLEEP  : Sleep,
                EXECUTING_NAME_FUNCS  : FUNCS,
            }[name]
            return impl(cfg, session)

        except KeyError as e:
            raise RuntimeError('AgentExecutingComponent %s unknown' % name) \
                from e


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        self._log.debug('===== exec base initialize')

        self._pwd = os.getcwd()

        # The AgentExecutingComponent needs LaunchMethods to construct
        # commands.
        self._launchers    = dict()
        self._launch_order = None

        lm_cfg = self._cfg.resource_cfg.launch_methods
        for name, lmcfg in lm_cfg.items():

            if name == 'order':
                self._launch_order = lmcfg
                continue

            try:
                self._log.debug('===== %s create start', name)
                lm = rpa.LaunchMethod.create(name, self._cfg,
                                                   self._log, self._prof)
                self._launchers[name] = lm
                self._log.debug('===== %s create stop', name)
                self._log.debug('===== %s lm info: %s', name, lm._info)

            except:
                self._log.exception('skip LM %s' % name)


        assert(self._launchers)

        if not self._launch_order:
            self._launch_order = list(self._cfg.resource_cfg.launchers.keys())

        self.gtod = '%s/gtod' % self._pwd
        self.prof = '%s/prof' % self._pwd

        self._log.debug('===== exec register work start')
        self.register_input(rps.AGENT_EXECUTING_PENDING,
                            rpc.AGENT_EXECUTING_QUEUE, self.work)
        self._log.debug('===== exec register work stop')

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

