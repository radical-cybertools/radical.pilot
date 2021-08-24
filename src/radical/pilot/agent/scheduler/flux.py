
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__   = "MIT"


import json

import radical.utils        as ru

from ...   import states    as rps
from ...   import constants as rpc

from ..launch_method import LaunchMethod
from .base           import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
class Flux(AgentSchedulingComponent):
    '''
    Pass all scheduling and execution control to Flux, and leave it to the
    execution component to collect state updates.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self.nodes = None

        AgentSchedulingComponent.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # don't advance tasks via the component's `advance()`, but push them
        # toward the executor *without state change* - state changes are
        # performed in retrospect by the executor, based on the scheduling and
        # execution events collected from Flux.
        qname   = rpc.AGENT_EXECUTING_QUEUE
        fname   = '%s/%s.cfg' % (self._cfg.path, qname)
        cfg     = ru.read_json(fname)
        self._q = ru.zmq.Putter(qname, cfg['put'])

        lm_cfg  = self._cfg.resource_cfg.launch_methods.get('FLUX')
        lm_cfg['pid']       = self._cfg.pid
        lm_cfg['reg_addr']  = self._cfg.reg_addr
        self._lm            = LaunchMethod.create('FLUX', lm_cfg, self._cfg,
                                                  self._log, self._prof)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        # overload the base class work method
        self._log.debug('==== submit tasks?')
        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)

        # FIXME: need actual job description, obviously
        jd   = ru.read_json('/home/merzky/projects/flux/spec.json')
        self._log.debug('==== submit tasks: %s', [jd for task in tasks])
        jids = self._lm.fh.submit_jobs([jd for task in tasks])
        self._log.debug('==== submitted tasks')

        for task, jid in zip(tasks, jids):
            self._log.debug('==== submit tasks %s -> %s', task['uid'], jid)
            task['flux_id'] = jid

        self._q.put(tasks)


  # # --------------------------------------------------------------------------
  # #
  # def _populate_task_environment(self):
  #     """Derive the environment for the t's from our own environment."""
  #
  #     # Get the environment of the agent
  #     new_env = copy.deepcopy(os.environ)
  #
  #     #
  #     # Mimic what virtualenv's "deactivate" would do
  #     #
  #     old_path = new_env.pop('_OLD_VIRTUAL_PATH', None)
  #     if old_path:
  #         new_env['PATH'] = old_path
  #
  #     old_ppath = new_env.pop('_OLD_VIRTUAL_PYTHONPATH', None)
  #     if old_ppath:
  #         new_env['PYTHONPATH'] = old_ppath
  #
  #     old_home = new_env.pop('_OLD_VIRTUAL_PYTHONHOME', None)
  #     if old_home:
  #         new_env['PYTHON_HOME'] = old_home
  #
  #     old_ps = new_env.pop('_OLD_VIRTUAL_PS1', None)
  #     if old_ps:
  #         new_env['PS1'] = old_ps
  #
  #     new_env.pop('VIRTUAL_ENV', None)
  #
  #     # Remove the configured set of environment variables from the
  #     # environment that we pass to Popen.
  #     for e in list(new_env.keys()):
  #         for r in self._lm.env_removables:
  #             if e.startswith(r):
  #                 new_env.pop(e, None)
  #
  #     return new_env


# ------------------------------------------------------------------------------

