
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

        import flux

        flux_url   = self._cfg['lm_info']['flux_env']['FLUX_URI']
        self._flux = flux.Flux(url=flux_url)

        # don't advance tasks via the component's `advance()`, but push them
        # toward the executor *without state change* - state changes are
        # performed in retrospect by the executor, based on the scheduling and
        # execution events collected from Flux.
        qname   = rpc.AGENT_EXECUTING_QUEUE
        fname   = '%s/%s.cfg' % (self._cfg.path, qname)
        cfg     = ru.read_json(fname)
        self._q = ru.zmq.Putter(qname, cfg['put'])

        # create job spec via the flux LM
        self._lm = LaunchMethod.create(name    = 'FLUX',
                                       cfg     = self._cfg,
                                       session = self._session)


    # --------------------------------------------------------------------------
    #
    def work(self, tasks):

        # overload the base class work method

        from flux import job as flux_job

        self.advance(tasks, rps.AGENT_SCHEDULING, publish=True, push=False)

        for task in tasks:

          # # FIXME: transfer from executor
          # self._task_environment = self._populate_task_environment()

            jd  = json.dumps(ru.read_json('/home/merzky/projects/flux/spec.json'))
            jid = flux_job.submit(self._flux, jd)
            task['flux_id'] = jid

            # publish without state changes - those are retroactively applied
            # based on flux event timestamps.
            # TODO: apply some bulking, submission is not really fast.
            #       But at the end performance is determined by flux now, so
            #       communication only affects timelyness of state updates.
            self._q.put(task)


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

