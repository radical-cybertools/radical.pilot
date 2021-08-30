
__copyright__ = 'Copyright 2017, http://radical.rutgers.edu'
__license__   = 'MIT'


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
    def schedule_task(self, task):

        # this abstract method is not used in this implementation
        assert(False)


    # --------------------------------------------------------------------------
    #
    def unschedule_task(self, task):

        # this abstract method is not used in this implementation
        assert(False)


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
        jds = [self.task_to_spec(task) for task in tasks]
        self._log.debug('==== submit tasks: %s', [jd for jd in jds])
        jids = self._lm.fh.submit_jobs([jd for jd in jds])
        self._log.debug('==== submitted tasks')

        for task, jid in zip(tasks, jids):
            self._log.debug('==== submit tasks %s -> %s', task['uid'], jid)
            task['flux_id'] = jid

        self._q.put(tasks)


    # --------------------------------------------------------------------------
    #
    def task_to_spec(self, task):

        td     = task['description']
        uid    = task['uid']
        sbox   = task['task_sandbox_path']
        stdout = td.get('stdout') or '%s/%s.out' % (sbox, uid)
        stderr = td.get('stderr') or '%s/%s.err' % (sbox, uid)

        cmd  = '%s %s 1>%s 2>%s' % (td['executable'], ' '.join(td['arguments']),
                                    stdout, stderr)
        spec = {
            'tasks': [{
                'slot' : 'task',
                'count': {
                    'per_slot': 1
                },
                'command': ['/bin/sh', '-c', cmd],
            }],
            'attributes': {
                'system': {
                    'cwd'     : task['task_sandbox_path'],
                    'duration': 0,
                }
            },
            'version': 1,
            'resources': [{
                'count': td['cpu_processes'],
                'type' : 'slot',
                'label': 'task',
                'with' : [{
                    'count': td['cpu_threads'],
                    'type' : 'core'
                # }, {
                #     'count': td['gpu_processes'],
                #     'type' : 'gpu'
                }]
            }]
        }

        if td['gpu_processes']:
            spec['resources']['with'].append({
                    'count': td['gpu_processes'],
                    'type' : 'gpu'})

        return spec

# ------------------------------------------------------------------------------

