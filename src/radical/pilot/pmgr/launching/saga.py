# pylint: disable=protected-access

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os

import radical.utils as ru
import radical.saga  as rs

from .base import PilotLauncherBase
from ...   import states as rps


# ------------------------------------------------------------------------------
# local constants

PILOT_CHECK_INTERVAL    =  60  # seconds between runs of the job state check loop

# FIXME: duplication from base class, use `self._cfg`

PILOT_CANCEL_DELAY      = 120  # seconds between cancel signal and job kill
PILOT_CHECK_MAX_MISSES  =   3  # number of times to find a job missing before
                               # declaring it dead


# ------------------------------------------------------------------------------
#
class PilotLauncherSAGA(PilotLauncherBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, log, prof, state_cb):

        PilotLauncherBase.__init__(self, name, log, prof, state_cb)

        self._saga_jobs = dict()      # pid      : rs.Job
        self._saga_js   = dict()      # resource : rs.JobService
        self._saga_lock = ru.RLock()  # lock for above

        # FIXME: get session from launching component
        self._saga_session = rs.Session()

        # FIXME: make interval configurable
        # FIXME: run as daemon watcher thread
        #
      # self._watcher = mt.Thread(target=self._pilot_watcher)
      # self._watcher.daemon = True
      # self._watcher.start()


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # FIXME: terminate thread

        with self._saga_lock:

            # cancel pilots
            for _, job in self._saga_jobs:
                job.cancel()

            # close job services
            for url, js in self._saga_js.items():
                self._log.debug('close js %s', url)
                js.close()


    # --------------------------------------------------------------------------
    #
    def can_launch(self, rcfg, pilot):

        # SAGA can launch all pilots
        return True


    # --------------------------------------------------------------------------
    #
    def launch_pilots(self, rcfg, pilots):

        js_ep  = rcfg['job_manager_endpoint']
        with self._saga_lock:
            if js_ep in self._saga_js:
                js = self._saga_js[js_ep]
            else:
                js = rs.job.Service(js_ep, session=self._saga_session)
                self._saga_js[js_ep] = js

        # now that the scripts are in place and configured,
        # we can launch the agent
        jc = rs.job.Container()

        for pilot in pilots:

            jd_dict = pilot['jd_dict']

            saga_jd_supplement = dict()
            if 'saga_jd_supplement' in jd_dict:
                saga_jd_supplement = jd_dict['saga_jd_supplement']
                del(jd_dict['saga_jd_supplement'])


            jd = rs.job.Description()
            for k, v in jd_dict.items():
                jd.set_attribute(k, v)

            # set saga_jd_supplement if not already set
            for key, val in saga_jd_supplement.items():
                if not jd[key]:
                    self._log.debug('supplement %s: %s', key, val)
                    jd[key] = val

            # set saga job description attribute based on env variable(s)
            # FIXME: is this used?
            if os.environ.get('RADICAL_SAGA_SMT'):
                try:
                    jd.system_architecture['smt'] = \
                        int(os.environ['RADICAL_SAGA_SMT'])
                except Exception as e:
                    self._log.debug('SAGA SMT not set: %s' % e)

          # self._log.debug('jd: %s', pprint.pformat(jd.as_dict()))
            jc.add(js.create_job(jd))

        jc.run()

        # Order of tasks in `rs.job.Container().tasks` is not changing over the
        # time, thus it's able to iterate over it and other list(s) all together
        for j, pilot in zip(jc.get_tasks(), pilots):

            # do a quick error check
            if j.state == rs.FAILED:
                self._log.error('%s: %s : %s : %s', j.id, j.state, j.stderr, j.stdout)
                raise RuntimeError("SAGA Job state is FAILED. (%s)" % j.name)

            pid = pilot['uid']

            # FIXME: update the right pilot
            with self._saga_lock:
                self._saga_jobs[pid] = j


    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, pids):

        tc = rs.job.Container()

        for pid in pids:

            job   = self._saga_jobs[pid]
            pilot = self._pilots[pid]['pilot']

            # don't overwrite resource_details from the agent
            if 'resource_details' in pilot:
                del(pilot['resource_details'])

            if pilot['state'] in rps.FINAL:
                continue

            self._log.debug('plan cancellation of %s : %s', pilot, job)
            self._log.debug('request cancel for %s', pilot['uid'])
            tc.add(job)

        self._log.debug('cancellation start')
        tc.cancel()
        tc.wait()
        self._log.debug('cancellation done')


  # # --------------------------------------------------------------------------
  # #
  # def _pilot_watcher(self):
  #
  #     # FIXME: we should actually use SAGA job state notifications!
  #     # FIXME: check how race conditions are handles: we may detect
  #     #        a finalized SAGA job and change the pilot state -- but that
  #     #        pilot may have transitioned into final state via the normal
  #     #        notification mechanism already.  That probably should be sorted
  #     #        out by the pilot manager, which will receive notifications for
  #     #        both transitions.  As long as the final state is the same,
  #     #        there should be no problem anyway.  If it differs, the
  #     #        'cleaner' final state should prevail, in this ordering:
  #     #          cancel
  #     #          timeout
  #     #          error
  #     #          disappeared
  #     #        This implies that we want to communicate 'final_cause'
  #
  #     # we don't want to lock our members all the time.  For that reason we
  #     # use a copy of the pilots_tocheck list and iterate over that, and only
  #     # lock other members when they are manipulated.
  #     tc = rs.job.Container()
  #     with self._pilots_lock, self._check_lock:
  #
  #         for pid in self._checking:
  #             tc.add(self._pilots[pid]['job'])
  #
  #     states = tc.get_states()
  #
  #     self._log.debug('bulk states: %s', states)
  #
  #     # if none of the states is final, we have nothing to do.
  #     # We can't rely on the ordering of tasks and states in the task
  #     # container, so we hope that the task container's bulk state query lead
  #     # to a caching of state information, and we thus have cache hits when
  #     # querying the pilots individually
  #
  #     final_pilots = list()
  #     with self._pilots_lock, self._check_lock:
  #
  #         for pid in self._checking:
  #
  #             state = self._pilots[pid]['job'].state
  #             self._log.debug('saga job state: %s %s %s', pid,
  #                              self._pilots[pid]['job'],  state)
  #
  #             if state in [rs.job.DONE, rs.job.FAILED, rs.job.CANCELED]:
  #                 pilot = self._pilots[pid]['pilot']
  #                 if state == rs.job.DONE    : pilot['state'] = rps.DONE
  #                 if state == rs.job.FAILED  : pilot['state'] = rps.FAILED
  #                 if state == rs.job.CANCELED: pilot['state'] = rps.CANCELED
  #                 final_pilots.append(pilot)
  #
  #     if final_pilots:
  #
  #         for pilot in final_pilots:
  #
  #             with self._check_lock:
  #                 # stop monitoring this pilot
  #                 self._checking.remove(pilot['uid'])
  #
  #             self._log.debug('final pilot %s %s', pilot['uid'], pilot['state'])
  #
  #         self.advance(final_pilots, push=False, publish=True)
  #
  #     # all checks are done, final pilots are weeded out.  Now check if any
  #     # pilot is scheduled for cancellation and is overdue, and kill it
  #     # forcefully.
  #     to_cancel  = list()
  #     with self._pilots_lock:
  #
  #         for pid in self._pilots:
  #
  #             pilot   = self._pilots[pid]['pilot']
  #             time_cr = pilot.get('cancel_requested')
  #
  #             # check if the pilot is final meanwhile
  #             if pilot['state'] in rps.FINAL:
  #                 continue
  #
  #             if time_cr and time_cr + PILOT_CANCEL_DELAY < time.time():
  #                 self._log.debug('pilot needs killing: %s :  %s + %s < %s',
  #                         pid, time_cr, PILOT_CANCEL_DELAY, time.time())
  #                 del(pilot['cancel_requested'])
  #                 self._log.debug(' cancel pilot %s', pid)
  #                 to_cancel.append(pid)
  #
  #     if to_cancel:
  #         self._kill_pilots(to_cancel)
  #
  #     return True


# ------------------------------------------------------------------------------

