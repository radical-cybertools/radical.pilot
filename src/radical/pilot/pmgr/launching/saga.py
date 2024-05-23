# pylint: disable=protected-access

__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import functools

import threading as mt

# saga is optional
rs    = None
rs_ex = None

try:
    import radical.saga
    rs = radical.saga
except ImportError as ex:
    rs_ex = ex

import radical.utils as ru

from .base import PilotLauncherBase
from ...   import states as rps


# ------------------------------------------------------------------------------
#
class PilotLauncherSAGA(PilotLauncherBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, log, prof, state_cb):

        if rs_ex:
            raise rs_ex

        assert rs

        PilotLauncherBase.__init__(self, name, log, prof, state_cb)

        self._jobs   = dict()      # pid      : rs.Job
        self._js     = dict()      # resource : rs.JobService
        self._pilots = dict()      # saga_id  : pilot job
        self._lock   = mt.RLock()  # lock for above


        # FIXME: get session from launching component
        self._session = rs.Session()


    # --------------------------------------------------------------------------
    #
    def _translate_state(self, saga_state):

        if   saga_state == rs.job.NEW       : return rps.NEW
        elif saga_state == rs.job.PENDING   : return rps.PMGR_LAUNCHING
        elif saga_state == rs.job.RUNNING   : return rps.PMGR_LAUNCHING
        elif saga_state == rs.job.SUSPENDED : return rps.PMGR_LAUNCHING
        elif saga_state == rs.job.DONE      : return rps.DONE
        elif saga_state == rs.job.FAILED    : return rps.FAILED
        elif saga_state == rs.job.CANCELED  : return rps.CANCELED
        else:
            raise ValueError('cannot interpret psij state: %s' % saga_state)


    # --------------------------------------------------------------------------
    #
    def _job_state_cb(self, job, _, saga_state, pid):

        self._log.debug('job state: %s %s %s', pid, saga_state, job.id)

        try:
            with self._lock:

                if pid not in self._pilots:
                    return

                rp_state = self._translate_state(saga_state)
                pilot    = self._pilots[pid]

            self._state_cb(pilot, rp_state)

        except Exception:
            self._log.exception('job status callback failed')

        return True


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # FIXME: terminate thread

        with self._lock:

            # cancel pilots
            for job in self._jobs.values():
                job.cancel()

            # close job services
            for url, js in self._js.items():
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
        self._log.debug('js_ep: %s', js_ep)
        with self._lock:
            if js_ep in self._js:
                js = self._js[js_ep]
            else:
                js = rs.job.Service(js_ep)
                self._js[js_ep] = js

        # now that the scripts are in place and configured,
        # we can launch the agent
        jc = rs.job.Container()

        for pilot in pilots:

            jd_dict = pilot['jd_dict']

            # we need to quote arguments for RS to handle
            jd_dict['arguments'] = [ru.sh_quote(arg)
                                       for arg in jd_dict['arguments']]

            saga_jd_supplement = dict()
            if 'saga_jd_supplement' in jd_dict:
                saga_jd_supplement = jd_dict['saga_jd_supplement']
                del jd_dict['saga_jd_supplement']

            # saga will take care of node_count itself
            if 'node_count' in jd_dict:
                del jd_dict['node_count']

            jd = rs.job.Description()
            for k, v in jd_dict.items():
                jd.set_attribute(k, v)

            # set saga_jd_supplement if not already set
            for key, val in saga_jd_supplement.items():
                if not jd[key]:
                    self._log.debug('supplement %s: %s', key, val)
                    jd[key] = val

            # remember the pilot
            pid = pilot['uid']
            self._pilots[pid] = pilot

            job = js.create_job(jd)
            cb  = functools.partial(self._job_state_cb, pid=pid)
            job.add_callback(rs.STATE, cb)
            jc.add(job)

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
            with self._lock:
                self._jobs[pid] = j


    # --------------------------------------------------------------------------
    #
    def kill_pilots(self, pids):

        tc = rs.job.Container()

        for pid in pids:

            job   = self._jobs[pid]
            pilot = self._pilots[pid]

            # don't overwrite resource_details from the agent
            if 'resource_details' in pilot:
                del pilot['resource_details']

            if pilot['state'] in rps.FINAL:
                continue

            self._log.debug('plan cancellation of %s : %s', pilot, job)
            self._log.debug('request cancel for %s', pilot['uid'])
            tc.add(job)

        self._log.debug('cancellation start')
        tc.cancel()
        tc.wait()

        for pid in pids:
            pilot = self._pilots[pid]
            self._state_cb(pilot, rps.CANCELED)

        self._log.debug('cancellation done')


# ------------------------------------------------------------------------------

