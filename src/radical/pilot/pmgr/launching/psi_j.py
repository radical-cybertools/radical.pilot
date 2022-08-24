
__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'


# configure the psij logger (captured in the launch components stderr)
import logging
logging.basicConfig(level='DEBUG')

import threading as mt

from .base import PilotLauncherBase
from ...   import states as rps

# psij is optional
psij    = None
psij_ex = None

try:
    import psij
except ImportError as ex:
    psij_ex = ex


# ------------------------------------------------------------------------------
#
class PilotLauncherPSIJ(PilotLauncherBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, log, prof, state_cb):


        # psij is an optional dependency - let an import exception fall through
        # to disable this pilot launcher
        if psij_ex:
            raise psij_ex

        assert psij

        PilotLauncherBase.__init__(self, name, log, prof, state_cb)

        self._jobs   = dict()      # map pilot id to psi_j job
        self._pilots = dict()      # map psi_j id to pilot job
        self._jex    = dict()      # map launch schema to psi_j job executors
        self._lock   = mt.RLock()  # lock above structures


    # --------------------------------------------------------------------------
    #
    def _get_schema(self, rcfg):

        url     = rcfg['job_manager_endpoint']
        schemas = url.split(':')[0].split('+')

        if 'ssh' in schemas:
            schemas.remove('ssh')

        if 'gsissh' in schemas:
            schemas.remove('gsissh')

        if len(schemas) > 1:
            return

        if not schemas:
            return

        schema = schemas[0]

        if schema == 'fork':
            schema = 'local'

        return schema


    # --------------------------------------------------------------------------
    #
    def _translate_state(self, status):

        if   status.state == psij.JobState.NEW       : return rps.NEW
        elif status.state == psij.JobState.QUEUED    : return rps.PMGR_LAUNCHING
        elif status.state == psij.JobState.ACTIVE    : return rps.PMGR_ACTIVE
        elif status.state == psij.JobState.COMPLETED : return rps.DONE
        elif status.state == psij.JobState.FAILED    : return rps.FAILED
        elif status.state == psij.JobState.CANCELED  : return rps.CANCELED
        else:
            raise ValueError('cannot interpret psij state: %s' % repr(status))


    # --------------------------------------------------------------------------
    #
    def _job_status_cb(self, job, status):

        try:
            with self._lock:

                if job.id not in self._pilots:
                    return

                rp_state = self._translate_state(status)
                pilot    = self._pilots[job.id]

            self._state_cb(pilot, rp_state)

        except Exception:
            self._log.exception('job status callback failed')


    # --------------------------------------------------------------------------
    #
    def can_launch(self, rcfg, pilot):

        schema = self._get_schema(rcfg)

        if not schema:
            return False

        if schema not in self._jex:

            self._log.debug('create executor for %s', schema)
            try:
                self._jex[schema] = psij.JobExecutor.get_instance(schema)
                self._jex[schema].set_job_status_callback(self._job_status_cb)
            except:
                self._log.exception('failed to create psij executor')
                return False

        return True


    # --------------------------------------------------------------------------
    #
    def launch_pilots(self, rcfg, pilots):

        assert psij

        for pilot in pilots:

            pid    = pilot['uid']
            schema = self._get_schema(rcfg)
            assert schema

            jex = self._jex.get(schema)

            assert jex

            jd = pilot['jd_dict']

            proj, res = None, None
            if jd.project:
                if ':' in jd.project:
                    proj, res = jd.project.split(':', 1)
                else:
                    proj = jd.project

            attr = psij.JobAttributes()
            attr.duration       = jd.wall_time_limit
            attr.queue_name     = jd.queue
            attr.project_name   = proj
            attr.reservation_id = res

            self._log.debug('=== rt: %s', jd.runtime)

            spec = psij.JobSpec()
            spec.attributes  = attr
            spec.executable  = jd.executable
            spec.arguments   = jd.arguments
            spec.environment = jd.environment
            spec.directory   = jd.working_directory
            spec.stdout_path = jd.output
            spec.stderr_path = jd.error

            spec.resources   = psij.ResourceSpecV1()
            spec.resources.node_count            = jd.node_count
            spec.resources.process_count         = jd.total_cpu_count
          # spec.resources.cpu_cores_per_process = 1
          # spec.resources.gpu_cores_per_process = jd.total_gpu_count

            job = psij.Job(spec)

            self._jobs[pid]      = job
            self._pilots[job.id] = pilot
            self._log.debug('added %s: %s', job.id, pid)

            jex.submit(job)


    # --------------------------------------------------------------------------
    #
    def kill_pilots(self, pids):

        for pid in pids:
            if pid not in pids:
                continue

            self._jobs[pid].cancel()


# ------------------------------------------------------------------------------

