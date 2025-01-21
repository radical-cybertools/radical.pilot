
__copyright__ = 'Copyright 2022-2024, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import datetime

# configure the psij logger (captured in the launch components stderr)
import logging
logging.basicConfig(level='DEBUG')

import threading as mt

from .base import PilotLauncherBase
from ...   import states as rps

SCHEMA_LOCAL = 'local'

# psij is optional
psij    = None
psij_ex = None

try:
    import psij
except ImportError as ex:
    psij_ex = ex

OPTIONS_MAPPING = {
    'slurm': {'key': 'constraint',  'delim': '&'},
    'pbs'  : {'key': 'l',           'delim': ','},
    'lsf'  : {'key': 'alloc_flags', 'delim': ' '}
}
SCHEMA_ALIAS = {
    'pbspro': 'pbs'
}


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

        if schema in SCHEMA_ALIAS:
            schema = SCHEMA_ALIAS[schema]

        if schema == 'fork':
            schema = SCHEMA_LOCAL

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

            # we don't report PMGR+ACTIVE here - that information comes from the
            # pilot itself once it is up and running
            if rp_state != rps.PMGR_ACTIVE:
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

            if jd.project and ':' in jd.project:
                proj, res = jd.project.split(':', 1)
            else:
                proj, res = jd.project, None

            if jd.queue and ':' in jd.queue:
                part, qos = jd.queue.split(':', 1)
            else:
                part, qos = jd.queue, None

            attr = psij.JobAttributes()
            attr.duration       = datetime.timedelta(minutes=jd.wall_time_limit)
            attr.queue_name     = part
            attr.project_name   = proj
            attr.reservation_id = res

            if qos:
                attr.set_custom_attribute(name=f'{schema}.qos', value=qos)

            # define batch options/constraints
            if (jd.system_architecture.get('options') and
                    schema in OPTIONS_MAPPING):
                opts = OPTIONS_MAPPING[schema]
                attr.set_custom_attribute(
                    name=f'{schema}.{opts["key"]}',
                    value=opts['delim'].join(jd.system_architecture['options']))

            spec = psij.JobSpec()
            spec.attributes          = attr
            spec.executable          = jd.executable
            spec.arguments           = jd.arguments
            spec.environment         = jd.environment
            spec.directory           = jd.working_directory
            spec.stdout_path         = jd.output
            spec.stderr_path         = jd.error
            # inherit environment for local executor only
            spec.inherit_environment = bool(schema == SCHEMA_LOCAL)

            spec.resources = psij.ResourceSpecV1(
                node_count=jd.node_count or None,
                process_count=jd.total_cpu_count,
                exclusive_node_use=bool(jd.system_architecture.get('exclusive'))
            )
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

