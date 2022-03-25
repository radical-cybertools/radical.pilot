
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import logging
logging.basicConfig(level='DEBUG')

from .base import PilotLauncherBase


# psij is an optional dependency right now.  We will report an import error
# during component construction - as that point the logger is set up
psij    = None
psij_ex = None
try:
    import psij
except Exception as e:
    psij_ex = e


# ------------------------------------------------------------------------------
#
class PilotLauncherPSIJ(PilotLauncherBase):

    # --------------------------------------------------------------------------
    #
    def __init__(self, log, prof, state_cb):

        if psij_ex:
            raise ImportError('psi-j-python not available') from psij_ex

        PilotLauncherBase.__init__(self, log, prof, state_cb)

        self._jobs     = dict()
        self._ex_cache = dict()


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
    def can_launch(self, rcfg, pilot):

        schema = self._get_schema(rcfg)

        if not schema:
            return False

        if schema not in self._ex_cache:

            self._log.debug('=== create executor for %s', schema)
            try:
                self._ex_cache[schema] = psij.JobExecutor.get_instance(schema)
            except:
                self._log.exception('failed to create psij executor')
                return False

        return True


    # --------------------------------------------------------------------------
    #
    def launch_pilots(self, rcfg, pilots):

        for pilot in pilots:

            pid    = pilot['uid']
            schema = self._get_schema(rcfg)
            assert(schema)

            ex = self._ex_cache.get(schema)

            assert(ex)

            jd   = pilot['jd_dict']
            spec = psij.JobSpec()

            spec.executable  = jd['executable']
            spec.arguments   = jd['arguments']
            spec.environment = jd['environment']
            spec.directory   = jd['working_directory']
            spec.stdout_path = jd['output']
            spec.stderr_path = jd['error']

            spec.resources   = psij.ResourceSpecV1()
            spec.resources.process_count    = 1
            spec.resources.cpu_cores_per_process = jd['total_cpu_count']
            spec.resources.gpu_cores_per_process = jd['total_gpu_count']

            job  = psij.Job(spec)
            ex.submit(job)

            self._jobs[pid] = job


    # --------------------------------------------------------------------------
    #
    def cancel_pilots(self, pids):

        for pid in pids:
            if pid not in pids:
                continue

            self._jobs[pid].cancel()


# ------------------------------------------------------------------------------

