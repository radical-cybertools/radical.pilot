
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import time
import threading       as mt
import subprocess      as sp

import radical.utils   as ru

from .base import LaunchMethod


# ------------------------------------------------------------------------------
#
class Flux(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def rm_config_hook(cls, name, cfg, rm, logger, profiler):

        profiler.prof('flux_start')

        flux = ru.which('flux')
        if not flux:
            raise Exception("Couldn't find flux")

        try:
            import sys
            print(sys.path)
            import flux
        except:
            raise Exception("Couldn't import flux")

      # cmd  = 'flux start -o,-v,-S,log-filename=out'.split()
      # proc = sp.Popen(cmd, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)
      # proc.stdin.write(ru.as_bytes('flux getattr local-uri\necho "OK"\n'))

        check = 'flux env; echo "OK"; while true; do echo "ok"; sleep 1; done'
        start = 'flux start -o,-v,-S,log-filename=out'
        cmd   = '/bin/bash -c "echo \\\"%s\\\" | %s"' % (check, start)
        proc  = sp.Popen(cmd, shell=True,
                          stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.STDOUT)

        flux_env = dict()
        while True:

            line = ru.as_string(proc.stdout.readline().strip())
            logger.debug('=== %s', line)

            if line.startswith('export '):
                k, v = line.split(' ', 1)[1].strip().split('=', 1)
                flux_env[k] = v.strip('"')
                logger.debug('%s = %s' % (k, v.strip('"')))

            elif line == 'OK':
                break


        assert('FLUX_URI' in flux_env)

        # TODO check perf implications
        flux_url             = ru.Url(flux_env['FLUX_URI'])
        flux_url.host        = ru.get_hostname()
        flux_url.scheme      = 'ssh'
        flux_env['FLUX_URI'] = str(flux_url)

        profiler.prof('flux_started')

        # ----------------------------------------------------------------------
        def _watch_flux(flux_env):

            logger.info('=== starting flux watcher')

            for k,v in flux_env.items():
                os.environ[k] = v

            ret = None
            while not ret:

                out, err, ret = ru.sh_callout('flux ping -c 1 all')
                logger.debug('=== flux watcher out: %s', out)

                if ret:
                    logger.error('=== flux watcher err: %s', err)
                    break

                time.sleep(0.1)

            logger.info('flux stopped?')
            # FIXME: trigger termination
        # ----------------------------------------------------------------------

        flux_watcher = mt.Thread(target=_watch_flux, args=[flux_env])
        flux_watcher.daemon = True
        flux_watcher.start()

        logger.info("flux startup successful: [%s]", flux_env['FLUX_URI'])

        lm_info = {'flux_env': flux_env,
                   'flux_pid': proc.pid}

        return lm_info


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        pass


  # # --------------------------------------------------------------------------
  # #
  # def construct_command(self, unit, launch_script_hop=None):
  #
  #     uid          = unit['uid']
  #     cud          = unit['description']
  #     procs        = cud['cpu_processes']
  #     cpn          = cud['cpu_threads']
  #     gpn          = cud['gpu_processes']
  #     task_exec    = cud['executable']
  #     task_args    = cud.get('arguments') or list()
  #     task_sandbox = unit['unit_sandbox_path']
  #
  #     self._log.debug('prep %s', uid)
  #
  #     return spec, None


# ------------------------------------------------------------------------------

