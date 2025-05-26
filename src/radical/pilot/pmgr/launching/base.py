
__copyright__ = 'Copyright 2022, The RADICAL-Cybertools Team'
__license__   = 'MIT'


import os
import copy
import math
import time
import pprint
import shutil
import tempfile

from collections import defaultdict

import threading          as mt

import radical.utils      as ru

from ... import states    as rps
from ... import constants as rpc
from ... import utils     as rpu

from ...staging_directives import complete_url, expand_staging_directives


# ------------------------------------------------------------------------------
# 'enum' for RP's PMGRraunching types
RP_UL_NAME_SAGA  = "SAGA"
RP_UL_NAME_PSI_J = "PSI_J"


# ------------------------------------------------------------------------------
#
class PilotLauncherBase(object):

    def __init__(self, name, log, prof, state_cb):
        '''
        log:      ru.Logger instance to use
        prof:     ru.Profiler instance to use
        state_cb: rp.pmgr.PNGRLaunchingComponent state callback to invoke on
                  pilot state updates
        '''

        self._name     = name
        self._log      = log
        self._prof     = prof
        self._state_cb = state_cb


    # --------------------------------------------------------------------------
    #
    @property
    def name(self):
        return self._name


    # --------------------------------------------------------------------------
    #
    def can_launch(self, rcfg, pilot):
        '''
        check if the give pilot can be launched on the specified target resource
        '''
        raise NotImplementedError('can_launch not implemented')


    # --------------------------------------------------------------------------
    #
    def launch_pilots(self, rcfg, pilots):
        '''
        rcfg:   resource config for resource to launch pilots topic
        pilots: pilot dictionaries for pilots to launch
                expected to contain `job_dict` as basis for job description
        '''
        raise NotImplementedError('launch_pilots not implemented')


    # --------------------------------------------------------------------------
    #
    def kill_pilots(self, pids):
        '''
        pids:   RP UIDs for pilots to cancel
        '''
        raise NotImplementedError('kill_pilots not implemented')


    # --------------------------------------------------------------------------
    #
    def stop(self):
        '''
        terminate this instance, clean up, destroy any threads etc
        '''

        pass


# ------------------------------------------------------------------------------
#
class PMGRLaunchingComponent(rpu.ClientComponent):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        self._uid = ru.generate_id(cfg['owner'] + '.launching.%(counter)s',
                                   ru.ID_CUSTOM)

        super().__init__(cfg, session)
        self._pmgr      = self._owner

        self._pilots    = dict()      # dict for all known pilots
        self._lock      = mt.RLock()  # lock on maipulating the above
        self._sandboxes = dict()      # cache of resource sandbox URLs
        self._cancelled = list()      # list of cancelled pilots

        self._mod_dir   = os.path.dirname(os.path.abspath(__file__))
        self._root_dir  = "%s/../../" % self._mod_dir

        # register input queue
        self.register_input(rps.PMGR_LAUNCHING_PENDING,
                            rpc.PMGR_LAUNCHING_QUEUE, self.work)

        # we don't really have an output queue, as we pass control over the
        # pilot jobs to the resource management system (ResourceManager).
        self._stager = rpu.StagingHelper(self._log)

        self._log.info(ru.get_version([self._mod_dir, self._root_dir]))
        self._rp_version, _, _, _, _ = \
                ru.get_version([self._mod_dir, self._root_dir])


        # load all launcher implementations
        self._launchers = dict()

        from .saga  import PilotLauncherSAGA
        from .psi_j import PilotLauncherPSIJ

        impl = {
            RP_UL_NAME_SAGA : PilotLauncherSAGA,
            RP_UL_NAME_PSI_J: PilotLauncherPSIJ
        }

        exceptions = dict()
        for name in [RP_UL_NAME_PSI_J, RP_UL_NAME_SAGA]:
            try:
                ctor = impl[name]
                self._launchers[name] = ctor(name, self._log, self._prof,
                                             self._state_cb)
            except Exception as e:
                self._log.warn('skip launcher %s' % name)
                exceptions[name] = e

        # if no launcher is usable, log the found exceptions
        if not self._launchers:
            for name in [RP_UL_NAME_PSI_J, RP_UL_NAME_SAGA]:
                e = exceptions.get(name)
                if e:
                    try   : raise e
                    except: self._log.exception('launcher %s unusable' % name)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def create(cls, cfg, session):

        return cls(cfg, session)


    # --------------------------------------------------------------------------
    #
    def _state_cb(self, pilot, rp_state):

        self._log.info('pilot state update: %s: %s', pilot['uid'], rp_state)

        if rp_state != pilot['state']:
            self.advance(pilot, state=rp_state, push=False, publish=True)


    # --------------------------------------------------------------------------
    #
    def finalize(self):

        # FIXME: is this called?

        try:
            self.unregister_input(rps.PMGR_LAUNCHING_PENDING,
                                  rpc.PMGR_LAUNCHING_QUEUE, self.work)

            # FIXME: always kill all pilot jobs for non-final pilots at
            #        termination, and set the pilot states to CANCELED.
            with self._lock:
                pids = list(self._pilots.keys())

            self._kill_pilots(pids)

            # TODO: close launchers

        except:
            self._log.exception('finalization error')


    # --------------------------------------------------------------------------
    #
    def control_cb(self, topic, msg):

        cmd = msg.get('cmd')
        arg = msg.get('arg')

        self._log.debug_9('launcher got %s', msg)

        if cmd == 'kill_pilots':

            pmgr = arg['pmgr']
            pids = arg['uids']

            if pmgr != self._pmgr:
                return True

            if not isinstance(pids, list):
                pids = [pids]

            self._log.info('received "kill_pilots" command (%s)', pids)

            self._kill_pilots(pids)


    # --------------------------------------------------------------------------
    #
    def _kill_pilots(self, pids):
        '''
        Send a cancellation request to the pilots.  This call will not wait for
        the request to get enacted, nor for it to arrive, but just send it.
        '''

        if not pids:
            if not self._pilots:
                return
            else:
                pids = list(self._pilots.keys())

        with self._lock:

            for pid in pids:

                self._log.debug('cancel pilot %s', pid)
                if pid not in self._pilots:
                    self._log.warn('cannot cancel unknown pilot %s', pid)
                    self._cancelled.append(pid)
                    continue

                pilot = self._pilots[pid]
                lname = pilot['launcher']

                if lname not in self._launchers:
                    self._log.warn('invalid pilot launcher name: %s', lname)
                    continue

                launcher = self._launchers[lname]
                try:
                    launcher.kill_pilots([pid])
                except:
                    self._log.exception('pilot cancel failed for %s' % pid)


    # --------------------------------------------------------------------------
    #
    def work(self, pilots):

        pilots = ru.as_list(pilots)

        # weed out pilots for which we have already received a cancel request
        to_cancel = [p for p in pilots if p['uid']     in self._cancelled]
        to_start  = [p for p in pilots if p['uid'] not in self._cancelled]

        self.advance(to_cancel, rps.CANCELED,       publish=True, push=False)
        self.advance(to_start,  rps.PMGR_LAUNCHING, publish=True, push=False)

        # We can only use bulk submission for pilots which go to the same
        # target, thus we sort them into buckets and launch the buckets
        # individually
        buckets = defaultdict(lambda: defaultdict(list))
        for pilot in to_start:
            resource = pilot['description']['resource']
            schema   = pilot['description']['access_schema']
            buckets[resource][schema].append(pilot)

        for resource in buckets:

            for schema in buckets[resource]:

                try:
                    pilots = buckets[resource][schema]
                    pids   = [p['uid'] for p in pilots]
                    self._log.info("Launching pilots on %s: %s", resource, pids)

                    self._start_pilot_bulk(resource, schema, pilots)

                    self.advance(pilots, rps.PMGR_ACTIVE_PENDING,
                                         push=False, publish=True)

                except Exception:
                    self._log.exception('bulk launch failed')
                    self.advance(pilots, rps.FAILED, push=False, publish=True)


    # --------------------------------------------------------------------------
    #
    def _start_pilot_bulk(self, resource, schema, pilots):
        '''
        For each pilot, we prepare by determining what files need to be staged,
        and what job description needs to be submitted.  Files are then be
        staged, and jobs are launched.

        Two files are staged: a bootstrapper and a tarball - the latter
        containing the pilot sandboxes, agent configs, and any other auxilliary
        files needed to bootstrap.  The bootstrapper will untar those parts of
        the tarball which it needs to bootstrap one specific pilot.
        '''

        rcfg = self._session.get_resource_config(resource, schema)
        sid  = self._session.uid

        # ----------------------------------------------------------------------
        # the rcfg can contain keys with string expansion placeholders where
        # values from the pilot description need filling in.  A prominent
        # example is `%(pd.project)s`, where the pilot description's `PROJECT`
        # value needs to be filled in (here in lowercase).
        #
        # FIXME: right now we assume all pilot descriptions to contain similar
        #        entries, so that the expansion is only done on the first PD.
        expand = dict()
        pd     = pilots[0]['description']
        for k,v in pd.items():
            if v is None:
                v = ''
            expand['pd.%s' % k] = v
            if isinstance(v, str):
                expand['pd.%s' % k.upper()] = v.upper()
                expand['pd.%s' % k.lower()] = v.lower()
            else:
                expand['pd.%s' % k.upper()] = v
                expand['pd.%s' % k.lower()] = v

        for k in rcfg:
            if isinstance(rcfg[k], str):
                orig     = rcfg[k]
                rcfg[k]  = rcfg[k] % expand
                expanded = rcfg[k]
                if orig != expanded:
                    self._log.debug('RCFG:\n%s\n%s', orig, expanded)

        # we create a fake session_sandbox with all pilot_sandboxes in /tmp, and
        # then tar it up.  Once we untar that tarball on the target machine, we
        # should have all sandboxes and all files required to bootstrap the
        # pilots
        tmp_dir  = os.path.abspath(tempfile.mkdtemp(prefix='rp_agent_tmp'))
        tar_name = '%s.%s.tgz' % (sid, self._uid)
        tar_tgt  = '%s/%s'     % (tmp_dir, tar_name)
        tar_url  = ru.Url('file://localhost/%s' % tar_tgt)

        # we need the session sandbox url, but that is (at least in principle)
        # dependent on the schema to use for pilot startup.  So we confirm here
        # that the bulk is consistent wrt. to the schema.  Also include
        # `staging_input` files and place them in the `pilot_sandbox`.
        #
        # FIXME: if it is not, it needs to be splitted into schema-specific
        # sub-bulks
        #
        schema = pd.get('access_schema')
        for pilot in pilots[1:]:
            assert schema == pilot['description'].get('access_schema'), \
                    'inconsistent scheme on launch / staging'

        # get and expand sandboxes (this bulk uses the same schema toward the
        # same target resource, so all session sandboxes are the same)
        # FIXME: expansion actually may differ per pilot (queue names, project
        #        names, etc could be expanded)
        session_sandbox = self._session._get_session_sandbox(pilots[0]).path
        session_sandbox = session_sandbox % expand

        # we will create the session sandbox before we untar, so we can use that
        # as workdir, and pack all paths relative to that session sandbox.  That
        # implies that we have to recheck that all URLs in fact do point into
        # the session sandbox.
        #
        # We also create a file `staging_output.json` for each pilot which
        # contains the list of files to be tarred up and prepared for output
        # staging.

        ft_list = list()  # files to stage
        sd_list = list()  # staging directives

        for pilot in pilots:

            pid = pilot['uid']
            os.makedirs('%s/%s' % (tmp_dir, pid))

            self._prepare_pilot(resource, rcfg, pilot, expand, tar_name)

            ft_list += pilot['fts']
            sd_list += pilot['sds']


        # ----------------------------------------------------------------------
        # handle pilot data staging
        for pilot in pilots:

            pid = pilot['uid']
            self._prof.prof('staging_in_start', uid=pid)

            for sd in ru.as_list(pilot['description'].get('input_staging')):

                if isinstance(sd, dict):
                    sd['rem'] = False
                    ft_list.append(sd)

                else:
                    fname = sd
                    base = os.path.basename(fname)
                    # checking if input staging file exists
                    if fname.startswith('./'):
                        fname = fname.split('./', maxsplit=1)[1]
                    if not fname.startswith('/'):
                        fname = os.path.join(self._cfg.base, fname)
                    if not os.path.exists(fname):
                        raise RuntimeError('input_staging file does not exists: '
                                           '%s for pilot %s' % (fname, pid))

                    ft_list.append({'source': fname,
                                    'target': '%s/%s' % (pid, base),
                                    'rem': False})

            output_staging = pilot['description'].get('output_staging')
            if output_staging:
                fname = '%s/%s/staging_output.txt' % (tmp_dir, pilot['uid'])
                with ru.ru_open(fname, 'w') as fout:
                    for entry in output_staging:
                        fout.write('%s\n' % entry)

        # direct staging, use first pilot for staging context
        # NOTE: this implies that the SDS can only refer to session
        #       sandboxes, not to pilot sandboxes!
        self._stage_in(pilots[0], sd_list)


        for ft in ft_list:
            source     = os.path.abspath(ft['source'])
            target     = os.path.relpath(os.path.normpath(ft['target']), session_sandbox)
          # source_dir = os.path.dirname(source)
            target_dir = os.path.dirname(target)

            if target_dir.startswith('..'):
                target = ft['target']
                target_dir = os.path.dirname(target)

            if not os.path.isdir('%s/%s' % (tmp_dir, target_dir)):
                os.makedirs('%s/%s' % (tmp_dir, target_dir))

            if source == '/dev/null':
                # we want an empty file -- touch it (tar will refuse to
                # handle a symlink to /dev/null)
                ru.ru_open('%s/%s' % (tmp_dir, target), 'a').close()
            else:
                # use a shell callout to account for wildcard expansion
                cmd = 'ln -s %s %s/%s' % (os.path.abspath(source), tmp_dir,
                                          target)
                out, err, ret = ru.sh_callout(cmd, shell=True)
                if ret:
                    self._log.debug('cmd: %s', cmd)
                    self._log.debug('out: %s', out)
                    self._log.debug('err: %s', err)
                    raise RuntimeError('callout failed: %s' % cmd)


        # tar.  If any command fails, this will raise.
        cmd = "cd %s && tar zchf %s *" % (tmp_dir, tar_tgt)
        out, err, ret = ru.sh_callout(cmd, shell=True)

        if ret:
            self._log.debug('cmd: %s', cmd)
            self._log.debug('out: %s', out)
            self._log.debug('err: %s', err)
            raise RuntimeError('callout failed: %s' % cmd)

        # remove all files marked for removal-after-pack
        for ft in ft_list:
            if ft['rem']:
                os.unlink(ft['source'])

        fs_endpoint  = rcfg['filesystem_endpoint']
        fs_url       = ru.Url(fs_endpoint)
        tar_rem      = ru.Url(fs_url)
        tar_rem.path = "%s/%s" % (session_sandbox, tar_name)

        self._log.debug('stage tarball for %s', pilots[0]['uid'])
        self._stage_in(pilots[0], {'source': tar_url,
                                   'target': tar_rem,
                                   'action': rpc.TRANSFER})
        shutil.rmtree(tmp_dir)

        # FIXME: the untar was moved into the bootstrapper (see `-z`).  That
        #        is actually only correct for the single-pilot case...

        now = time.time()
        for pilot in pilots:
            self._prof.prof('staging_in_stop',  uid=pilot['uid'], ts=now)
            self._prof.prof('submission_start', uid=pilot['uid'], ts=now)

        # find launchers to handle pilot job submission.  We sort by launcher so
        # that each launcher can handle a bulk of pilots at once.
        buckets = defaultdict(list)
        for pilot in pilots:
            for lname,launcher in self._launchers.items():
                if launcher.can_launch(rcfg, pilots):
                    pilot['launcher'] = lname
                    buckets[lname].append(pilot)
                    self._log.info('use launcher %s for pilot %s',
                                   lname, pilot['uid'])
                    break

            if not pilot.get('launcher'):
                raise RuntimeError('no launcher found for %s' % pilot['uid'])

        with self._lock:
            for lname, bucket in buckets.items():
                launcher = self._launchers[lname]
                launcher.launch_pilots(rcfg, bucket)
                for pilot in bucket:
                    pid = pilot['uid']
                    self._pilots[pid] = pilot
                    self._prof.prof('submission_stop', uid=pid)


    # --------------------------------------------------------------------------
    #
    def _prepare_pilot(self, resource, rcfg, pilot, expand, tar_name):

        rcfg.verify()

        pid = pilot["uid"]
        pilot['fts'] = list()  # tar for staging
        pilot['sds'] = list()  # direct staging
        pilot['jd_dict' ] = None    # job description

        # ----------------------------------------------------------------------
        # Database connection parameters
        sid       = self._session.uid
        proxy_url = self._session.cfg.proxy_url

        # ----------------------------------------------------------------------
        # pilot description and resource configuration
        backup_nodes     = pilot['description']['backup_nodes']
        requested_nodes  = pilot['description']['nodes']
        requested_cores  = pilot['description']['cores']
        requested_gpus   = pilot['description']['gpus']
        requested_memory = pilot['description']['memory']
        runtime          = pilot['description']['runtime']
        app_comm         = pilot['description']['app_comm']
        queue            = pilot['description']['queue']
        job_name         = pilot['description']['job_name']
        project          = pilot['description']['project']
        cleanup          = pilot['description']['cleanup']
        services         = pilot['description']['services']
        enable_ep        = pilot['description']['enable_ep']
        prepare_env      = pilot['description']['prepare_env']
        reconfig_src     = pilot['description']['reconfig_src']

        # ----------------------------------------------------------------------
        # get parameters from resource cfg
        agent_spawner           = rcfg.agent_spawner
        agent_config            = rcfg.agent_config
        agent_scheduler         = rcfg.agent_scheduler
        default_queue           = rcfg.default_queue
        forward_tunnel_endpoint = rcfg.forward_tunnel_endpoint
        resource_manager        = rcfg.resource_manager
        pre_bootstrap_0         = rcfg.pre_bootstrap_0
        pre_bootstrap_1         = rcfg.pre_bootstrap_1
        python_interpreter      = rcfg.python_interpreter
        rp_version              = rcfg.rp_version
        virtenv_mode            = rcfg.virtenv_mode
        virtenv                 = rcfg.virtenv
        cores_per_node          = rcfg.cores_per_node
        gpus_per_node           = rcfg.gpus_per_node
        lfs_path_per_node       = rcfg.lfs_path_per_node
        lfs_size_per_node       = rcfg.lfs_size_per_node
        python_dist             = rcfg.python_dist
        task_tmp                = rcfg.task_tmp
        task_pre_launch         = rcfg.task_pre_launch
        task_post_launch        = rcfg.task_post_launch
        task_pre_exec           = rcfg.task_pre_exec
        task_post_exec          = rcfg.task_post_exec
        mandatory_args          = rcfg.mandatory_args
        network_interface       = rcfg.network_interface
        system_architecture     = rcfg.system_architecture
        raptor_cfg              = rcfg.raptor

        # part of the core specialization settings
        blocked_cores           = system_architecture.get('blocked_cores', [])
        blocked_gpus            = system_architecture.get('blocked_gpus',  [])

        # some default values are determined at runtime
        if not virtenv:
            virtenv = '%%(resource_sandbox)s/ve.%s.%s' \
                    % (resource, self._rp_version)

        self._log.debug(pprint.pformat(rcfg))

        # make sure that mandatory args are known
        for ma in mandatory_args:
            if pilot['description'].get(ma) is None:
                raise  ValueError('attribute "%s" is required for "%s"'
                                 % (ma, resource))

        # get pilot and global sandbox
        endpoint_fs      = self._session._get_endpoint_fs     (pilot)
        resource_sandbox = self._session._get_resource_sandbox(pilot)
        session_sandbox  = self._session._get_session_sandbox (pilot)
        pilot_sandbox    = self._session._get_pilot_sandbox   (pilot)
        client_sandbox   = self._session._get_client_sandbox  ()

        pilot['endpoint_fs']      = str(endpoint_fs)      % expand
        pilot['resource_sandbox'] = str(resource_sandbox) % expand
        pilot['session_sandbox']  = str(session_sandbox)  % expand
        pilot['pilot_sandbox']    = str(pilot_sandbox)    % expand
        pilot['client_sandbox']   = str(client_sandbox)

        # from here on we need only paths
        endpoint_fs      = endpoint_fs     .path % expand
        resource_sandbox = resource_sandbox.path % expand
        session_sandbox  = session_sandbox .path % expand
        pilot_sandbox    = pilot_sandbox   .path % expand
      # client_sandbox   = client_sandbox  # not expanded

        # expand variables in virtenv string
        virtenv = virtenv % {'pilot_sandbox'   : pilot_sandbox,
                             'session_sandbox' : session_sandbox,
                             'resource_sandbox': resource_sandbox}

        if not job_name:
            job_name = pid

        try:
            if isinstance(agent_config, dict):
                agent_cfg = ru.Config(cfg=agent_config)

            elif isinstance(agent_config, str):
                agent_cfg = ru.Config('radical.pilot',
                                      category='agent',
                                      name=agent_config)
            else:
                # we can't handle this type
                raise TypeError('agent config must be string or dict')

        except Exception:
            self._log.exception('Error using agent config')
            raise


        # Check for deprecated global_virtenv
        if 'global_virtenv' in rcfg:
            raise RuntimeError("'global_virtenv' is deprecated (%s)" % resource)

        # Create a host:port string for use by the bootstrap_0.
        tmp = ru.Url(proxy_url)
        if tmp.port:
            hostport = "%s:%d" % (tmp.host, tmp.port)
        else:
            hostport = tmp.host

        # ----------------------------------------------------------------------
        # the version of the agent is derived from
        # rp_version, which has the following format
        # and interpretation:
        #
        # case rp_version:
        #   @<token>:
        #   @tag/@branch/@commit:
        #       git clone $github_base radical.pilot.src
        #       (cd radical.pilot.src && git checkout token)
        #       pip install -t $VIRTENV/rp_install/ radical.pilot.src
        #       rm -rf radical.pilot.src
        #       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
        #
        #   release:
        #       pip install -t $VIRTENV/rp_install radical.pilot
        #       export PYTHONPATH=$VIRTENV/rp_install:$PYTHONPATH
        #
        #   installed:
        #       true
        # esac
        #
        # virtenv_mode
        #   private : error  if ve exists, otherwise create, then use
        #   update  : update if ve exists, otherwise create, then use
        #   create  : use    if ve exists, otherwise create, then use
        #   use     : use    if ve exists, otherwise error,  then exit
        #   recreate: delete if ve exists, otherwise create, then use
        #   local   : use the client virtualenv (assumes same FS)
        #
        # examples   :
        #   virtenv@v0.20
        #   virtenv@devel
        #   virtenv@release
        #   virtenv@installed
        #   stage@local
        #   stage@/tmp/my_agent.py
        #
        # Note that some combinations may be invalid,
        # specifically in the context of virtenv_mode.  If, for
        # example, virtenv_mode is 'use', then the 'virtenv:tag'
        # will not make sense, as the virtenv is not updated.
        # In those cases, the virtenv_mode is honored, and
        # a warning is printed.
        #
        # Also, the 'stage' mode can only be combined with the
        # 'local' source, or with a path to the agent (relative
        # to root_dir, or absolute).
        #
        # A rp_version which does not adhere to the
        # above syntax is ignored, and the fallback stage@local
        # is used.

        # use local VE ?
        if virtenv_mode == 'local':
            rp_version = 'installed'
            if os.environ.get('VIRTUAL_ENV'):
                python_dist = 'default'
                virtenv     = os.environ['VIRTUAL_ENV']
            elif os.environ.get('CONDA_PREFIX'):
                python_dist = 'anaconda'
                virtenv     = os.environ['CONDA_PREFIX']
            else:
                # we are not in a virtualenv, just use Python as-is.
                python_dist  = 'default'
                virtenv_mode = 'none'
                virtenv      = ''

        if not rp_version.startswith('@') and \
               rp_version not in ['installed', 'local', 'release']:
            raise ValueError("invalid rp_version '%s'" % rp_version)

        if rp_version.startswith('@'):
            rp_version  = rp_version[1:]  # strip '@'

        # ----------------------------------------------------------------------
        # sanity checks
        RE = RuntimeError
        if not python_dist     : raise RE("missing python distribution")
        if not agent_spawner   : raise RE("missing agent spawner")
        if not agent_scheduler : raise RE("missing agent scheduler")
        if not resource_manager: raise RE("missing resource manager")

        # massage some values
        if not queue:
            queue = default_queue

        if  cleanup and isinstance(cleanup, bool):
            #  l : log files
            #  u : task work dirs
            #  v : virtualenv
            #  e : everything (== pilot sandbox)
            cleanup = 'luve'

            # we never cleanup virtenvs which are not private
            if virtenv_mode != 'private':
                cleanup = cleanup.replace('v', '')

        # estimate requested resources
        smt = int(os.environ.get('RADICAL_SMT') or
                  system_architecture.get('smt', 1))

        if cores_per_node and smt:
            cores_per_node *= smt

        avail_cores_per_node = cores_per_node
        avail_gpus_per_node  = gpus_per_node

        if avail_cores_per_node and blocked_cores:
            avail_cores_per_node -= len(blocked_cores)
            assert (avail_cores_per_node > 0)

        if avail_gpus_per_node and blocked_gpus:
            avail_gpus_per_node -= len(blocked_gpus)
            assert (avail_gpus_per_node >= 0)

        if requested_nodes:
            if not avail_cores_per_node:
                raise RuntimeError('use "cores" in PilotDescription')

        else:
            if avail_cores_per_node:
                requested_nodes = requested_cores / avail_cores_per_node

            if avail_gpus_per_node:
                requested_nodes = max(requested_gpus / avail_gpus_per_node,
                                      requested_nodes)

            requested_nodes = math.ceil(requested_nodes)

        # now that we know the number of nodes to request, derive
        # the *actual* number of cores and gpus we allocate
        allocated_cores = (
            (requested_nodes + backup_nodes) * avail_cores_per_node) \
                    or requested_cores
        allocated_gpus  = (
            (requested_nodes + backup_nodes) * avail_gpus_per_node)  \
                    or requested_gpus

        if rcfg.numa_domain_map:
            numa_domains_per_node = len(rcfg.numa_domain_map)
        else:
            numa_domains_per_node = 1

        self._log.debug('nodes: %s[+%d] [%s %s | %s], cores: %s, gpus: %s',
                        requested_nodes, backup_nodes,
                        cores_per_node, gpus_per_node, numa_domains_per_node,
                        allocated_cores, allocated_gpus)

        # set mandatory args
        bs_args = ['-l', '%s/bootstrap_0.sh' % pilot_sandbox]

        bs_args.extend(['-p', pid])
        bs_args.extend(['-s', sid])
        bs_args.extend(['-m', virtenv_mode])
        bs_args.extend(['-r', rp_version])
        bs_args.extend(['-b', python_dist])
        bs_args.extend(['-v', virtenv])
        bs_args.extend(['-y', str(runtime)])
        bs_args.extend(['-z', tar_name])

        # set optional args
        if resource_manager == "CCM": bs_args.extend(['-c'])
        if forward_tunnel_endpoint:   bs_args.extend(['-f', forward_tunnel_endpoint])
        if forward_tunnel_endpoint:   bs_args.extend(['-h', hostport])
        if python_interpreter:        bs_args.extend(['-i', python_interpreter])
      # if tunnel_bind_device:        bs_args.extend(['-t', tunnel_bind_device])
        if cleanup:                   bs_args.extend(['-x', cleanup])

        for arg in pre_bootstrap_0:   bs_args.extend(['-e', arg])
        for arg in pre_bootstrap_1:   bs_args.extend(['-w', arg])

        agent_cfg['uid']                 = 'agent_0'
        agent_cfg['sid']                 = sid
        agent_cfg['pid']                 = pid
        agent_cfg['owner']               = pid
        agent_cfg['pmgr']                = self._pmgr
        agent_cfg['resource']            = resource
        agent_cfg['backup_nodes']        = backup_nodes
        agent_cfg['nodes']               = requested_nodes
        agent_cfg['cores']               = allocated_cores
        agent_cfg['gpus']                = allocated_gpus
        agent_cfg['spawner']             = agent_spawner
        agent_cfg['scheduler']           = agent_scheduler
        agent_cfg['runtime']             = runtime
        agent_cfg['app_comm']            = app_comm
        agent_cfg['proxy_url']           = proxy_url
        agent_cfg['pilot_sandbox']       = pilot_sandbox
        agent_cfg['session_sandbox']     = session_sandbox
        agent_cfg['resource_sandbox']    = resource_sandbox
        agent_cfg['resource_manager']    = resource_manager
        agent_cfg['cores_per_node']      = cores_per_node
        agent_cfg['gpus_per_node']       = gpus_per_node
        agent_cfg['lfs_path_per_node']   = lfs_path_per_node
        agent_cfg['lfs_size_per_node']   = lfs_size_per_node
        agent_cfg['task_tmp']            = task_tmp
        agent_cfg['task_pre_launch']     = task_pre_launch
        agent_cfg['task_pre_exec']       = task_pre_exec
        agent_cfg['task_post_launch']    = task_post_launch
        agent_cfg['task_post_exec']      = task_post_exec
        agent_cfg['resource_cfg']        = copy.deepcopy(rcfg)
        agent_cfg['log_lvl']             = self._log.level
        agent_cfg['debug_lvl']           = self._log.debug_level
        agent_cfg['services']            = services
        agent_cfg['enable_ep']           = enable_ep
        agent_cfg['prepare_env']         = prepare_env
        agent_cfg['reconfig_src']        = reconfig_src
        agent_cfg['raptor']              = raptor_cfg
        agent_cfg['network_interface']   = network_interface

        pilot['cfg']       = agent_cfg
        pilot['resources'] = {'cpu': allocated_cores,
                              'gpu': allocated_gpus}


        # ----------------------------------------------------------------------
        # Write agent config dict to a json file in pilot sandbox.

        agent_cfg_name = 'agent_0.cfg'
        cfg_tmp_handle, cfg_tmp_file = tempfile.mkstemp(prefix='rp.agent_cfg.')
        os.close(cfg_tmp_handle)  # file exists now

        # Convert dict to json file
        self._log.debug("Write agent cfg to '%s'.", cfg_tmp_file)
        agent_cfg.write(cfg_tmp_file)

        # always stage agent cfg for each pilot, not in the tarball
        # FIXME: purge the tmp file after staging
        self._log.debug('cfg %s -> %s', agent_cfg['pid'], pilot_sandbox)
        pilot['sds'].append({'source': cfg_tmp_file,
                             'target': '%s/%s' % (pilot['pilot_sandbox'],
                                                  agent_cfg_name),
                             'action': rpc.TRANSFER})

        # always stage the bootstrapper for each pilot, not in the tarball
        # FIXME: this results in many staging ops for many pilots
        bootstrapper_path = os.path.abspath("%s/agent/bootstrap_0.sh"
                                           % self._root_dir)
        pilot['sds'].append({'source': bootstrapper_path,
                             'target': '%s/bootstrap_0.sh'
                                     % pilot['pilot_sandbox'],
                             'action': rpc.TRANSFER})

        # always stage RU env helper
        env_helper = ru.which('radical-utils-env.sh')
        assert env_helper
        self._log.debug('env %s -> %s', env_helper, pilot_sandbox)
        pilot['sds'].append({'source': env_helper,
                             'target': '%s/%s' % (pilot['pilot_sandbox'],
                                                  os.path.basename(env_helper)),
                             'action': rpc.TRANSFER})

        # check if we have a sandbox cached for that resource.  If so, we have
        # nothing to do.  Otherwise we create the sandbox and stage the RP
        # stack etc.
        #
        # NOTE: this will race when multiple pilot launcher instances are used!
        #
        if resource not in self._sandboxes:

            self._sandboxes[resource] = True

        # ----------------------------------------------------------------------
        # Create Job description

        jd_dict = rpu.FastTypedDict()

        jd_dict.name                  = job_name
        jd_dict.executable            = '/bin/bash'
        jd_dict.arguments             = bs_args
        jd_dict.working_directory     = pilot_sandbox
        jd_dict.project               = project
        jd_dict.output                = 'bootstrap_0.out'
        jd_dict.error                 = 'bootstrap_0.err'
        jd_dict.node_count            = requested_nodes + backup_nodes
        jd_dict.total_cpu_count       = allocated_cores
        jd_dict.total_gpu_count       = allocated_gpus
        jd_dict.total_physical_memory = requested_memory
        jd_dict.processes_per_host    = avail_cores_per_node
        jd_dict.wall_time_limit       = runtime
        jd_dict.queue                 = queue
        jd_dict.file_transfer         = list()
        jd_dict.environment           = dict()
        jd_dict.system_architecture   = dict(system_architecture)

        # forward all `RADICAL_*` env variables by default
        for k,v in os.environ.items():
            if k.startswith('RADICAL_'):
                jd_dict.environment[k] = v

        jd_dict.environment['RP_PILOT_SANDBOX'] = pilot_sandbox
        jd_dict.environment['RADICAL_BASE']     = resource_sandbox
        jd_dict.environment['RADICAL_SMT']      = str(smt)

        if self._prof.enabled:
            jd_dict.environment['RADICAL_PROFILE'] = 'TRUE'


        # for condor backends and the like which do not have shared FSs, we add
        # additional staging directives so that the backend system binds the
        # files from the session and pilot sandboxes to the pilot job.

        self._log.debug("Bootstrap command line: %s %s", jd_dict.executable,
                jd_dict.arguments)

        pilot['jd_dict'] = jd_dict


    # --------------------------------------------------------------------------
    #
    def _stage_in(self, pilot, sds):
        '''
        Run some input staging directives.
        '''

        # contexts for staging url expansion
        tgt_ctx = {'pwd'     : pilot['pilot_sandbox'],      # !
                   'client'  : pilot['client_sandbox'],
                   'pilot'   : pilot['pilot_sandbox'],
                   'resource': pilot['resource_sandbox']}

        src_ctx = {'pwd'     : pilot['client_sandbox'],     # !
                   'client'  : pilot['client_sandbox'],
                   'pilot'   : pilot['pilot_sandbox'],
                   'resource': pilot['resource_sandbox']}

        sds = expand_staging_directives(sds, src_ctx, tgt_ctx)

        for sd in sds:
            self._prof.prof('staging_in_start', uid=pilot['uid'], msg=sd['uid'])
            self._stager.handle_staging_directive(sd)
            self._prof.prof('staging_in_stop', uid=pilot['uid'], msg=sd['uid'])


    # --------------------------------------------------------------------------
    #
    def _stage_out(self, pilot, sds):
        '''
        Run some output staging directives.
        '''

        # contexts for staging url expansion
        loc_ctx = {'pwd'     : pilot['client_sandbox'],
                   'client'  : pilot['client_sandbox'],
                   'pilot'   : pilot['pilot_sandbox'],
                   'resource': pilot['resource_sandbox']}

        rem_ctx = {'pwd'     : pilot['pilot_sandbox'],
                   'client'  : pilot['client_sandbox'],
                   'pilot'   : pilot['pilot_sandbox'],
                   'resource': pilot['resource_sandbox']}

        sds = ru.as_list(sds)

        for sd in sds:
            sd['source'] = str(complete_url(sd['source'], rem_ctx, self._log))
            sd['target'] = str(complete_url(sd['target'], loc_ctx, self._log))

            self._prof.prof('staging_out_start', uid=pilot['uid'], msg=sd['uid'])
            self._stager.handle_staging_directive(sd)
            self._prof.prof('staging_out_stop', uid=pilot['uid'], msg=sd['uid'])


# ------------------------------------------------------------------------------

