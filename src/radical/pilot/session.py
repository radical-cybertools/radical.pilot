
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import glob
import time
import threading as mt

import radical.utils        as ru
import saga                 as rs
import saga.utils.pty_shell as rsup

from .                import utils   as rpu
from .client          import Client
from .resource_config import ResourceConfig


# ------------------------------------------------------------------------------
#
class Session(rs.Session):
    '''
    An `rp.Session` is the RP API handle to, well, an radical pilot session,
    which manages configuration and lifetime of pilot and unit managers, and
    thus also of pilots and units.  Closing a session will clean up all objects
    managed by that session.
    '''

    # the reporter is an process-level singleton
    _reporter = None

    # We keep a static typemap for component startup. If we ever want to
    # become reeeealy fancy, we can derive that typemap from rp module
    # inspection.
    #
    # --------------------------------------------------------------------------
    #
    def __init__(self, uid=None, _cfg=None):
        '''
        Creates a new session.  A new Session instance is created and 
        stored in the database.

        **Arguments:**

            * **uid**   (`string`): Create a session with this UID.  
        **Returns:**
            * A new Session instance.
        '''

        self._uid         = uid
        self._cfg         = _cfg

        self._client      = None
        self._cmgr        = None

        self._t_start     = time.time()
        self._valid       = True
        self._closed      = False
        self._valid_iter  = 0  # detect recursive calls of `is_valid()`

        if not self._cfg:
            self._cfg = dict()

        # Only one session gets created without a configuration: the one
        # instantiated by the application.  This is the only one which will hold
        # a client module, which manages client side communication bridges and
        # components.
        #
        # FIXME: this is a sign of an inverted hierarchy: it looks like a client
        #        should have a session, not the way around.  We leave this for
        #        now for API stability...
        create_client = False

        if not self._cfg:
            # read the default (or other) session config, and create a client
            self._cfg = ru.read_json("%s/configs/session_%s.json"
                      % (os.path.dirname(__file__),
                         os.environ.get('RADICAL_PILOT_SESSION_CFG','default')))
            create_client = True


        # similarrly, we only create a component manager if we have any
        # components or units to start and manage
        create_cmgr = False
        if self._cfg.get('bridges') or self._cfg.get('components'):
            create_cmgr = True


        # fall back to config data where possible
        # sanity check on parameters
        if not self._uid: 
            self._uid = self._cfg.get('session_id')

        if not self._uid:
            # generate new uid, reset all other ID counters
            # FIXME: this will screw up counters for *concurrent* sessions, 
            #        as the ID generation is managed in a process singleton.
            self._uid = ru.generate_id('rp.session',  mode=ru.ID_PRIVATE)
            ru.reset_id_counters(prefix='rp.session', reset_all_others=True)

        # The session's sandbox is either configured (agent side),
        # or falls back to `./$SID` (client).
        if 'session_sandbox' in self._cfg:
            self._sandbox = self._cfg['session_sandbox']

        else:
            self._sandbox = './%s/' % self.uid
            self._cfg['session_sandbox'] = self._sandbox

        if not self._cfg.get('session_id'): self._cfg['session_id'] = self._uid 
        if not self._cfg.get('owner')     : self._cfg['owner']      = self._uid 
        if not self._cfg.get('logdir')    : self._cfg['logdir']     = self._sandbox

        self._logdir = self._cfg['logdir']
        self._prof   = self.get_profiler(name=self._cfg['owner'])
        self._rep    = self.get_reporter(name=self._cfg['owner'])
        self._log    = self.get_logger  (name=self._cfg['owner'],
                                         level=self._cfg.get('debug'))

        # now we have config and uid - initialize base class (saga session)
        rs.Session.__init__(self, uid=self._uid)

        # prepare to handle resource configs
        self._rcfgs       = None
        self._cache       = dict()  # cache sandboxes etc.
        self._cache_lock  = mt.RLock()

        self._cache['resource_sandbox'] = dict()
        self._cache['session_sandbox']  = dict()
        self._cache['pilot_sandbox']    = dict()
        self._client_sandbox = os.getcwd()  ## different in agent, use cfg?

        if create_client:

            # create a client instance
            self._client = Client(self)

            # inform client application
            self._prof.prof('session_start', uid=self._uid)
            self._rep.info ('<<new session: ')
            self._rep.plain('[%s]' % self._uid)
            self._rep.info ('<<database   : ')

            self._rec = os.environ.get('RADICAL_PILOT_RECORD_SESSION')
            if self._rec:

                # create recording path and record session
                self._rec = "%s/%s" % (self._rec, self._uid)
                os.system('mkdir -p %s' % self._rec)
                ru.write_json(self._cfg, "%s/session.json" % self._rec)
                self._log.info("recording session in %s" % self._rec)


        if create_cmgr:
            self._cmgr = rpu.ComponentManager(self, self._cfg)

        # done!
        if create_client:
            self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __enter__(self):
        return self


    # --------------------------------------------------------------------------
    # Allow Session to function as a context manager in a `with` clause
    def __exit__(self, type, value, traceback):

        # FIXME: use cleanup_on_close, terminate_on_close attributes
        self.close()


    # --------------------------------------------------------------------------
    #
    def is_valid(self, term=True):

        # don't check validity during termination
        if self._closed:
            return True

        # if we check any manager or agent, it will likely also check the
        # session in turn.  We break that loop here.
        self._valid_iter += 1

        try:
            if self._valid_iter >= 2:
                # we are too deep - abort this line or tests
                return True

            if self._valid:
                if self._client:
                    if not self._client.is_valid():
                        self._valid = False

        finally:
            pass

        if not self._valid and term:
            self._log.warn("session %s is invalid" % self.uid)
            self.close()
          # raise RuntimeError("session %s is invalid" % self.uid)

        return self._valid


    # --------------------------------------------------------------------------
    #
    def close(self, cleanup=False, terminate=True, download=False):
        """Closes the session.

        All subsequent attempts access objects attached to the session will 
        result in an error. If cleanup is set to True (default) the session
        data is removed from the database.

        **Arguments:**
            * **cleanup** (`bool`):   Remove session from MongoDB (implies
                                      terminate)
            * **terminate** (`bool`): Shut down all pilots associated with the
                                      session. 
        """

        # close only once
        if self._closed:
            return

        self._rep.info('closing session %s' % self._uid)
        self._log.debug("session %s closing", self._uid)
        self._prof.prof("session_close", uid=self._uid)

        if  cleanup:
            # cleanup implies terminate
            terminate = True

        if self._cmgr  : self._cmgr.close()
        if self._client: self._client.close(terminate=terminate,
                                            cleanup=cleanup)

        self._log.debug("session %s closed (delete=%s)", self._uid, cleanup)
        self._prof.prof("session_stop", uid=self._uid)
        self._prof.close()

        self._closed = True
        self._valid  = False

        # after all is said and done, we attempt to download the pilot log- and
        # profiles, if so wanted
        if download:

            self._prof.prof("session_fetch_start", uid=self._uid)
            self._log.debug('start download')
            tgt = os.getcwd()

            try:
                self.fetch_json(tgt='%s/%s' % (tgt, self.uid))
                self.fetch_profiles(tgt=tgt)
                self.fetch_logfiles(tgt=tgt)
            except:
                self._log.error('download failed')

            self._prof.prof("session_fetch_stop", uid=self._uid)

        now = time.time()
        self._rep.info('<<session lifetime: %.1fs' % (now - self._t_start))
        self._rep.ok('>>ok\n')


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    @property
    def logdir(self):
        return self._logdir


    # --------------------------------------------------------------------------
    #
    @property
    def created(self):
        '''
        Returns the UTC date and time the session was created.
        '''
        # FIXME
        return None


    # --------------------------------------------------------------------------
    #
    @property
    def closed(self):
        '''
        Returns the time of closing
        '''
        # FIXME
        return None


    # --------------------------------------------------------------------------
    #
    def get_logger(self, name, level=None):
        """
        This is a thin wrapper around `ru.Logger()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """
        return ru.Logger(name=name, ns='radical.pilot', targets=['.'], 
                         path=self._logdir, level=level)


    # --------------------------------------------------------------------------
    #
    def get_reporter(self, name):
        """
        This is a thin wrapper around `ru.Reporter()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        if not self._reporter:
            self._reporter = ru.Reporter(name=name, ns='radical.pilot',
                                         targets=['stdout'], path=self._logdir)
        return self._reporter


    # --------------------------------------------------------------------------
    #
    def get_profiler(self, name):
        """
        This is a thin wrapper around `ru.Profiler()` which makes sure that
        log files end up in a separate directory with the name of `session.uid`.
        """

        prof = ru.Profiler(name=name, ns='radical.pilot', path=self._logdir)

        return prof


    # -------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_profiles(self._uid, tgt=tgt, session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_logfiles(self, tgt=None, fetch_client=False):

        return rpu.fetch_logfiles(self._uid, tgt=tgt, session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_json(self, tgt=None, fetch_client=False):

        return rpu.fetch_json(self._uid, tgt=tgt, session=self)


    # --------------------------------------------------------------------------
    #
    def insert_metadata(self, metadata):

        # FIXME
        self.is_valid()


    # --------------------------------------------------------------------------
    #
    def _register_pmgr(self, pmgr):

        self.is_valid()
        assert(self._client)
        return self._client.register_pmgr(pmgr)


    # --------------------------------------------------------------------------
    #
    def list_pilot_managers(self):

        self.is_valid()
        assert(self._client)
        return self._client.list_pmgrs()


    # --------------------------------------------------------------------------
    #
    def get_pilot_managers(self, pmgr_uids=None):

        self.is_valid()
        assert(self._client)
        return self._client.get_pmgrs(pmgr_uids)


    # --------------------------------------------------------------------------
    #
    def _register_umgr(self, umgr):

        self.is_valid()
        assert(self._client)
        return self._client.register_umgr(umgr)


    # --------------------------------------------------------------------------
    #
    def list_unit_managers(self):

        self.is_valid()
        assert(self._client)
        return self._client.list_umgrs()


    # --------------------------------------------------------------------------
    #
    def get_unit_managers(self, umgr_uids=None):

        self.is_valid()
        assert(self._client)
        return self._client.get_umgrs(umgr_uids)


    # -------------------------------------------------------------------------
    #
    def list_resources(self):
        '''
        Returns a list of known resource labels which can be used in a pilot
        description.  Not that resource aliases won't be listed.
        '''

        self.load_resource_configs()

        return sorted(self._rcfgs.keys())


    # --------------------------------------------------------------------------
    #
    def load_resource_configs(self):

        self.is_valid()

        if self._rcfgs is not None:
            return

        self._prof.prof('config_parser_start', uid=self._uid)

        # Loading all "default" resource configurations
        self._rcfgs  = dict()
        module_path  = os.path.dirname(os.path.abspath(__file__))
        default_cfgs = "%s/configs/resource_*.json" % module_path
        config_files = glob.glob(default_cfgs)

        for config_file in config_files:

            try:
                self._log.info("Load resource configs from %s" % config_file)
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e:
                self._log.exception("skip config file %s: %s" % (config_file, e))
                raise RuntimeError('config error (%s) - abort' % e)

            for rc in rcs:
                self._log.info("Load resource configurations for %s" % rc)
                self._rcfgs[rc] = rcs[rc].as_dict() 
                self._log.debug('read rcfg for %s (%s)', 
                        rc, self._rcfgs[rc].get('cores_per_node'))

        home         = os.environ.get('HOME', '')
        user_cfgs    = "%s/.radical/pilot/configs/resource_*.json" % home
        config_files = glob.glob(user_cfgs)

        for config_file in config_files:

            try:
                rcs = ResourceConfig.from_file(config_file)
            except Exception as e:
                self._log.exception("skip config file %s: %s" % (config_file, e))
                raise RuntimeError('config error (%s) - abort' % e)

            for rc in rcs:
                self._log.info("Load resource configurations for %s" % rc)

                if rc in self._rcfgs:
                    # config exists -- merge user config into it
                    ru.dict_merge(self._rcfgs[rc],
                                  rcs[rc].as_dict(),
                                  policy='overwrite')
                else:
                    # new config -- add as is
                    self._rcfgs[rc] = rcs[rc].as_dict() 

                self._log.debug('fix  rcfg for %s (%s)', 
                        rc, self._rcfgs[rc].get('cores_per_node'))

        default_aliases = "%s/configs/resource_aliases.json" % module_path
        self._resource_aliases = ru.read_json_str(default_aliases)['aliases']

        # check if we have aliases to merge
        usr_aliases = '%s/.radical/pilot/configs/resource_aliases.json' % home
        if os.path.isfile(usr_aliases):
            ru.dict_merge(self._resource_aliases,
                          ru.read_json_str(usr_aliases).get('aliases', {}),
                          policy='overwrite')

        self._prof.prof('config_parser_stop', uid=self._uid)


    # -------------------------------------------------------------------------
    #
    def add_resource_config(self, resource_config):
        """Adds a new :class:`radical.pilot.ResourceConfig` to the PilotManager's 
           dictionary of known resources, or accept a string which points to
           a configuration file.

           For example::

                  rc = radical.pilot.ResourceConfig(label="mycluster")
                  rc.job_manager_endpoint = "ssh+pbs://mycluster
                  rc.filesystem_endpoint  = "sftp://mycluster
                  rc.default_queue        = "private"
                  rc.bootstrapper         = "default_bootstrapper.sh"

                  pm = radical.pilot.PilotManager(session=s)
                  pm.add_resource_config(rc)

                  pd = radical.pilot.ComputePilotDescription()
                  pd.resource = "mycluster"
                  pd.cores    = 16
                  pd.runtime  = 5 # minutes

                  pilot = pm.submit_pilots(pd)
        """

        self.is_valid()

        if isinstance(resource_config, basestring):

            # let exceptions fall through
            rcs = ResourceConfig.from_file(resource_config)

            for rc in rcs:
                self._log.info("Loaded resource configurations for %s" % rc)
                self._rcfgs[rc] = rcs[rc].as_dict() 
                self._log.debug('add  rcfg for %s (%s)', 
                        rc, self._rcfgs[rc].get('cores_per_node'))

        else:
            self._rcfgs[resource_config.label] = resource_config.as_dict()
            self._log.debug('Add  rcfg for %s (%s)', 
                    resource_config.label, 
                    self._rcfgs[resource_config.label].get('cores_per_node'))

    # -------------------------------------------------------------------------
    #
    def get_resource_config(self, resource, schema=None):
        """
        Returns a dictionary of the requested resource config
        """

        self.is_valid()
        self.load_resource_configs()

        if  resource in self._resource_aliases:
            self._log.warning("using alias '%s' for deprecated resource key '%s'" \
                              % (self._resource_aliases[resource], resource))
            resource = self._resource_aliases[resource]

        if  resource not in self._rcfgs:
            raise RuntimeError("Resource '%s' is not known." % resource)

        resource_cfg = copy.deepcopy(self._rcfgs[resource])
        self._log.debug('get rcfg 1 for %s (%s)',  
                        resource, resource_cfg.get('cores_per_node'))

        if  not schema:
            if 'schemas' in resource_cfg:
                schema = resource_cfg['schemas'][0]

        if  schema:
            if  schema not in resource_cfg:
                raise RuntimeError("schema %s unknown for resource %s" \
                                  % (schema, resource))

            for key in resource_cfg[schema]:
                # merge schema specific resource keys into the
                # resource config
                resource_cfg[key] = resource_cfg[schema][key]

        self._log.debug('get rcfg 2 for %s (%s)',
                        resource, resource_cfg.get('cores_per_node'))

        return resource_cfg



    # -------------------------------------------------------------------------
    #
    def get_client_sandbox(self):
        """
        For the session in the client application, this is os.getcwd().  For the
        session in any other component, specifically in pilot components, the
        client sandbox needs to be read from the session config (or pilot
        config).  The latter is not yet implemented, so the pilot can not yet
        interpret client sandboxes.  Since pilot-side stagting to and from the
        client sandbox is not yet supported anyway, this seems acceptable
        (FIXME).
        """

        return self._client_sandbox


    # -------------------------------------------------------------------------
    #
    def get_resource_sandbox(self, pilot):
        """
        for a given pilot dict, determine the global RP sandbox, based on the
        pilot's 'resource' attribute.
        """

        self.is_valid()
        self.load_resource_configs()

        # FIXME: this should get 'resource, schema=None' as parameters

        resource = pilot['description'].get('resource')
        schema   = pilot['description'].get('access_schema')

        if not resource:
            raise ValueError('Cannot get pilot sandbox w/o resource target')

        # the global sandbox will be the same for all pilots on any resource, so
        # we cache it
        with self._cache_lock:

            if resource not in self._cache['resource_sandbox']:

                # cache miss -- determine sandbox and fill cache
                rcfg   = self.get_resource_config(resource, schema)
                fs_url = rs.Url(rcfg['filesystem_endpoint'])

                # Get the sandbox from either the pilot_desc or resource conf
                sandbox_raw = pilot['description'].get('sandbox')
                if not sandbox_raw:
                    sandbox_raw = rcfg.get('default_remote_workdir', "$PWD")

                # If the sandbox contains expandables, we need to resolve those remotely.
                # NOTE: Note that this will only work for (gsi)ssh or shell based access mechanisms
                if '$' not in sandbox_raw and '`' not in sandbox_raw:
                    # no need to expand further
                    sandbox_base = sandbox_raw

                else:
                    js_url = rs.Url(rcfg['job_manager_endpoint'])

                    if 'ssh' in js_url.schema.split('+'):
                        js_url.schema = 'ssh'
                    elif 'gsissh' in js_url.schema.split('+'):
                        js_url.schema = 'gsissh'
                    elif 'fork' in js_url.schema.split('+'):
                        js_url.schema = 'fork'
                    elif '+' not in js_url.schema:
                        # For local access to queueing systems use fork
                        js_url.schema = 'fork'
                    else:
                        raise Exception("unsupported schema: %s" % js_url.schema)

                    self._log.debug("rsup.PTYShell('%s')", js_url)
                    shell = rsup.PTYShell(js_url, self)

                    ret, out, err = shell.run_sync(' echo "WORKDIR: %s"'
                                                   % sandbox_raw)
                    if ret == 0 and 'WORKDIR:' in out:
                        sandbox_base = out.split(":")[1].strip()
                        self._log.debug("sandbox base %s: '%s'",
                                        js_url, sandbox_base)
                    else:
                        raise RuntimeError("Couldn't get remote workdir.")

                # we have determined the remote 'pwd' - the global sandbox
                # is relative to it.
                fs_url.path = "%s/radical.pilot.sandbox" % sandbox_base

                # before returning, keep the URL string in cache
                self._cache['resource_sandbox'][resource] = fs_url

            return self._cache['resource_sandbox'][resource]


    # --------------------------------------------------------------------------
    #
    def get_session_sandbox(self, pilot=None):

        self.is_valid()

        if pilot is None:
            return self._sandbox


        self.load_resource_configs()

        # FIXME: this should get 'resource, schema=None' as parameters
        resource = pilot['description'].get('resource')

        if not resource:
            raise ValueError('Cannot get session sandbox w/o resource target')

        with self._cache_lock:

            if resource not in self._cache['session_sandbox']:

                # cache miss
                resource_sandbox      = self.get_resource_sandbox(pilot)
                session_sandbox       = rs.Url(resource_sandbox)
                session_sandbox.path += '/%s' % self.uid

                self._cache['session_sandbox'][resource] = session_sandbox

            return self._cache['session_sandbox'][resource]


    # --------------------------------------------------------------------------
    #
    def get_pilot_sandbox(self, pilot):

        self.is_valid()
        self.load_resource_configs()

        # FIXME: this should get 'pid, resource, schema=None' as parameters

        self.is_valid()

        pilot_sandbox = pilot.get('pilot_sandbox')
        if str(pilot_sandbox):
            return rs.Url(pilot_sandbox)

        pid = pilot['uid']
        with self._cache_lock:
            if  pid in self._cache['pilot_sandbox']:
                return self._cache['pilot_sandbox'][pid]

        # cache miss
        session_sandbox     = self.get_session_sandbox(pilot)
        pilot_sandbox       = rs.Url(session_sandbox)
        pilot_sandbox.path += '/%s/' % pilot['uid']

        with self._cache_lock:
            self._cache['pilot_sandbox'][pid] = pilot_sandbox

        return pilot_sandbox


    # --------------------------------------------------------------------------
    #
    def get_unit_sandbox(self, unit, pilot):

        self.is_valid()
        self.load_resource_configs()

        # we don't cache unit sandboxes, they are just a string concat.
        pilot_sandbox = self.get_pilot_sandbox(pilot)
        return "%s/%s/" % (pilot_sandbox, unit['uid'])


    # --------------------------------------------------------------------------
    #
    def get_jsurl(self, pilot):
        '''
        get job service endpoint and hop URL for the pilot's target resource.
        '''

        self.is_valid()
        self.load_resource_configs()

        resrc   = pilot['description']['resource']
        schema  = pilot['description']['access_schema']
        rcfg    = self.get_resource_config(resrc, schema)

        js_url  = rs.Url(rcfg.get('job_manager_endpoint'))
        js_hop  = rs.Url(rcfg.get('job_manager_hop', js_url))

        # make sure the js_hop url points to an interactive access
        # TODO: this is an unreliable heuristics - we should require the js_hop
        #       URL to be specified in the resource configs.
        if   '+gsissh' in js_hop.schema or \
             'gsissh+' in js_hop.schema    : js_hop.schema = 'gsissh'
        elif '+ssh'    in js_hop.schema or \
             'ssh+'    in js_hop.schema    : js_hop.schema = 'ssh'
        else                               : js_hop.schema = 'fork'

        return js_url, js_hop


    # --------------------------------------------------------------------------
    #
    @staticmethod
    def autopilot(user, passwd):

        import github3
        import random

        labels = 'type:autopilot'
        titles = ['+++ Out of Cheese Error +++',
                  '+++ Redo From Start! +++',
                  '+++ Mr. Jelly! Mr. Jelly! +++',
                  '+++ Melon melon melon',
                  '+++ Wahhhhhhh! Mine! +++',
                  '+++ Divide By Cucumber Error +++',
                  '+++ Please Reinstall Universe And Reboot +++',
                  '+++ Whoops! Here comes the cheese! +++',
                  '+++ End of Cheese Error +++',
                  '+++ Can Not Find Drive Z: +++',
                  '+++ Unknown Application Error +++',
                  '+++ Please Reboot Universe +++',
                  '+++ Year Of The Sloth +++',
                  '+++ error of type 5307 has occured +++',
                  '+++ Eternal domain error +++',
                  '+++ Error at Address Number 6, Treacle Mine Road +++']

        def excuse():
            cmd_fetch  = "telnet bofh.jeffballard.us 666 2>&1 "
            cmd_filter = "grep 'Your excuse is:' | cut -f 2- -d :"
            out        = ru.sh_callout("%s | %s" % (cmd_fetch, cmd_filter),
                                       shell=True)[0]
            return out.strip()


        github = github3.login(user, passwd)
        repo   = github.repository("radical-cybertools", "radical.pilot")

        title = 'autopilot: %s' % titles[random.randint(0, len(titles) - 1)]

        print '----------------------------------------------------'
        print 'autopilot'

        for issue in repo.issues(labels=labels, state='open'):
            if issue.title == title:
                reply = 'excuse: %s' % excuse()
                issue.create_comment(reply)
                print '  resolve: %s' % reply
                return

        # issue not found - create
        body  = 'problem: %s' % excuse()
        issue = repo.create_issue(title=title, body=body, labels=[labels],
                                  assignee=user)
        print '  issue  : %s' % title
        print '  problem: %s' % body
        print '----------------------------------------------------'


# -----------------------------------------------------------------------------

