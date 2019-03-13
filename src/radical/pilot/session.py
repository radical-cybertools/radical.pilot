
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import glob
import time
import threading      as mt

import radical.utils  as ru
import radical.saga   as rs
import radical.saga   as rs

import radical.saga.utils.pty_shell as rsup

from .utils           import ComponentManager
from .utils           import fetch_profiles, fetch_logfiles, fetch_json 
from .client          import Client


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

        # Only one session gets created without a configuration: the one
        # instantiated by the application.  This is the only one which will hold
        # a client module, which manages client side communication bridges and
        # components.
        #
        # FIXME: this is a sign of an inverted hierarchy: the *module* should
        #        have a *session*, not the way around.  We leave this for now, 
        #        to keep the API stabie.
        create_client = False

        self._resource_cfgs = ru.Config(module='radical.pilot', name='resource_*')
        self._session_cfgs  = ru.Config(module='radical.pilot', name='session_*')
        self._agent_cfgs    = ru.Config(module='radical.pilot', name='agent_*')
        self._umgr_cfgs     = ru.Config(module='radical.pilot', name='umgr_*')
        self._pmgr_cfgs     = ru.Config(module='radical.pilot', name='pmgr_*')

        if isinstance(_cfg, dict):
            # this is an agent session which already has a config
            self._cfg = _cfg

        else:
            # this is a client session - create client instance
            if not _cfg:
                _cfg = 'default'
            self._cfg = self._session_cfgs[_cfg]
            create_client = True


        # similarrly, we only create a component manager if we have any
        # components or units to start and manage
        create_cmgr = False
        if self._cfg.get('bridges') or self._cfg.get('components'):
            create_cmgr = True

        # cache for bridge addresses
        # FIXME: this should be a simple, distributed key/val store with locking
        #        and transactions.
        self._addresses = dict()

        # fall back to config data where possible
        # sanity check on parameters
        if not self._uid: 
            self._uid = self._cfg.get('uid')

        if not self._uid:
            # generate new uid, reset all other ID counters
            # FIXME: this will screw up counters for *concurrent* sessions, 
            #        as the ID generation is managed in a process singleton.
            self._uid = ru.generate_id('rp.session',  mode=ru.ID_PRIVATE)
            ru.reset_id_counters(prefix='rp.session', reset_all_others=True)

        # The session's sandbox is either configured (agent side),
        # or falls back to `./$SID` (client).
        if 'sandbox' in self._cfg:
            # agent sets the sandbox
            self._sandbox = self._cfg['sandbox']

        else:
            self._sandbox = './%s/' % self.uid
            self._cfg['sandbox'] = self._sandbox

        if 'uid'   not in self._cfg: self._cfg['uid']   = self._uid 
        if 'owner' not in self._cfg: self._cfg['owner'] = self._uid 

        # FIXME: this is wrong for the pilot
        ru_def = ru.DefaultConfig()
        ru_def['ns']          = 'radical'
        ru_def['log_dir']     = self._sandbox
        ru_def['profile_dir'] = self._sandbox

        self._prof = ru.Profiler(name=self._cfg['owner'])
        self._rep  = ru.Reporter(name=self._cfg['owner'])
        self._log  = ru.Logger  (name=self._cfg['owner'])

        # now we have config and uid - initialize base class (saga session)
        rs.Session.__init__(self, uid=self._uid)

        # prepare to handle resource configs
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
            self._cmgr = ComponentManager(self, self._cfg, self.uid)

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


    # -------------------------------------------------------------------------
    #
    def fetch_profiles(self, tgt=None, fetch_client=False):

        return fetch_profiles(self._uid, tgt=tgt, session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_logfiles(self, tgt=None, fetch_client=False):

        return fetch_logfiles(self._uid, tgt=tgt, session=self)


    # -------------------------------------------------------------------------
    #
    def fetch_json(self, tgt=None, fetch_client=False):

        return fetch_json(self._uid, tgt=tgt, session=self)


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

        ret = list()

        for site in self._resource_cfgs:
            for host in self._resource_cfgs[site]:
                ret.append('%s.%s' % (site, host))

        return ret


    # -------------------------------------------------------------------------
    #
    def get_resource_config(self, descr):
        '''
        Returns a dictionary of the resource config matching the resource
        defined in the given pilot description, specialized by the access and
        system parameters of the description.

        The returned dict will have three sections: resource, access and system.
        '''

        self.is_valid()

        ret = dict()

        resource = descr['resource']
        access   = descr.get('access')
        system   = descr.get('system')

        aliases = self._resource_cfgs.get('aliases', dict())
        if  resource in aliases:
            self._log.warning("using alias '%s' for deprecated resource '%s'"
                              % (aliases[resource], resource))
            resource = aliases[resource]

        if '.' not in resource:
            raise RuntimeError("malformed resource '%s' (site.host)" % resource)

        site, host = resource.split('.', 1)

        if site not in self._resource_cfgs:
            raise RuntimeError("Site '%s' not configured." % site)

        if host not in self._resource_cfgs[site]:
            raise RuntimeError("Host '%s' not configured." % host)

        cfg = self._resource_cfgs[site][host]

        if not access: access = cfg['access'].get('default', 'default')
        if not system: system = cfg['system'].get('default', 'default')

        if access not in cfg['access']:
            raise RuntimeError("Access '%s' not configured" % access)

        if system not in cfg['system']:
            raise RuntimeError("system '%s' not configured" % system)

        ret['resource'] = copy.deepcopy(cfg['resource'])
        ret['access'  ] = copy.deepcopy(cfg['access'][access])
        ret['system'  ] = copy.deepcopy(cfg['system'][system])

        ret['resource']['label'] = resource

        return ret


    # -------------------------------------------------------------------------
    #
    def get_agent_config(self, descr, rcfg):
        '''
        Returns a dictionary of the agent config matching the
        given pilot description and resource config.

        The returned dict will have three sections: layout, config and tuning.
        '''

        self.is_valid()

        acfgs  = self._agent_cfgs

        # configs defined by pilot description?
        layout = descr.get('layout')
        config = descr.get('config')
        tuning = descr.get('tuning')

        # cfgs preferred by resource config?
        if not layout: rcfg['resource'].get('agent_layout')
        if not config: rcfg['resource'].get('agent_config')
        if not tuning: rcfg['resource'].get('agent_tuning')

        # fallback to default cfgs
        if not layout: layout = acfgs['layout']['default']
        if not config: config = acfgs['config']['default']
        if not tuning: tuning = acfgs['tuning']['default']

        # resolve named cfgs (string) to actual dicts, if needed
        if isinstance(layout, basestring): layout = acfgs['layout'][layout]
        if isinstance(config, basestring): config = acfgs['config'][config]
        if isinstance(tuning, basestring): tuning = acfgs['tuning'][tuning]

        ret = copy.deepcopy({'layout': layout,
                             'config': config,
                             'tuning': tuning})

        return ret


    # -------------------------------------------------------------------------
    #
    def get_client_sandbox(self):
        '''
        For the session in the client application, this is os.getcwd().  For the
        session in any other component, specifically in pilot components, the
        client sandbox needs to be read from the session config (or pilot
        config).  The latter is not yet implemented, so the pilot can not yet
        interpret client sandboxes.  Since pilot-side stagting to and from the
        client sandbox is not yet supported anyway, this seems acceptable
        (FIXME).
        '''

        return self._client_sandbox


    # -------------------------------------------------------------------------
    #
    def get_resource_sandbox(self, pilot):
        """
        for a given pilot dict, determine the global RP sandbox, based on the
        pilot's 'resource' attribute.
        """

        self.is_valid()

        # FIXME: this should get 'resource, schema=None' as parameters

        resource = pilot['description']['resource']

        if not resource:
            raise ValueError('Cannot get pilot sandbox w/o resource target')

        # the global sandbox will be the same for all pilots on any resource, so
        # we cache it
        with self._cache_lock:

            if resource not in self._cache['resource_sandbox']:

                # cache miss -- determine sandbox and fill cache
                rcfg   = self.get_resource_config(pilot['description'])
                fs_url = rs.Url(rcfg['access']['filesystem'])

                # Get the sandbox from either the pilot_desc or resource conf
                sandbox_raw = pilot['description'].get('sandbox')
                if not sandbox_raw:
                    sandbox_raw = rcfg.get('default_sandbox_base', "$PWD")
                    sandbox_raw = rcfg.get('default_remote_workdir', "$PWD")

                # If the sandbox contains expandables, we need to resolve those remotely.
                # NOTE: Note that this will only work for (gsi)ssh or shell based access mechanisms
                if '$' not in sandbox_raw and '`' not in sandbox_raw:
                    # no need to expand further
                    sandbox_base = sandbox_raw

                else:
                    js_url  = rs.Url(rcfg['access']['job_manager'])
                    schemas = js_url.schema.split('+')

                    if   'ssh'    in schemas: js_url.schema = 'ssh'
                    elif 'gsissh' in schemas: js_url.schema = 'gsissh'
                    elif 'fork'   in schemas: js_url.schema = 'fork'
                    elif len(schemas) == 1  : js_url.schema = 'fork'  # local
                    else: raise Exception("unsupported schema: %s" % js_url)

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

        rcfg   = self.get_resource_config(pilot['description'])

        js_url = rs.Url(rcfg.get('job_manager'))
        js_hop = rs.Url(rcfg.get('job_manager_hop', js_url))

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
    def get_address_fname(self, name):
        '''
        return name of the address file for the named queue or pubsub channel
        otherwise
        '''

        fnames = ['%s/%s.url' % (self._sandbox, name), 
                  '%s/%s.url' % (os.getcwd(),   name)]

        for fname in fnames:

            if os.path.isfile(fname):
                return fname

        return None


    # --------------------------------------------------------------------------
    #
    def get_address(self, name):
        '''
        return in and out address for the named bridge, if known - None
        otherwise
        '''
        if name in self._addresses:
            return self._addresses[name]

        addr  = dict()
        fname = self.get_address_fname(name)

        if not fname:
            raise ValueError('no addresses found for %s' % name)

        with open(fname, 'r') as fin:
            for line in fin.readlines():
                key, val = line.split()
                if key in ['PUB', 'PUT']: addr[key] = val
                if key in ['SUB', 'GET']: addr[key] = val

        assert(len(addr) == 2), 'malformed url file %s' % fname

        return addr


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

