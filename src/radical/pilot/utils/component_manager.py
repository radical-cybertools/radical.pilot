
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time

import radical.utils as ru

USE_SHELL = False


# ------------------------------------------------------------------------------
#
class ComponentManager(object):
    '''
    A 'rp.ComponentManager` manages, as the name suggests, RP componnents - but
    it also creates and manages communication bridges which connect those
    components.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, session, cfg, owner):

        self._session    = session
        self._cfg        = cfg
        self._owner      = owner

        self._uid  = ru.generate_id('%s.cmgr' % self._owner, ru.ID_CUSTOM)

        self._prof = ru.Profiler(name=self._uid)
        self._log  = ru.Logger  (name=self._uid)

        self._bridges    = dict()  # map uids to pids
        self._components = dict()  # map uids to pids

        with open('cmgr.cfg', 'w') as fout:
            import pprint
            fout.write('\n%s\n\n' % pprint.pformat(self._cfg))

        self._start_bridges()
        self._start_components()
      # self._start_agents()


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    def _start_bridges(self):

        if 'bridges' not in self._cfg:
            self._cfg['bridges'] = dict()

        # merge configs into bridge specs
        ccfg = self._cfg.get('config', dict()).get('bridges')
        if ccfg:
            ru.dict_merge(self._cfg['bridges'], ccfg, ru.OVERWRITE)


        ru_def = ru.DefaultConfig()

        # all file system interactions should happen in the session sandbox
        sbox = self._session.get_session_sandbox()

        # first start all bridge processes, then get url, pid etc. via the
        # filesystem for all of them, to overlap bridge bootstrapping.
        to_check = list()
        for bname, bcfg in self._cfg['bridges'].iteritems():

            buid = bname  # only one bridge for any bridge name
            fout = '%s/%s.out' % (sbox, buid)
            ferr = '%s/%s.err' % (sbox, buid)
            fcfg = '%s/%s.cfg' % (sbox, buid)
            fpid = '%s/%s.pid' % (sbox, buid)
            furl = '%s/%s.url' % (sbox, buid)

            bcfg['ru_def']     = copy.deepcopy(ru_def.as_dict())
            bcfg['session_id'] = self._session.uid
            bcfg['sandbox']    = sbox

            bcfg['owner']      = self.uid
            bcfg['name']       = bname
            bcfg['uid']        = buid
            bcfg['fpid']       = fpid
            bcfg['furl']       = furl
            bcfg['fout']       = fout
            bcfg['ferr']       = ferr
            bcfg['ppid']       = os.getpid()

            if   'pubsub' in bname: bcfg['kind'] = 'pubsub'
            elif 'queue'  in bname: bcfg['kind'] = 'queue'
            else: raise ValueError('invalid bridge name %s' % bname)

            ru.write_json(bcfg, fcfg)

            os.environ['RU_WATCH_PID1'] = str(os.getpid())
            stdout = '%s/%s.out' % (sbox, bname)
            stderr = '%s/%s.err' % (sbox, bname)
            pw     = 'radical-utils-pwatch'
            cmd    = '%s radical-pilot-bridge %s'  % (pw, fcfg)
            ru.sh_callout_bg(cmd, stdout=stdout, stderr=stderr, shell=USE_SHELL)

            self._log.debug('started bridge %s: %s' % (buid, cmd))

            to_check.append(bcfg)


        # check if all bridges came up, get pid and addresses via files
        for bcfg in to_check:

            buid  = bcfg['uid']
            fpid  = bcfg['fpid']
            furl  = bcfg['furl']
            kind  = bcfg['kind']

            start = time.time()

            fin      = None
            pid      = None
            addr_in  = None
            addr_out = None

            while True:

                if not pid:

                    try   : fin = open(fpid, 'r')
                    except: pass

                    if fin:
                        pid = int(fin.read().split()[1])
                        fin.close()
                        fin = None


                if not addr_in or not addr_out:

                    try   : fin = open(furl, 'r')
                    except: pass

                    if fin:
                        for line in fin.readlines():
                            key, val = line.split()
                            if key in ['PUB', 'PUT']: addr_in  = val
                            if key in ['SUB', 'GET']: addr_out = val

                        fin.close()
                        fin = None

                if pid and addr_in and addr_out:
                    break

                time.sleep(0.1)
                if time.time() - start > 5.0:  # FIXME: configurable
                    break

            assert(pid and addr_in and addr_out), \
                   'bridge %s not alive (%s)' % (bcfg['uid'], fpid)

            # watch the pid
            ru.pid_watcher(pid=pid, uid=self.uid)

            if kind == 'pubsub': 
                bcfg['PUB'] = addr_in
                bcfg['SUB'] = addr_out

            elif kind == 'queue':
                bcfg['PUT'] = addr_in
                bcfg['GET'] = addr_out

            self._bridges[buid] = int(pid)

            self._log.debug('bridge %s [%s] started with %s/%s', buid, pid,
                            addr_in, addr_out)


    # --------------------------------------------------------------------------
    #
    def _start_components(self):
        '''
        `start_components()` is very similar to `start_bridges()`, in that it
        interprets a given configuration and creates all listed component
        instances.  Components are, however,  *always* created, independent of
        any existing instances.

        This method will return a list of created component instances.  It is up
        to the callee to watch those components for health and to terminate them
        as needed.  
        '''

        if 'components' not in self._cfg:
            # nothing to do
            return

        # merge specific configs into component specs
        ccfg = self._cfg.get('config', dict()).get('components')
        if ccfg:
            ru.dict_merge(self._cfg['components'], ccfg, ru.OVERWRITE)

        ru_def   = ru.DefaultConfig()
        sbox     = self._session.get_session_sandbox()
        to_check = list()

        for cname in self._cfg['components']:

            cfg = self._cfg['components'][cname]
            cfg['rcfg'] = self._cfg.get('rcfg', dict())
            cfg['acfg'] = self._cfg.get('acfg', dict())

            for num in range(cfg.get('count', 1)):

                ccfg = copy.deepcopy(cfg)

                # merge global config settings
                dcfg = self._cfg.get('default')
                if dcfg:
                    ru.dict_merge(ccfg, dcfg, ru.OVERWRITE)

                cuid = ru.generate_id(cname + '.%(counter)04d', ru.ID_CUSTOM)
                cuid = self._owner + '.' + cuid

                fout = '%s/%s.out' % (sbox, cuid)
                ferr = '%s/%s.err' % (sbox, cuid)
                fcfg = '%s/%s.cfg' % (sbox, cuid)
                fpid = '%s/%s.pid' % (sbox, cuid)

                ccfg['ru_def']     = copy.deepcopy(ru_def.as_dict())
                ccfg['session_id'] = self._session.uid
                ccfg['sandbox']    = sbox

                ccfg['owner']      = self.uid
                ccfg['name']       = cname
                ccfg['kind']       = cname
                ccfg['uid']        = cuid
                ccfg['fpid']       = fpid
                ccfg['fout']       = fout
                ccfg['ferr']       = ferr
                ccfg['ppid']       = os.getpid()

                ru.write_json(ccfg, fcfg)

                os.environ['RU_WATCH_PID1'] = str(os.getpid())
                stdout = '%s/%s.pw.out' % (sbox, cname)
                stderr = '%s/%s.pw.err' % (sbox, cname)
                pw     = 'radical-utils-pwatch'
                cmd    = '%s radical-pilot-component %s'  % (pw, fcfg)
                ru.sh_callout_bg(cmd, shell=USE_SHELL,
                                 stdout=stdout, stderr=stderr)

                self._log.debug('started component %s: %s' % (cuid, cmd))
                to_check.append(ccfg)

        # wait until all components are up
        for ccfg in to_check:

            cuid  = ccfg['uid']
            fpid  = ccfg['fpid']

            start = time.time()

            fin      = None
            pid      = None

            while True:

                if not pid:

                    try   : fin = open(fpid, 'r')
                    except: pass

                    if fin:
                        pid = int(fin.read().split()[1])
                        fin.close()
                        fin = None

                if pid:
                    break

                time.sleep(0.1)
                if time.time() - start > 5.0:  # FIXME: configurable
                    break

            assert(pid), 'component %s not alive (%s)' % (ccfg['uid'], fpid)

            # watch the pid
            ru.pid_watcher(pid=pid, uid=self.uid)

            self._components[cuid] = int(pid)

            self._log.debug('component %s [%s] started', cuid, pid)

            # component is usable - replace the original config
            self._cfg['components'][cuid] = ccfg


    # --------------------------------------------------------------------------
    #
    def _start_agents(self):
        '''
        `start_components()` is very similar to `start_bridges()`, in that it
        interprets a given configuration and creates all listed component
        instances.  Components are, however,  *always* created, independent of
        any existing instances.

        This method will return a list of created component instances.  It is up
        to the callee to watch those components for health and to terminate them
        as needed.  
        '''

        if 'components' not in self._cfg:
            # nothing to do
            return

        # merge specific configs into component specs
        ccfg = self._cfg.get('config', dict()).get('components')
        if ccfg:
            ru.dict_merge(self._cfg['components'], ccfg, ru.OVERWRITE)

        ru_def   = ru.DefaultConfig()
        sbox     = self._session.get_session_sandbox()
        to_check = list()

        for cname in self._cfg['components']:

            cfg = self._cfg['components'][cname]
            cfg['rcfg'] = self._cfg.get('rcfg', dict())
            cfg['acfg'] = self._cfg.get('acfg', dict())

            for num in range(cfg.get('count', 1)):

                ccfg = copy.deepcopy(cfg)

                # merge global config settings
                dcfg = self._cfg.get('default')
                if dcfg:
                    ru.dict_merge(ccfg, dcfg, ru.OVERWRITE)

                cuid = ru.generate_id(cname + '.%(counter)04d', ru.ID_CUSTOM)
                cuid = self._owner + '.' + cuid

                fout = '%s/%s.out' % (sbox, cuid)
                ferr = '%s/%s.err' % (sbox, cuid)
                fcfg = '%s/%s.cfg' % (sbox, cuid)
                fpid = '%s/%s.pid' % (sbox, cuid)

                ccfg['ru_def']     = copy.deepcopy(ru_def.as_dict())
                ccfg['session_id'] = self._session.uid
                ccfg['sandbox']    = sbox

                ccfg['owner']      = self.uid
                ccfg['name']       = cname
                ccfg['kind']       = cname
                ccfg['uid']        = cuid
                ccfg['fpid']       = fpid
                ccfg['fout']       = fout
                ccfg['ferr']       = ferr
                ccfg['ppid']       = os.getpid()

                ru.write_json(ccfg, fcfg)

                cmd  = 'radical-pilot-component %s' % fcfg
                ru.sh_callout_bg(cmd, shell=USE_SHELL)

                self._log.debug('started component %s: %s' % (cuid, cmd))
                to_check.append(ccfg)

        # wait until all components are up
        for ccfg in to_check:

            cuid  = ccfg['uid']
            fpid  = ccfg['fpid']

            start = time.time()

            fin      = None
            pid      = None

            while True:

                if not pid:

                    try   : fin = open(fpid, 'r')
                    except: pass

                    if fin:
                        pid = int(fin.read().split()[1])
                        fin.close()
                        fin = None

                if pid:
                    break

                time.sleep(0.1)
                if time.time() - start > 5.0:  # FIXME: configurable
                    break

            assert(pid), 'component %s not alive (%s)' % (ccfg['uid'], fpid)

            # watch the pid
            ru.pid_watcher(pid=pid, uid=self.uid)

            self._components[cuid] = int(pid)

            self._log.debug('component %s [%s] started', cuid, pid)

            # component is usable - replace the original config
            self._cfg['components'][cuid] = ccfg


    # --------------------------------------------------------------------------
    #
    def close(self, cleanup=False):
        '''
        kill all bridges and components
        '''

        for uid, pid in self._components.iteritems():

            self._log.debug('client kills component %s [%s]', uid, pid)

            try   : os.kill(pid, 9)
            except: pass

        for uid, pid in self._bridges.iteritems():

            self._log.debug('client kills bridge    %s [%s]', uid, pid)

            try   : os.kill(pid, 9)
            except: pass


# -----------------------------------------------------------------------------

