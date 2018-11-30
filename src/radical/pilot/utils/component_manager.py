
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import copy
import time

import subprocess    as sp
import radical.utils as ru

# FIXME: this depends on nohup - implement in python or sh...


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
    def __init__(self, session, cfg, owner=None):

        self._session    = session
        self._cfg        = cfg
        self._owner      = owner

        self._bridges    = dict()
        self._components = dict()

        if self._owner:
            self._uid = ru.generate_id('%s.cmgr' % self._owner, ru.ID_CUSTOM)
        else:
            self._uid = ru.generate_id('%s.cmgr' % self._session.uid, ru.ID_CUSTOM)

        self._prof = self._session.get_profiler(name=self._uid)
        self._log  = self._session.get_logger  (name=self._uid, level='DEBUG')

        self._start_bridges()
        self._start_components()


    # --------------------------------------------------------------------------
    #
    def _start_bridges(self):

        # FIXME: all file system interactions should happen in the session
        #        sandbox - which is not defined, and is differently formed for
        #        client and agent side
        to_check = list()
        for bname in self._cfg.get('bridges', {}):

            buid = bname
            sbox = self._session.get_session_sandbox()
            bcfg = copy.deepcopy(self._cfg)
            ru.dict_merge(bcfg, self._cfg['bridges'][bname], ru.OVERWRITE)

            del(bcfg['bridges'])
            del(bcfg['components'])

            bout = '%s/%s.out' % (sbox, buid)
            fcfg = '%s/%s.cfg' % (sbox, buid)
            fpid = '%s/%s.pid' % (sbox, buid)
            furl = '%s/%s.url' % (sbox, buid)

            bcfg['sid']       = self._session.uid
            bcfg['name']      = bname
            bcfg['uid']       = buid
            bcfg['pwd']       = sbox
            bcfg['fpid']      = fpid
            bcfg['furl']      = furl
            bcfg['owner']     = self._owner
            bcfg['ppid']      = os.getpid()
            bcfg['log_level'] = 'DEBUG'  ## FIXME

            if   'pubsub' in bname: bcfg['kind'] = 'pubsub'
            elif 'queue'  in bname: bcfg['kind'] = 'queue'
            else: raise ValueError('invalid bridge name %s' % bname)

            ru.write_json(bcfg, fcfg)
            fout = open(bout, 'w')
            cmd  = 'radical-pilot-bridge %s &' % fcfg
            pid  = sp.Popen(cmd, shell=True, 
                            stdout=fout, stderr=sp.STDOUT)
            self._bridges[buid] = pid

            to_check.append(bcfg)

        # now check if all bridges came up
        for bcfg in to_check:

            # wait until the bridge is up.  The URL file will appear only
            # after the pid file - but we have to wait for it,  otherwise
            # the bridge *process* is up, but the  bridge is not yet usable.
            alive = False
            fpid  = bcfg['fpid']
            furl  = bcfg['furl']
            start = time.time()
            while True:
                if os.path.isfile(furl) and os.path.isfile(fpid):
                    with open(fpid, 'r') as fin:
                        pid = fin.read().strip()
                        if pid:
                            self._bridges[buid] = int(pid)
                            alive = True
                            break

                time.sleep(0.1)
                if time.time() - start > 5.0:
                    break

            assert(alive), 'bridge %s not alive' % bcfg['uid']


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

        to_check = list()
        for cname in self._cfg.get('components', {}):

            sbox = self._session.get_session_sandbox()
            ccfg = copy.deepcopy(self._cfg)
            ru.dict_merge(ccfg, self._cfg['components'][cname], ru.OVERWRITE)

            del(ccfg['bridges'])
            del(ccfg['components'])

            for num in range(ccfg.get('count', 1)):

                cuid = ru.generate_id(cname + '.%(counter)04d', ru.ID_CUSTOM)
                if self._owner:
                    cuid = self._owner + '.' + cuid

                cout = '%s/%s.out' % (sbox, cuid)
                fcfg = '%s/%s.cfg' % (sbox, cuid)
                fpid = '%s/%s.pid' % (sbox, cuid)
                fchk = '%s/%s.chk' % (sbox, cuid)

                ccfg['sid']   = self._session.uid
                ccfg['uid']   = cuid
                ccfg['kind']  = cname
                ccfg['pwd']   = sbox
                ccfg['fpid']  = fpid
                ccfg['fchk']  = fchk
                ccfg['owner'] = self._owner
                ccfg['ppid']  = os.getpid()

                ccfg['log_level'] = 'DEBUG'  ## FIXME

                ru.write_json(ccfg, fcfg)
                fout = open(cout, 'w')
                cmd  = 'nohup radical-pilot-component %s &' % fcfg
                pid  = sp.Popen(cmd, shell=True,
                                stdout=fout, stderr=sp.STDOUT)
                self._components[cuid] = pid

                to_check.append(ccfg)

        # wait until all components are up
        for ccfg in to_check:

            alive = False
            fpid  = ccfg['fpid']
            fchk  = ccfg['fchk']
            start = time.time()
            while True:
                if os.path.isfile(fchk) and os.path.isfile(fpid):
                    with open(fpid, 'r') as fin:
                        pid = fin.read().strip()
                        if pid:
                            self._components[cuid] = int(pid)
                            alive = True
                            break

                time.sleep(0.1)
                if time.time() - start > 5.0:
                    break

            assert(alive), 'component %s not alive' % ccfg['uid']


    # --------------------------------------------------------------------------
    #
    def close(self, cleanup=False):
        '''
        kill all bridges and components
        '''

        for uid, pid in self._components.iteritems():
            self._log.debug('client kills component %s [%s]', uid, pid)
            try:
                os.kill(pid, 9)
            except:
                pass

        for uid, pid in self._bridges.iteritems():
            self._log.debug('client kills bridge    %s [%s]', uid, pid)
            try:
                os.kill(pid, 9)
            except:
                pass


# -----------------------------------------------------------------------------

