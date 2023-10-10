
__copyright__ = 'Copyright 2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

# pylint: disable=global-statement   # W0603 global `_components`

import os
import copy
import time

import threading       as mt
import radical.utils   as ru

from ..          import constants      as rpc
from ..          import states         as rps
from ..messages  import HeartbeatMessage


# ------------------------------------------------------------------------------
#
class ComponentManager(object):
    '''
    RP spans a hierarchy of component instances: the application has a pmgr and
    tmgr, and the tmgr has a staging component and a scheduling component, and
    the pmgr has a launching component, and components also can have bridges,
    etc. This ComponentManager centralises the code needed to spawn, manage and
    terminate such components. Any code which needs to create component should
    create a ComponentManager instance and pass the required component and
    bridge layout and configuration.  Callng `stop()` on the cmgr will terminate
    the components and brisged.
    '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, sid, reg_addr, owner):

        # create a registry client to obtain the session config and to store
        # component and bridge configs

        self._sid      = sid
        self._reg_addr = reg_addr
        self._owner    = owner

        self._reg    = ru.zmq.RegistryClient(url=self._reg_addr)
        self._cfg    = ru.Config(from_dict=self._reg['cfg'])
        self._hb_cfg = ru.Config(from_dict=self._reg['heartbeat'])

        self._uid    = ru.generate_id('cmgr.%(item_counter)04d',
                                      ru.ID_CUSTOM, ns=self._sid)

        self._prof   = ru.Profiler(self._uid, ns='radical.pilot',
                                   path=self._cfg.path)
        self._log    = ru.Logger(self._uid, ns='radical.pilot',
                                 path=self._cfg.path,
                                 level=self._cfg.log_lvl,
                                 debug=self._cfg.debug_lvl)

        self._prof.prof('init2', uid=self._uid, msg=self._cfg.path)

        self._log.debug('cmgr %s (%s)', self._uid, self._owner)

        # component managers listen on the heartbeat pubsub to see if spawned
        # components come alive
        self._heartbeats = dict()  # heartbeats we have seen
        ru.zmq.Subscriber(channel='heartbeat_pubsub',
                          topic='heartbeat',
                          url=self._hb_cfg.addr_sub,
                          cb=self._hb_msg_cb,
                          log=self._log,
                          prof=self._prof)


    # --------------------------------------------------------------------------
    #
    def _hb_msg_cb(self, topic, msg):

        hb_msg = HeartbeatMessage(from_dict=msg)
        self._heartbeats[hb_msg.uid] = time.time()


    # --------------------------------------------------------------------------
    #
    def _wait_startup(self, uids, timeout):
        '''
        Wait for the first heartbeat of the given component UIDs to appear.  If
        that does not happen before timeout, an exception is raised.
        '''

        start = time.time()
        ok    = list()
        nok   = uids
        while True:

            self._log.debug('wait for : %s', nok)

            ok  = [uid for uid in uids if uid     in self._heartbeats]
            nok = [uid for uid in uids if uid not in ok]

            if len(ok) == len(uids):
                break

            if time.time() - start > timeout:
                self._log.debug('wait failed: %s', nok)
                raise RuntimeError('uids %s not found' % nok)

            time.sleep(0.25)

        self._log.debug('wait for done: %s', ok)


    # --------------------------------------------------------------------------
    #
    @property
    def uid(self):
        return self._uid


    # --------------------------------------------------------------------------
    #
    def start_bridges(self, bridges):

        self._prof.prof('start_bridges_start', uid=self._uid)

        buids = list()
        for bname, bcfg in bridges.items():

            uid = bname
            buids.append(uid)

            bcfg.uid       = uid
            bcfg.channel   = bname
            bcfg.cmgr      = self.uid
            bcfg.owner     = self._owner
            bcfg.sid       = self._cfg.sid
            bcfg.path      = self._cfg.path
            bcfg.reg_addr  = self._cfg.reg_addr
            bcfg.log_lvl   = self._cfg.log_lvl
            bcfg.debug_lvl = self._cfg.debug_lvl
            bcfg.heartbeat = self._hb_cfg

            self._reg['bridges.%s.cfg' % bname] = bcfg

          # self._reg.put('bridge.%s' % bname, bcfg)
            cmd = 'radical-pilot-bridge %s %s %s' \
                % (self._sid, self._reg.url, bname)

            out, err, ret = ru.sh_callout(cmd, cwd=self._cfg.path)

            if ret:
                raise RuntimeError('bridge startup failed')

            self._heartbeats[bname] = None
            self._log.info('created bridge %s [%s]', bname, bname)

        # all bridges are started, wait for their heartbeats
        self._log.debug('wait   for %s', buids)
        self._wait_startup(buids, timeout=self._hb_cfg.timeout)

        self._prof.prof('start_bridges_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def start_components(self, components, cfg = None):

        self._prof.prof('start_components_start: %s', uid=self._uid)

        cuids = list()
        for cname, ccfg in components.items():

            for _ in range(ccfg.get('count', 1)):

                uid = ru.generate_id(cname + '.%(item_counter)04d',
                                     ru.ID_CUSTOM, ns=self._sid)
                cuids.append(uid)

                ccfg.uid       = uid
                ccfg.kind      = cname
                ccfg.owner     = self._owner
                ccfg.sid       = self._cfg.sid
                ccfg.cmgr      = self._cfg.uid
                ccfg.base      = self._cfg.base
                ccfg.path      = self._cfg.path
                ccfg.reg_addr  = self._cfg.reg_addr
                ccfg.proxy_url = self._cfg.proxy_url
                ccfg.log_lvl   = self._cfg.log_lvl
                ccfg.debug_lvl = self._cfg.debug_lvl
                ccfg.heartbeat = self._hb_cfg

                if cfg:
                    ru.dict_merge(ccfg, cfg, ru.OVERWRITE)

                self._reg['components.%s.cfg' % uid] = ccfg

                self._log.info('create  component %s [%s]', cname, uid)

                cmd = 'radical-pilot-component %s %s %s' \
                    % (self._sid, self._reg.url, uid)
                out, err, ret = ru.sh_callout(cmd, cwd=self._cfg.path)

                self._log.debug('component startup out: %s' , out)
                self._log.debug('component startup err: %s' , err)

                if ret:
                    raise RuntimeError('component startup failed')

                self._log.info('created component %s [%s]', cname, uid)

        # all components should start now, wait for heartbeats to appear.
        self._log.debug('wait   for %s', cuids)
        self._wait_startup(cuids, timeout=self._hb_cfg.timeout)

        self._prof.prof('start_components_stop', uid=self._uid)


    # --------------------------------------------------------------------------
    #
    def close(self):

        self._prof.prof('close', uid=self._uid)


# ------------------------------------------------------------------------------
