
### Session (Component)
  
    session_start       : session is being created (not reconnected) (uid: sid)
    config_parser_start : begin parsing config files                 (uid: sid)
    config_parser_stop  : stops parsing config files                 (uid: sid)
    session_close       : session close is requested                 (uid: sid)
    session_stop        : session is closed                          (uid: sid)
    session_fetch_start : start fetching logs/profs/json after close (uid: sid, optional)
    session_fetch_stop  : stops fetching logs/profs/json after close (uid: sid, optional)
  
  
### PilotManager (Component)
  
### PMGRLaunchingComponent (Component)
  
### ComputePilot (in session profile, all optional)
    staging_in_start    : pilot level staging request starts         (uid: pid, msg: did)
    staging_in_fail     : pilot level staging request failed         (uid: pid, msg: did)
    staging_in_stop     : pilot level staging request stops          (uid: pid, msg: did)
  
### UnitManager (Component)
  
    get                 : units   received from application          (uid: umgrid, msg: 'bulk size: %d')
    get                 : unit    received from application          (uid: uid)
  
### UMGRSchedulingComponent (Component)
  
### UMGRStagingInputComponent (Component)
  
    create_sandbox_start: create_unit_sandbox starts                 (uid: uid, optional)
    create_sandbox_stop : create_unit_sandbox stops                  (uid: uid, optional)
    staging_in_start    : staging request starts                     (uid: uid, msg: did, optional)
    staging_in_stop     : staging request stops                      (uid: uid, msg: did, optional)
  
### bootstrap_1.sh
    bootstrap_1_start   : pilot bootstrapper 1 starts                (uid: pid)
    tunnel_setup_start  : setting up tunnel    starts                (uid: pid)
    tunnel_setup_stop   : setting up tunnel    stops                 (uid: pid, optional)
    ve_setup_start      : pilot ve setup       starts                (uid: pid)
    ve_create_start     : pilot ve creation    starts                (uid: pid, optional)
    ve_activate_start   : pilot ve activation  starts                (uid: pid, optional)
    ve_activate_start   : pilot ve activation  stops                 (uid: pid, optional)
    ve_update_start     : pilot ve update      starts                (uid: pid, optional)
    ve_update_start     : pilot ve update      stops                 (uid: pid, optional)
    ve_create_stop      : pilot ve creation    stops                 (uid: pid, optional)
    rp_install_start    : rp stack install     starts                (uid: pid, optional)
    rp_install_stop     : rp stack install     stops                 (uid: pid, optional)
    ve_setup_stop       : pilot ve setup       stops                 (uid: pid, optional)
    ve_activate_start   : pilot ve activation  starts                (uid: pid, optional)
    ve_activate_start   : pilot ve activation  stops                 (uid: pid)
    client_barrier_start: wait for client signal                     (uid: pid, optional)
    client_barrier_stop : client signal received                     (uid: pid, optional)
    sync_rel            : time sync event                            (uid: pid, msg: 'agent_0 start')
    cleanup_start       : sandbox deletion     starts                (uid: pid)
    cleanup_stop        : sandbox deletion     stops                 (uid: pid)
    bootstrap_1_stop    : pilot bootstrapper 1 stops                 (uid: pid)
  
### agent_0 (Component)
    sync_rel            : sync with bootstrapper profile             (uid: pid, msg: 'agent_0 start')
    hostname            : host or nodename for agent_0               (uid: pid)
    cmd                 : command received from pmgr                 (uid: pid, msg: command, optional)
    get                 : units   received from unit manager         (uid: pid, msg: 'bulk size: %d')
    get                 : unit    received from unit manager         (uid: uid)
  
  
### AgentSchedulingComponent (Component)
  
    schedule_try        : search for unit resources starts           (uid: uid)
    schedule_fail       : search for unit resources failed           (uid: uid, optional)
    schedule_ok         : search for unit resources succeeded        (uid: uid)
    unschedule_start    : unit resource freeing starts               (uid: uid)
    unschedule_stop     : unit resource freeing stops                (uid: uid)
  
### AgentStagingInputComponent (Component)
  
    staging_in_start    : staging request starts                     (uid: uid, msg: did, optional)
    staging_in_skip     : staging request is not handled here        (uid: uid, msg: did, optional)
    staging_in_fail     : staging request failed                     (uid: uid, msg: did, optional)
    staging_in_stop     : staging request stops                      (uid: uid, msg: did, optional)
  
### AgentExecutingComponent: (Component)
  
    exec_start          : pass to exec layer (orte, ssh, mpi...)     (uid: uid)
    exec_ok             : exec layer accepted task                   (uid: uid)
    exec_fail           : exec layer refused task                    (uid: uid, optional)
    exec_stop           : exec layer passed back control             (uid: uid)
    
    exec_cancel_start   : try to cancel task via exec layer (kill)   (uid: uid, optional)
    exec_cancel_stop    : did cancel    task via exec layer (kill)   (uid: uid, optional)
  
### ABDS : needs sync
### ORTE : as above, no cancel
### POPEN: as above
### SHELL: as above
  
  
### AgentStagingOutputComponent (Component)
  
    staging_stdout_start: reading unit stdout starts                 (uid: uid)
    staging_stdout_stop : reading unit stdout stops                  (uid: uid)
    staging_stderr_start: reading unit stderr starts                 (uid: uid)
    staging_stderr_stop : reading unit stderr stops                  (uid: uid)
    staging_uprof_start : reading unit profile starts                (uid: uid, optional)
    staging_uprof_stop  : reading unit profile stops                 (uid: uid, optional)
    staging_out_start   : staging request starts                     (uid: uid, msg: did, optional)
    staging_out_skip    : staging request is not handled here        (uid: uid, msg: did, optional)
    staging_out_fail    : staging request failed                     (uid: uid, msg: did, optional)
    staging_out_stop    : staging request stops                      (uid: uid, msg: did, optional)
  
### UMGRStagingOutputComponent (Component)
  
    staging_out_start   : staging request starts                     (uid: uid, msg: did, optional)
    staging_out_stop    : staging request stops                      (uid: uid, msg: did, optional)
  
### UpdateWorker (Component)
#### This Component handles DB write updates from client and agent
  
    update_request      : a state update is requested                (uid: uid, msg: state)
    update_pushed       : bulk state update has been sent            (          msg: 'bulk size: %d')
    update_pushed       : a state update has been send               (uid: uid, msg: state)
  
    
### All *Components*
  
    get                 : component receives an entity               (uid: eid, state: estate)
    advance             : component advances entity state            (uid: eid, state: estate) 
    publish             : component publishes entity state           (uid: eid, state: estate) 
    put                 : component pushes an entity out             (uid: eid, state: estate, msg: channel)
    component_init      : component child  initializes
    component_init      : component parent initializes
    component_final     : component finalizes
  
  
### All profiles
  
    sync_abs            : timstamp sync information (*)
    END                 : last entry, profiler is being closed
  
  
### time syncing
    sync_abs            : sets an absolute, NTP synced time stamp
    sync_rel            : sets a *pair* of time stamps considered simultaneous
  
