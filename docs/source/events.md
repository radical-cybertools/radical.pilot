
### Format of this file:

    event_name          : semantic event description (details on 'uid', 'msg', 'state' fields)

Events marked as `optional` depend on the content of unit descriptions etc,
all other events will usually be present in 'normal' runs.  All events have an
event name, a timestamp, and a component (which recorded the event) defined -
all other fields (uid, state, msg) are optional.  The names of the actual
component IDs depend on the exact RP configuration and startup sequence.

The exact order and multiplicity of events is ill defined, as they depend on
many boundary conditions: system properties, system noise, system
synchronization, RP API call order, application timings, RP configuration,
resource configuration, and noise.  However, while a global event model is
thus hard to define, the order presented in the lists below gives some basic
indication on event ordering *within each individual component*.


### All Components

    get                 : component receives an entity               (uid: eid, state: estate)
    advance             : component advances  entity state           (uid: eid, state: estate)
    publish             : component publishes entity state           (uid: eid, state: estate)
    put                 : component pushes an entity out             (uid: eid, state: estate, msg: channel)
    lost                : component lost   an entity (state error)   (uid: eid, state: estate)
    drop                : component drops  an entity (final state)   (uid: eid, state: estate)
    component_init      : component child  initializes after start()
    component_init      : component parent initializes after start()
    component_final     : component finalizes

    partial orders
    * per component     : component_init, *, component_final
    * per entity        : get, advance, publish, put



### Session (Component)

    session_start       : session is being created (not reconnected) (uid: sid)
    config_parser_start : begin parsing config files                 (uid: sid)
    config_parser_stop  : stops parsing config files                 (uid: sid)
    session_close       : session close is requested                 (uid: sid)
    session_stop        : session is closed                          (uid: sid)
    session_fetch_start : start fetching logs/profs/json after close (uid: sid, [API])
    session_fetch_stop  : stops fetching logs/profs/json after close (uid: sid, [API])

    partial orders
    * per session       : session_start, config_parser_start, \
                          config_parser_stop, session_close,  \
                          session_stop,  session_fetch_start, \
                          session_fetch_stop


### PilotManager (Component)

    setup_done          : manager has bootstrapped                   (uid: pmgr)


### PMGRLaunchingComponent (Component)

    staging_in_start    : pilot sandbox staging starts               (uid: pilot)
    staging_in_stop     : pilot sandbox staging stops                (uid: pilot)
    submission_start    : pilot job submission starts                (uid: pilot)
    submission_stop     : pilot job submission stops                 (uid: pilot)

    partial orders
    * per pilot         : staging_in_start, staging_in_stop, \
                          submission_start, submission_stop

### ComputePilot (in session profile, all optional)

    staging_in_start    : pilot level staging request starts         (uid: pilot, msg: did, [PILOT-DS])
    staging_in_fail     : pilot level staging request failed         (uid: pilot, msg: did, [PILOT-DS])
    staging_in_stop     : pilot level staging request stops          (uid: pilot, msg: did, [PILOT-DS])

    partial orders
    * per file          : staging_in_start, (staging_in_fail | staging_in_stop)


### UnitManager (Component)

    setup_done          : manager has bootstrapped                   (uid: umgr)
    get                 : units   received from application          (uid: umgr, msg: 'bulk size: %d')
    get                 : unit    received from application          (uid: unit)


### UMGRSchedulingComponent (Component)


### UMGRStagingInputComponent (Component)

    create_sandbox_start: create_unit_sandbox starts                 (uid: unit, [CU-DS])
    create_sandbox_stop : create_unit_sandbox stops                  (uid: unit, [CU-DS])
    staging_in_start    : staging request starts                     (uid: unit, msg: did, [CU-DS])
    staging_in_stop     : staging request stops                      (uid: unit, msg: did, [CU-DS])
    staging_in_tar_start: tar optimization starts                    (uid: unit, msg: did, [CU-DS])
    staging_in_tar_stop : tar optimization stops                     (uid: unit, msg: did, [CU-DS])

    partial orders
    * per unit          : create_sandbox_start, create_sandbox_stop,
                          (staging_in_start | staging_in_stop)*
    * per file          : staging_in_start, staging_in_stop


### bootstrap_0.sh

    bootstrap_0_start   : pilot bootstrapper 1 starts                (uid: pilot)
    tunnel_setup_start  : setting up tunnel    starts                (uid: pilot)
    tunnel_setup_stop   : setting up tunnel    stops                 (uid: pilot, [CFG-R])
    ve_setup_start      : pilot ve setup       starts                (uid: pilot)
    ve_create_start     : pilot ve creation    starts                (uid: pilot, [CFG-R])
    ve_activate_start   : pilot ve activation  starts                (uid: pilot, [CFG-R])
    ve_activate_start   : pilot ve activation  stops                 (uid: pilot, [CFG-R])
    ve_update_start     : pilot ve update      starts                (uid: pilot, [CFG-R])
    ve_update_start     : pilot ve update      stops                 (uid: pilot, [CFG-R])
    ve_create_stop      : pilot ve creation    stops                 (uid: pilot, [CFG-R])
    rp_install_start    : rp stack install     starts                (uid: pilot, [CFG-R])
    rp_install_stop     : rp stack install     stops                 (uid: pilot, [CFG-R])
    ve_setup_stop       : pilot ve setup       stops                 (uid: pilot, [CFG-R])
    ve_activate_start   : pilot ve activation  starts                (uid: pilot, [CFG-R])
    ve_activate_start   : pilot ve activation  stops                 (uid: pilot)
    client_barrier_start: wait for client signal                     (uid: pilot, [CFG-R])
    client_barrier_stop : client signal received                     (uid: pilot, [CFG-R])
    sync_rel            : time sync event                            (uid: pilot, msg: 'agent_0 start')
    cleanup_start       : sandbox deletion     starts                (uid: pilot)
    cleanup_stop        : sandbox deletion     stops                 (uid: pilot)
    bootstrap_0_stop    : pilot bootstrapper 1 stops                 (uid: pilot)

    partial orders
    * as above


### agent_0 (Component)

    sync_rel            : sync with bootstrapper profile             (uid: pilot, msg: 'agent_0 start')
    hostname            : host or nodename for agent_0               (uid: pilot)
    cmd                 : command received from pmgr                 (uid: pilot, msg: command, [API])
    get                 : units   received from unit manager         (uid: pilot, msg: 'bulk size: %d')
    get                 : unit    received from unit manager         (uid: unit)
    dvm_start           : DVM startup by launch method               (uid: pilot) [CFG-R])
    dvm_ok              : DVM startup completed                      (uid: pilot) [CFG-R])
    dvm_fail            : DVM startup failed                         (uid: pilot) [CFG-R])
    dvm_stop            : DVM stopped                                (uid: pilot) [CFG-R])


    partial orders
    * per instance      : sync_rel, hostname, (cmd | get)*
    * per instance      : dvm_start, (dvm_ok | dvm_fail), dvm_stop


### AgentSchedulingComponent (Component)

    schedule_try        : search for unit resources starts           (uid: unit)
    schedule_fail       : search for unit resources failed           (uid: unit, [RUNTIME])
    schedule_ok         : search for unit resources succeeded        (uid: unit)
    unschedule_start    : unit resource freeing starts               (uid: unit)
    unschedule_stop     : unit resource freeing stops                (uid: unit)

    partial orders
    * per unit          : schedule_try, schedule_fail*, schedule_ok, \
                          unschedule_start, unschedule_stop


### AgentStagingInputComponent (Component)

    staging_in_start    : staging request starts                     (uid: unit, msg: did, [CU-DS])
    staging_in_skip     : staging request is not handled here        (uid: unit, msg: did, [CU-DS])
    staging_in_fail     : staging request failed                     (uid: unit, msg: did, [CU-DS])
    staging_in_stop     : staging request stops                      (uid: unit, msg: did, [CU-DS])

    partial orders
    * per file          : staging_in_skip 
                        | (staging_in_start, (staging_in_fail | staging_in_stop))


### AgentExecutingComponent: (Component)

    exec_mkdir          : creation of sandbox requested              (uid: unit)
    exec_mkdir_done     : creation of sandbox completed              (uid: unit)
    exec_start          : pass to exec layer (orte, ssh, mpi...)     (uid: unit)
    exec_ok             : exec layer accepted task                   (uid: unit)
    exec_fail           : exec layer refused task                    (uid: unit, [RUNTIME], optional)
    cu_start            : cu shell script: starts                    (uid: unit)
    cu_cd_done          : cu shell script: changed workdir           (uid: unit)
    cu_pre_start        : cu shell script: pre-exec starts           (uid: unit, [CU_PRE])
    cu_pre_stop         : cu shell script: pre_exec stopped          (uid: unit, [CU_PRE])
    cu_exec_start       : cu shell script: launch method starts      (uid: unit)
    app_start           : application executable started             (uid: unit, [APP])
    app_*               : application specific events                (uid: unit, [APP], optional)
    app_stop            : application executable stops               (uid: unit, [APP])
    cu_exec_stop        : cu shell script: launch method returned    (uid: unit)
    cu_post_start       : cu shell script: post-exec starts          (uid: unit, [CU_POST])
    cu_post_stop        : cu shell script: post_exec stopped         (uid: unit, [CU_POST])
    cu_stop             : cu shell script: stops                     (uid: unit)
    exec_stop           : exec layer passed back control             (uid: unit)

    exec_cancel_start   : try to cancel task via exec layer (kill)   (uid: unit, [API])
    exec_cancel_stop    : did cancel    task via exec layer (kill)   (uid: unit, [API])

    partial orders
    * per unit          : exec_start, (exec_ok | exec_fail), cu_start, 
                          cu_cd_done, cu_pre_start, cu_pre_stop, cu_exec_start,
                          app_start, app_*, app_stop, cu_exec_stop,
                          cu_post_start, cu_post_stop, cu_stop, exec_stop
    * per unit          : exec_cancel_start, exec_cancel_stop


### AgentStagingOutputComponent (Component)

    staging_stdout_start: reading unit stdout starts                 (uid: unit)
    staging_stdout_stop : reading unit stdout stops                  (uid: unit)
    staging_stderr_start: reading unit stderr starts                 (uid: unit)
    staging_stderr_stop : reading unit stderr stops                  (uid: unit)
    staging_uprof_start : reading unit profile starts                (uid: unit, [APP])
    staging_uprof_stop  : reading unit profile stops                 (uid: unit, [APP])
    staging_out_start   : staging request starts                     (uid: unit, msg: did, [CU-DS])
    staging_out_skip    : staging request is not handled here        (uid: unit, msg: did, [CU-DS])
    staging_out_fail    : staging request failed                     (uid: unit, msg: did, [CU-DS])
    staging_out_stop    : staging request stops                      (uid: unit, msg: did, [CU-DS])

    partial orders 
    * per unit          : staging_stdout_start, staging_stdout_stop,
                          staging_stderr_start, staging_stderr_stop,
                          staging_uprof_start,  staging_uprof_stop,
    * per file          : staging_out_skip \
                        | (staging_out_start, (staging_out_fail | staging_out_stop))


### UMGRStagingOutputComponent (Component)

    staging_out_start   : staging request starts                     (uid: unit, msg: did, [CU-DS])
    staging_out_stop    : staging request stops                      (uid: unit, msg: did, [CU-DS])

    partial orders
    * per file          : staging_out_start, staging_out_stop


### UpdateWorker (Component)

    update_request      : a state update is requested                (uid: unit, msg: state)
    update_pushed       : bulk state update has been sent            (           msg: 'bulk size: %d')
    update_pushed       : a state update has been send               (uid: unit, msg: state)

    partial orders
    * per state update  : update_request, update_pushed


### All profiles

    sync_abs            : sets an absolute, NTP synced time stamp               ([INTERNAL])
    sync_rel            : sets a *pair* of time stamps considered simultaneous  ([INTERNAL])
    END                 : last entry, profiler is being closed

    partial orders
    * per profile       : (sync_abs | sync_rel), *, END



## Conditional events

    - [API]           - only for corresponding RP API calls
    - [CFG]           - only for some RP configurations
      - [CFG-R]       - only for some bootstrapping configurations
      - [CFG-DVM]     - only for launch methods which use a DVM
    - [CU]            - only for some CU descriptions
      - [CU-DS]       - only for units specifying data staging directives
      - [CU-PRE]      - only for units specifying pre-exec directives
      - [CU-POST]     - only for units specifying post-exec directives
    - [PILOT]         - only for certain pilot
    - [APP]           - only for applications writing compatible profiles
    - [RUNTIME]       - only on  certain runtime decisions and system configuration
    - [INTERNAL]      - only for certain internal states

