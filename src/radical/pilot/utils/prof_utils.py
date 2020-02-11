
import os
import glob

import radical.utils as ru

from ..       import states as rps
from .session import fetch_json

_debug = os.environ.get('RP_PROF_DEBUG')


# ------------------------------------------------------------------------------
#
# pilot and unit activities: core hours are derived by multiplying the
# respective time durations with pilot size / unit size.  The 'idle' utilization
# and the 'agent' utilization are derived separately.
#
# Note that durations should add up to the `x_total` generations to ensure
# accounting for the complete unit/pilot utilization.


PILOT_DURATIONS = {
        'provide' : {
            'total'     : [{ru.EVENT: 'bootstrap_0_start'},
                           {ru.EVENT: 'bootstrap_0_stop' }]
        },

        # times between PMGR_ACTIVE and the termination command are not
        # considered pilot specific consumptions.  If some resources remain
        # unused during that time, it is either due to inefficiencies of
        # workload management (accounted for in the unit consumption metrics),
        # or the pilot is starving for workload.
        #
        'consume' : {
            'boot'      : [{ru.EVENT: 'bootstrap_0_start'},
                           {ru.EVENT: 'sync_rel'         }],
            'setup_1'   : [{ru.EVENT: 'sync_rel'         },
                           {ru.STATE: rps.PMGR_ACTIVE    }],
            'ignore'    : [{ru.STATE: rps.PMGR_ACTIVE    },
                           {ru.EVENT: 'cmd'              }],
            'term'      : [{ru.EVENT: 'cmd'              },
                           {ru.EVENT: 'bootstrap_0_stop' }],
        },


        # FIXME: separate out DVM startup time
        #   'rte'       : [{ru.STATE: rps.PMGR_ACTIVE    },
        #                  {ru.STATE: rps.PMGR_ACTIVE    }],
        #   'setup_2'   : [{ru.STATE: rps.PMGR_ACTIVE    },
        #                  {ru.STATE: rps.PMGR_ACTIVE    }],

        # resources on agent nodes are consumed for all of the pilot's lifetime
        'agent' : {
            'total'     : [{ru.EVENT: 'bootstrap_0_start'},
                           {ru.EVENT: 'bootstrap_0_stop' }]
        }
}


UNIT_DURATIONS_DEFAULT = {
        'consume' : {
            'exec_queue'  : [{ru.EVENT: 'schedule_ok'            },
                             {ru.STATE: rps.AGENT_EXECUTING      }],
            'exec_prep'   : [{ru.STATE: rps.AGENT_EXECUTING      },
                             {ru.EVENT: 'exec_start'             }],
            'exec_rp'     : [{ru.EVENT: 'exec_start'             },
                             {ru.EVENT: 'cu_start'               }],
            'exec_sh'     : [{ru.EVENT: 'cu_start'               },
                             {ru.EVENT: 'cu_start'               }],
            'exec_cmd'    : [{ru.EVENT: 'cu_start'               },
                             {ru.EVENT: 'cu_stop'                }],
            'term_sh'     : [{ru.EVENT: 'cu_stop'                },
                             {ru.EVENT: 'cu_stop'                }],
            'term_rp'     : [{ru.EVENT: 'cu_stop'                },
                             {ru.EVENT: 'exec_stop'              }],
            'unschedule'  : [{ru.EVENT: 'exec_stop'              },
                             {ru.EVENT: 'unschedule_stop'        }]

          # # if we have cmd_start / cmd_stop:
          # 'exec_sh'     : [{ru.EVENT: 'cu_start'               },
          #                  {ru.EVENT: 'cmd_start'              }],
          # 'exec_cmd'    : [{ru.EVENT: 'cmd_start'              },
          #                  {ru.EVENT: 'cmd_stop'               }],
          # 'term_sh'     : [{ru.EVENT: 'cmd_stop'               },
          #                  {ru.EVENT: 'cu_stop'                }],
        }
}

UNIT_DURATIONS_PRTE = {
        'consume' : {
            'exec_queue'  : [{ru.EVENT: 'schedule_ok'            },
                             {ru.STATE: rps.AGENT_EXECUTING      }],
            'exec_prep'   : [{ru.STATE: rps.AGENT_EXECUTING      },
                             {ru.EVENT: 'exec_start'             }],
            'exec_rp'     : [{ru.EVENT: 'exec_start'             },
                             {ru.EVENT: 'cu_start'               }],
            'exec_sh'     : [{ru.EVENT: 'cu_start'               },
                             {ru.EVENT: 'cu_exec_start'          }],
            'prte_phase_1': [{ru.EVENT: 'cu_exec_start'          },
                             {ru.EVENT: 'prte_init_complete'     }],
            'prte_phase_2': [{ru.EVENT: 'prte_init_complete'     },
                             {ru.EVENT: 'prte_sending_launch_msg'}],
            'exec_cmd'    : [{ru.EVENT: 'prte_sending_launch_msg'},
                             {ru.EVENT: 'prte_iof_complete'      }],
            'prte_phase_3': [{ru.EVENT: 'prte_iof_complete'      },
                             {ru.EVENT: 'prte_notify_completed'  }],
            'term_sh'     : [{ru.EVENT: 'prte_notify_completed'  },
                             {ru.EVENT: 'cu_stop'                }],
            'term_rp'     : [{ru.EVENT: 'cu_stop'                },
                             {ru.EVENT: 'exec_stop'              }],
            'unschedule'  : [{ru.EVENT: 'exec_stop'              },
                             {ru.EVENT: 'unschedule_stop'        }],

            # if we have cmd_start / cmd_stop:
            'prte_phase_2': [{ru.EVENT: 'prte_init_complete'     },
                             {ru.EVENT: 'app_start'              }],
            'exec_cmd'    : [{ru.EVENT: 'app_start'              },
                             {ru.EVENT: 'app_stop'               }],
            'prte_phase_3': [{ru.EVENT: 'app_stop'               },
                             {ru.EVENT: 'prte_notify_completed'  }],

          # # if we have app_start / app_stop:
          # 'prte_phase_2': [{ru.EVENT: 'prte_init_complete'     },
          #                  {ru.EVENT: 'cmd_start'              }],
          # 'exec_cmd'    : [{ru.EVENT: 'cmd_start'              },
          #                  {ru.EVENT: 'cmd_stop'               }],
          # 'prte_phase_3': [{ru.EVENT: 'cmd_stop'               },
          #                  {ru.EVENT: 'prte_notify_completed'  }],
        }
}


# ------------------------------------------------------------------------------
#
def get_hostmap(profile):
    '''
    We abuse the profile combination to also derive a pilot-host map, which
    will tell us on what exact host each pilot has been running.  To do so, we
    check for the PMGR_ACTIVE advance event in agent.0.prof, and use the NTP
    sync info to associate a hostname.
    '''
    # FIXME: This should be replaced by proper hostname logging
    #        in `pilot.resource_details`.

    hostmap = dict()  # map pilot IDs to host names
    for entry in profile:
        if  entry[ru.EVENT] == 'hostname':
            hostmap[entry[ru.UID]] = entry[ru.MSG]

    return hostmap


# ------------------------------------------------------------------------------
#
def get_hostmap_deprecated(profiles):
    '''
    This method mangles combine_profiles and get_hostmap, and is deprecated.  At
    this point it only returns the hostmap
    '''

    hostmap = dict()  # map pilot IDs to host names
    for pname, prof in profiles.items():

        if not len(prof):
            continue

        if not prof[0][ru.MSG]:
            continue

        host, ip, _, _, _ = prof[0][ru.MSG].split(':')
        host_id = '%s:%s' % (host, ip)

        for row in prof:

            if 'agent.0.prof' in pname    and \
                row[ru.EVENT] == 'advance' and \
                row[ru.STATE] == rps.PMGR_ACTIVE:
                hostmap[row[ru.UID]] = host_id
                break

    return hostmap


# ------------------------------------------------------------------------------
#
def get_session_profile(sid, src=None):

    if not src:
        src = "%s/%s" % (os.getcwd(), sid)

    if os.path.exists(src):
        # we have profiles locally
        profiles  = glob.glob("%s/*.prof"   % src)
        profiles += glob.glob("%s/*/*.prof" % src)
    else:
        # need to fetch profiles
        from .session import fetch_profiles
        profiles = fetch_profiles(sid=sid, skip_existing=True)

    #  filter out some frequent, but uninteresting events
    efilter = {ru.EVENT: [
                        # 'get',
                          'publish',
                          'schedule_skip',
                          'schedule_fail',
                          'staging_stderr_start',
                          'staging_stderr_stop',
                          'staging_stdout_start',
                          'staging_stdout_stop',
                          'staging_uprof_start',
                          'staging_uprof_stop',
                          'update_pushed',
                         ]}

    profiles          = ru.read_profiles(profiles, sid, efilter=efilter)
    profile, accuracy = ru.combine_profiles(profiles)
    profile           = ru.clean_profile(profile, sid, rps.FINAL, rps.CANCELED)
    hostmap           = get_hostmap(profile)

    if not hostmap:
        # FIXME: legacy host notation - deprecated
        hostmap = get_hostmap_deprecated(profiles)

    return profile, accuracy, hostmap


# ------------------------------------------------------------------------------
#
def get_session_description(sid, src=None, dburl=None):
    """
    This will return a description which is usable for radical.analytics
    evaluation.  It informs about
      - set of stateful entities
      - state models of those entities
      - event models of those entities (maybe)
      - configuration of the application / module

    If `src` is given, it is interpreted as path to search for session
    information (json dump).  `src` defaults to `$PWD/$sid`.

    if `dburl` is given, its value is used to fetch session information from
    a database.  The dburl value defaults to `RADICAL_PILOT_DBURL`.
    """

    if not src:
        src = "%s/%s" % (os.getcwd(), sid)

    if os.path.isfile('%s/%s.json' % (src, sid)):
        json = ru.read_json('%s/%s.json' % (src, sid))
    else:
        ftmp = fetch_json(sid=sid, dburl=dburl, tgt=src, skip_existing=True)
        json = ru.read_json(ftmp)

    # make sure we have uids
    # FIXME v0.47: deprecate
    def fix_json(json):
        def fix_uids(json):
            if isinstance(json, list):
                for elem in json:
                    fix_uids(elem)
            elif isinstance(json, dict):
                if 'unitmanager' in json and 'umgr' not in json:
                    json['umgr'] = json['unitmanager']
                if 'pilotmanager' in json and 'pmgr' not in json:
                    json['pmgr'] = json['pilotmanager']
                if '_id' in json and 'uid' not in json:
                    json['uid'] = json['_id']
                    if 'cfg' not in json:
                        json['cfg'] = dict()
                for k,v in json.items():
                    fix_uids(v)
        fix_uids(json)
    fix_json(json)

    assert(sid == json['session']['uid']), 'sid inconsistent'

    ret             = dict()
    ret['entities'] = dict()

    tree      = dict()
    tree[sid] = {'uid'      : sid,
                 'etype'    : 'session',
                 'cfg'      : json['session']['cfg'],
                 'has'      : ['umgr', 'pmgr'],
                 'children' : list()
                }

    for pmgr in sorted(json['pmgr'], key=lambda k: k['uid']):
        uid = pmgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'pmgr',
                     'cfg'      : pmgr['cfg'],
                     'has'      : ['pilot'],
                     'children' : list()
                    }

    for umgr in sorted(json['umgr'], key=lambda k: k['uid']):
        uid = umgr['uid']
        tree[sid]['children'].append(uid)
        tree[uid] = {'uid'      : uid,
                     'etype'    : 'umgr',
                     'cfg'      : umgr['cfg'],
                     'has'      : ['unit'],
                     'children' : list()
                    }
        # also inject the pilot description, and resource specifically
        tree[uid]['description'] = dict()

    for pilot in sorted(json['pilot'], key=lambda k: k['uid']):
        uid  = pilot['uid']
        pmgr = pilot['pmgr']
        pilot['cfg']['resource_details'] = pilot['resource_details']
        tree[pmgr]['children'].append(uid)
        tree[uid] = {'uid'        : uid,
                     'etype'      : 'pilot',
                     'cfg'        : pilot['cfg'],
                     'description': pilot['description'],
                     'has'        : ['unit'],
                     'children'   : list()
                    }
        # also inject the pilot description, and resource specifically

    for unit in sorted(json['unit'], key=lambda k: k['uid']):
        uid  = unit['uid']
        pid  = unit['pilot']
        umgr = unit['umgr']
        tree[pid ]['children'].append(uid)
        tree[umgr]['children'].append(uid)
        tree[uid] = {'uid'         : uid,
                     'etype'       : 'unit',
                     'cfg'         : unit,
                     'description' : unit['description'],
                     'has'         : list(),
                     'children'    : list()
                    }
        # remove duplicate
        del(tree[uid]['cfg']['description'])

    ret['tree'] = tree

    ret['entities']['pilot']   = {'state_model'  : rps._pilot_state_values,
                                  'state_values' : rps._pilot_state_inv_full,
                                  'event_model'  : dict()}
    ret['entities']['unit']    = {'state_model'  : rps._unit_state_values,
                                  'state_values' : rps._unit_state_inv_full,
                                  'event_model'  : dict()}
    ret['entities']['session'] = {'state_model'  : None,  # has no states
                                  'state_values' : None,
                                  'event_model'  : dict()}

    ret['config'] = dict()  # session config goes here

    return ret


# ------------------------------------------------------------------------------
#
def get_node_index(node_list, node, cpn, gpn):

    r0 = node_list.index(node) * (cpn + gpn)
    r1 = r0 + cpn + gpn - 1

    return [r0, r1]


# ------------------------------------------------------------------------------
#
def get_duration(thing, dur):


    for e in dur:
        if ru.STATE in e and ru.EVENT not in e:
            e[ru.EVENT] = 'state'

    t0 = thing.timestamps(event=dur[0])
    t1 = thing.timestamps(event=dur[1])

    if not len(t0) or not len(t1):
        return [None, None]

    return(t0[0], t1[-1])


# ------------------------------------------------------------------------------
#
def cluster_resources(resources):
    # resources is a list of
    #   - single index (single core of gpu
    #   - [r0, r1] tuples (ranges of core, gpu indexes)
    # cluster continuous stretches of resources

    ret = list()
    idx = set()
    for r in resources:
        if isinstance(r, int):
            idx.add(r)
        else:
            for idx in range(r[0], r[1] + 1):
                idx.add(idx)

    r0 = None
    r1 = None
    for i in sorted(list(idx)):
        if r0 is None:
            r0 = i
            continue
        if r1 is None:
            if i == r0 + 1:
                r1 = i
                continue
            ret.append([r0, r0])
            r0 = None
            continue
        if i == r1 + 1:
            r1 = i
            continue
        ret.append([r0, r1])
        r0 = i
        r1 = None

    if r0 is not None:
        if r1 is not None:
            ret.append([r0, r1])
        else:
            ret.append([r0, r0])

    return ret


# ------------------------------------------------------------------------------
#
def _get_pilot_provision(session, pilot):

    pid   = pilot.uid
    cpn   = pilot.cfg['resource_details']['rm_info']['cores_per_node']
    gpn   = pilot.cfg['resource_details']['rm_info']['gpus_per_node']
    ret   = dict()

    nodes, anodes, pnodes = _get_nodes(pilot)

    for metric in PILOT_DURATIONS['provide']:

        boxes  = list()
        t0, t1 = get_duration(pilot, PILOT_DURATIONS['provide'][metric])

        if t0 is None:
            t0 = pilot.events [0][ru.TIME]
            t1 = pilot.events[-1][ru.TIME]

        for node in nodes:
            r0, r1 = get_node_index(nodes, node, cpn, gpn)
            boxes.append([t0, t1, r0, r1])

        ret['total'] = {pid: boxes}

    return ret


# ------------------------------------------------------------------------------
#
def get_provided_resources(session):
    '''
    For all ra.pilots, return the amount and time of resources provided.
    This computes sets of 4-tuples of the form: [t0, t1, r0, r1] where:

        t0: time, begin of resource provision
        t1: time, begin of resource provision
        r0: int,  index of resources provided (min)
        r1: int,  index of resources provided (max)

    The tuples are formed so that t0 to t1 and r0 to r1 are continuous.
    '''

    provided = dict()
    for p in session.get(etype='pilot'):

        data = _get_pilot_provision(session, p)

        for metric in data:

            if metric not in provided:
                provided[metric] = dict()

            for uid in data[metric]:
                provided[metric][uid] = data[metric][uid]

    return provided


# ------------------------------------------------------------------------------
#
def get_consumed_resources(session):
    '''
    For all ra.pilot or ra.unit entities, return the amount and time of
    resources consumed.  A consumed resource is characterized by:

      - a resource type (we know about cores and gpus)
      - a metric name (what the resource was used for
      - a list of 4-tuples of the form: [t0, t1, r0, r1]
          - t0: time, begin of resource consumption
          - t1: time, begin of resource consumption
          - r0: int,  index of resources consumed (min)
          - r1: int,  index of resources consumed (max)
        The tuples are formed so that t0 to t1 and r0 to r1 are continuous.

    An entity can consume different resources under different metrics - but the
    returned consumption specs will never overlap, meaning, that any resource is
    accounted for exactly one metric at any point in time.  The returned
    structure has the following overall form:

        {
          'metric_1' : {
              uid_1 : [[t0, t1, r0, r1],
                       [t2, t3, r2, r3],
                       ...
                      ],
              uid_2 : ...
          },
          'metric_2' : ...
        }
    '''

    log = ru.Logger('radical.pilot.utils')

    consumed = dict()
    for e in session.get(etype=['pilot', 'unit']):

        if   e.etype == 'pilot': data = _get_pilot_consumption(session, e)
        elif e.etype == 'unit' : data = _get_unit_consumption(session,  e)

        for metric in data:

            if metric not in consumed:
                consumed[metric] = dict()

            for uid in data[metric]:
                consumed[metric][uid] = data[metric][uid]


    # we defined two additional metricsL 'warmup' and 'drain', which are defined
    # for all resources of the pilot.  `warmup` is defined as the time from
    # when the pilot becomes active, to the time the resource is first consumed
    # by a unit.  `drain` is the inverse: the  time from when any unit last
    # consumed the resource to the time when the pilot terminates.
    for pilot in session.get(etype='pilot'):

        if pilot.cfg['task_launch_method'] == 'PRTE':
          # print('\nusing prte configuration')
            unit_durations = UNIT_DURATIONS_PRTE
        else:
          # print('\nusing default configuration')
            unit_durations = UNIT_DURATIONS_DEFAULT

        pt    = pilot.timestamps
        log.debug('timestamps:')
        for ts in pt():
            log.debug('    %10.2f  %-20s  %-15s  %-15s  %-15s  %-15s  %s',
                           ts[0],  ts[1], ts[2], ts[3], ts[4], ts[5], ts[6])

        p_min = pt(event=PILOT_DURATIONS['consume']['ignore'][0]) [0]
        p_max = pt(event=PILOT_DURATIONS['consume']['ignore'][1])[-1]
      # p_max = pilot.events[-1][ru.TIME]
        log.debug('pmin, pmax: %10.2f / %10.2f', p_min, p_max)

        pid = pilot.uid
        cpn = pilot.cfg['resource_details']['rm_info']['cores_per_node']
        gpn = pilot.cfg['resource_details']['rm_info']['gpus_per_node']

        nodes, anodes, pnodes = _get_nodes(pilot)

        # find resource utilization scope for all resources. We begin filling
        # the resource dict with
        #
        #   resource_id : [t_min=None, t_max=None]
        #
        # and then iterate over all units.  Wen we find a unit using some
        # resource id, we set or adjust t_min / t_max.
        resources = dict()
        for pnode in pnodes:
            idx = get_node_index(nodes, pnode, cpn, gpn)
            for c in range(idx[0], idx[1] + 1):
                resources[c] = [None, None]


        for unit in session.get(etype='unit'):

            if unit.cfg.get('pilot') != pid:
                continue

            try:
                snodes = unit.cfg['slots']['nodes']
                ut     = unit.timestamps
                u_min  = ut(event=unit_durations['consume']['exec_queue'][0]) [0]
                u_max  = ut(event=unit_durations['consume']['unschedule'][1])[-1]
            except:
                continue

            for snode in snodes:

                node  = [snode['name'], snode['uid']]
                r0, _ = get_node_index(nodes, node, cpn, gpn)

                for core_map in snode['core_map']:
                    for core in core_map:
                        idx = r0 + core
                        t_min = resources[idx][0]
                        t_max = resources[idx][1]
                        if t_min is None or t_min > u_min: t_min = u_min
                        if t_max is None or t_max < u_max: t_max = u_max
                        resources[idx] = [t_min, t_max]

                for gpu_map in snode['gpu_map']:
                    for gpu in gpu_map:
                        idx = r0 + cpn + gpu
                        t_min = resources[idx][0]
                        t_max = resources[idx][1]
                        if t_min is None or t_min > u_min: t_min = u_min
                        if t_max is None or t_max < u_max: t_max = u_max
                        resources[idx] = [t_min, t_max]

        # now sift through resources and find buckets of pairs with same t_min
        # or same t_max
        bucket_min  = dict()
        bucket_max  = dict()
        bucket_none = list()
        for idx in resources:

            t_min = resources[idx][0]
            t_max = resources[idx][1]

            if t_min is None:

                assert(t_max is None)
                bucket_none.append(idx)

            else:

                if t_min not in bucket_min:
                    bucket_min[t_min] = list()
                bucket_min[t_min].append(idx)

                if t_max not in bucket_max:
                    bucket_max[t_max] = list()
                bucket_max[t_max].append(idx)

        boxes_warm  = list()
        boxes_drain = list()
        boxes_idle  = list()

        # now cluster all lists and add the respective boxes
        for t_min in bucket_min:
            for r in cluster_resources(bucket_min[t_min]):
                boxes_warm.append([p_min, t_min, r[0], r[1]])

        for t_max in bucket_max:
            for r in cluster_resources(bucket_max[t_max]):
                boxes_drain.append([t_max, p_max, r[0], r[1]])

        for r in cluster_resources(bucket_none):
            boxes_idle.append([p_min, p_max, r[0], r[1]])

        if 'warm'  not in consumed: consumed['warm']  = dict()
        if 'drain' not in consumed: consumed['drain'] = dict()
        if 'idle'  not in consumed: consumed['idle']  = dict()

        consumed['warm'][pid]  = boxes_warm
        consumed['drain'][pid] = boxes_drain
        consumed['idle'][pid]  = boxes_idle

  # pprint.pprint(consumed)

    return consumed


# ------------------------------------------------------------------------------
#
def _get_nodes(pilot):

    pnodes = pilot.cfg['resource_details']['rm_info']['node_list']
    agents = pilot.cfg['resource_details']['rm_info'].get('agent_nodes')
    anodes = list()
    nodes  = list()

    for agent in agents:
        anodes.append(agents[agent])

    nodes = pnodes + anodes

    return nodes, anodes, pnodes


# ------------------------------------------------------------------------------
#
def _get_pilot_consumption(session, pilot):

    # Pilots consume resources in different ways:
    #
    #   - the pilot needs to bootstrap and initialize before becoming active,
    #     i.e., before it can begin to manage the workload, and needs to
    #     terminate and clean up during shutdown;
    #   - the pilot may block one or more nodes or cores for it's own components
    #     (sub-agents), and those components are not available for workload
    #     execution
    #   - the pilot may perform operations while managing the workload.
    #
    # This method will compute the first two contributions and part of the 3rd.
    # It will *not* account for those parts of the 3rd which are performed while
    # specfic resources are blocked for the affected workload element (task)
    # - those resource consumption is considered to be a consumption *of that
    # task*, which allows us to compute tasks specific resource utilization
    # overheads.

    pid   = pilot.uid
    cpn   = pilot.cfg['resource_details']['rm_info']['cores_per_node']
    gpn   = pilot.cfg['resource_details']['rm_info']['gpus_per_node']
    ret   = dict()

    # Account for agent resources.  Agents use full nodes, i.e., cores and GPUs
    # We happen to know that agents use the first nodes in the allocation and
    # their resource tuples thus start at index `0`, but for completeness we
    # ensure that by inspecting the pilot cfg.

    # Duration is for all of the pilot runtime. This is not precises really,
    # since several bootstrapping phases happen before the agents exist - but we
    # consider the nodes blocked for the sub-agents from the get-go.
    t0, t1 = get_duration(pilot, PILOT_DURATIONS['agent']['total'])
    boxes  = list()

    # Substract agent nodes from the nodelist, so that we correctly attribute
    # other global pilot metrics to the remaining nodes.
    nodes, anodes, pnodes = _get_nodes(pilot)

    if anodes and t0 is not None:
        for anode in anodes:
            r0, r1 = get_node_index(nodes, anode, cpn, gpn)
            boxes.append([t0, t1, r0, r1])

    ret['agent'] = {pid: boxes}

    # account for all other pilot metrics
    for metric in PILOT_DURATIONS['consume']:

        if metric == 'ignore':
            continue

        boxes = list()
        t0, t1 = get_duration(pilot, PILOT_DURATIONS['consume'][metric])

        if t0 is not None:
            for node in pnodes:
                r0, r1 = get_node_index(nodes, node, cpn, gpn)
                boxes.append([t0, t1, r0, r1])

        ret[metric] = {pid: boxes}

    return ret


# ------------------------------------------------------------------------------
#
def _get_unit_consumption(session, unit):

    # we need to know what pilot the unit ran on.  If we don't find a designated
    # pilot, no resources were consumed
    uid = unit.uid
    pid = unit.cfg['pilot']

    if not pid:
        return dict()

    # get the pilot for inspection
    pilot = session.get(uid=pid)

    if isinstance(pilot, list):
        assert(len(pilot) == 1)
        pilot = pilot[0]

    # FIXME: it is inefficient to query those values again and again
    cpn   = pilot.cfg['resource_details']['rm_info']['cores_per_node']
    gpn   = pilot.cfg['resource_details']['rm_info']['gpus_per_node']
    nodes, anodes, pnodes = _get_nodes(pilot)

    # Units consume only those resources they are scheduled on.
    if 'slots' not in unit.cfg:
        return dict()

    snodes    = unit.cfg['slots']['nodes']
    resources = list()
    for snode in snodes:

        node  = [snode['name'], snode['uid']]
        r0, _ = get_node_index(nodes, node, cpn, gpn)

        for core_map in snode['core_map']:
            for core in core_map:
                resources.append(r0 + core)

        for gpu_map in snode['gpu_map']:
            for gpu in gpu_map:
                resources.append(r0 + cpn + gpu)

    # find continuous stretched of resources to minimize number of boxes
    resources = cluster_resources(resources)

    # we heuristically switch between PRTE event traces and normal (fork) event
    # traces
    if pilot.cfg['task_launch_method'] == 'PRTE':
        unit_durations = UNIT_DURATIONS_PRTE
    else:
        unit_durations = UNIT_DURATIONS_DEFAULT

    if _debug:
        print()

    ret = dict()
    for metric in unit_durations['consume']:

        boxes = list()
        t0, t1 = get_duration(unit, unit_durations['consume'][metric])


        if t0 is not None:

            if _debug:
                print('%s: %-15s : %10.3f - %10.3f = %10.3f'
                     % (unit.uid, metric, t1, t0, t1 - t0))
            for r in resources:
                boxes.append([t0, t1, r[0], r[1]])

        else:
            if _debug:
                print('%s: %-15s : -------------- ' % (unit.uid, metric))
                dur = unit_durations['consume'][metric]
                print(dur)

                for e in dur:
                    if ru.STATE in e and ru.EVENT not in e:
                        e[ru.EVENT] = 'state'

                t0 = unit.timestamps(event=dur[0])
                t1 = unit.timestamps(event=dur[1])
                print(t0)
                print(t1)
                for e in unit.events:
                    print('\t'.join([str(x) for x in e]))

              # sys.exit()

        ret[metric] = {uid: boxes}

    return ret


# ------------------------------------------------------------------------------

