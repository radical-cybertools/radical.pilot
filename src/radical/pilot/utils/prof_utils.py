
import os

# "label", "component", "event", "message"
prof_entries = [
    ('a_get_u',         'MainThread',       'get', 'MongoDB to Agent (PendingExecution)'),
    ('a_build_u',       'MainThread',       'Agent get unit meta', ''),
    ('a_mkdir_u',       'MainThread',       'Agent get unit mkdir', ''),
    ('a_notify_alloc',  'MainThread',       'put', 'Agent to update_queue (Allocating)'),
    ('a_to_s',          'MainThread',       'put', 'Agent to schedule_queue (Allocating)'),

    ('s_get_alloc',     'CONTINUOUS',       'get', 'schedule_queue to Scheduler (Allocating)'),
    ('s_alloc_failed',  'CONTINUOUS',       'schedule', 'allocation failed'),
    ('s_allocated',     'CONTINUOUS',       'schedule', 'allocated'),
    ('s_to_ewo',        'CONTINUOUS',       'put', 'Scheduler to execution_queue (Allocating)'),
    ('s_unqueue',       'CONTINUOUS',       'unqueue', 're-allocation done'),
  
    ('ewo_get',         'ExecWorker-',      'get', 'executing_queue to ExecutionWorker (Executing)'),
    ('ewo_launch',      'ExecWorker-',      'ExecWorker unit launch', ''),
    ('ewo_spawn',       'ExecWorker-',      'ExecWorker spawn', ''),
    ('ewo_script',      'ExecWorker-',      'launch script constructed', ''),
    ('ewo_pty',         'ExecWorker-',      'spawning passed to pty', ''),  
    ('ewo_notify_exec', 'ExecWorker-',      'put', 'ExecWorker to update_queue (Executing)'),
    ('ewo_to_ewa',      'ExecWorker-',      'put', 'ExecWorker to watcher (Executing)'),
  
    ('ewa_get',         'ExecWatcher-',     'get', 'ExecWatcher picked up unit'),
    ('ewa_complete',    'ExecWatcher-',     'execution complete', ''),
    ('ewa_notify_so',   'ExecWatcher-',     'put', 'ExecWatcher to update_queue (StagingOutput)'),
    ('ewa_to_sow',      'ExecWatcher-',     'put', 'ExecWatcher to stageout_queue (StagingOutput)'),
  
    ('sow_get_u',       'StageoutWorker-',  'get', 'stageout_queue to StageoutWorker (StagingOutput)'),
    ('sow_u_done',      'StageoutWorker-',  'final', 'stageout done'),
    ('sow_notify_done', 'StageoutWorker-',  'put', 'StageoutWorker to update_queue (Done)'),

    ('uw_get_alloc',    'UpdateWorker-',    'get', 'update_queue to UpdateWorker (Allocating)'),   
    ('uw_push_alloc',   'UpdateWorker-',    'unit update pushed (Allocating)', ''),
    ('uw_get_exec',     'UpdateWorker-',    'get', 'update_queue to UpdateWorker (Executing)'),
    ('uw_push_exec',    'UpdateWorker-',    'unit update pushed (Executing)', ''),
    ('uw_get_so',       'UpdateWorker-',    'get', 'update_queue to UpdateWorker (StagingOutput)'),
    ('uw_push_so',      'UpdateWorker-',    'unit update pushed (StagingOutput)', ''),
    ('uw_get_done',     'UpdateWorker-',    'get', 'update_queue to UpdateWorker (Done)'),
    ('uw_push_done',    'UpdateWorker-',    'unit update pushed (Done)', '')
]

# ------------------------------------------------------------------------------
#
# Lookup tuples in dataframe based on uid and the tuple from the elements list
#
def _tup2ts(df, uid, tup):

    import numpy as np
    
    all_for_uid = df[df.uid == uid].fillna('')
    val = all_for_uid[(all_for_uid.component.str.startswith(tup[1])) &
                      (all_for_uid.event == tup[2]) &
                      (all_for_uid.message == tup[3])].time
    try:
        return val.iloc[0]
    except Exception as e:
        return np.NaN


# ------------------------------------------------------------------------------
#
# Construct a unit based dataframe from a raw dataframe
#
def _prof2df(rawdf, units): 
    
    import pandas as pd

    print units.keys()
    
    indices = [unit for unit in units['all']] 
    print "--"
    print len(indices)
    info    = [{t[0]:_tup2ts(rawdf, unit, t) for t in prof_entries} for unit in units['all']]
    print info
    print "--"
    
    # TODO: Also do this for cloned units

    return pd.DataFrame(info) # , index=indices[exp]).sort_index()


# ------------------------------------------------------------------------------
#
# Add additional (derived) colums to dataframes
# create columns based on two other columns using an operator
# 
def add_derived(df):
    
    import operator
    
    df['executor_queue'] = operator.sub(df['ewo_get'],      df['s_to_ewo'])
    df['raw_runtime']    = operator.sub(df['ewa_complete'], df['ewo_launch'])
    df['full_runtime']   = operator.sub(df['uw_push_done'], df['s_to_ewo'])
    df['watch_delay']    = operator.sub(df['ewa_get'],      df['ewo_to_ewa'])
    df['allocation']     = operator.sub(df['s_allocated'],  df['a_to_s'])



# ------------------------------------------------------------------------------
#
def _prof2uids(rawdf):

    units = {}
  
    # Using "native" Python    
    units['all']    = [x for x in rawdf.uid.dropna().unique() if x.startswith('unit')]
    units['cloned'] = [x for x in units['all']                if 'clone' in x]
    units['real']   = list(set(units['all']) - set(units['cloned']))
  
    return units

# ------------------------------------------------------------------------------
#
# Get raw CSV datasets as DataFrames based on selection filter
#
def get_prof_frames(sid, pids, profdir):

    import pandas as pd

    frames = dict()

    # Get multiple pilots from session
    for pid in pids:

        prof_file = os.path.join(profdir, sid + '-' + pid + '.prof')
        prof_data = pd.read_csv(prof_file)

        print "----"
        print prof_file
        print len(prof_file)
        print prof_data
        return []

        units = _prof2uids(prof_data)
        print len(units['all']),
        print len(units['real']),
        print len(units['cloned'])
        df    = _prof2df(prof_data, units)

        print "----"
        frames[pid] = df

    return frames


# ------------------------------------------------------------------------------

