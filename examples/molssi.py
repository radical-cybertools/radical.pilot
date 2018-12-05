#!/usr/bin/env python

import radical.pilot as rp
import radical.utils as ru

import os
import sys
import time
import json
import pprint
import requests


from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

address = "https://localhost:7777/"


# ------------------------------------------------------------------------------
#
def get_task(n):

    # Pull data
    payload = {
        "meta": {
            "name" : "testing_manager",
            "tag"  : None,
            "limit": n
        },
        "data": {}
    }
    r    = requests.get(address + "queue_manager", json=payload, verify=False)
    task = r.json()
    
  # pprint.pprint(tasks)
    return task


# ------------------------------------------------------------------------------
#
def push_tasks(bulk_id, unit, sandbox):

    data_ok  = dict()
    data_nok = list()

    for unit in units:

        if unit.state == rp.DONE:
            fout   = unit.metadata['fout']
            result = ru.read_json(fout)
            data_ok[unit.name] = (result, u'single', [])

        else:
            data_nok.apppend(unit.name)


    if data_ok:
        payload = {"meta": {"name": bulk_id},
                   "data": data_ok}
        r = requests.post(address + "queue_manager", json=payload, verify=False)
        print '%s ok: %s' % (bulk_id, r.json())


    if data_nok:
        payload = {"meta": {"name"     : bulk_id,
                            "operation": 'shutdown'}, 
                   "data" : data_nok}
        r = requests.put(address + "queue_manager", json=payload, verify=False)
        print '%s nok: %s' % (bulk_id, r)


# ------------------------------------------------------------------------------
#
report = ru.Reporter(name='radical.pilot')
resource = 'local.localhost'
session = rp.Session()
try:

    sandbox = '%s/data' % os.getcwd()
    rp.utils.rec_makedir(sandbox)

    report.header('submit pilots')
    pmgr = rp.PilotManager(session=session)
    pd_init = {'resource'      : resource,
               'runtime'       : 60,  # pilot runtime (min)
               'exit_on_error' : True,
               'cores'         : 128
              }
    pdesc = rp.ComputePilotDescription(pd_init)
    pilot = pmgr.submit_pilots(pdesc)

    report.header('submit units')

    # Register the ComputePilot in a UnitManager object.
    umgr = rp.UnitManager(session=session)
    umgr.add_pilots(pilot)

    # Create a workload of ComputeUnits.
    # Each compute unit runs '/bin/date'.

    for bulk in range(5):

        bulk_size = 500
        bulk_id   = '%s.%04d' % (session.uid, bulk)

        report.info('handle bulk %s (%d)\n\t' % (bulk_id, bulk_size))
        cuds  = list()
        tasks = get_task(bulk_size)
        for task in tasks['data']:

          # pprint.pprint(task)

            args  = task['spec']['args'][0]
            prog  = args['program']
            tid   = task['id']
            fin   = '%s/%s.in.json'  % (sandbox, tid)
            fout  = '%s/%s.out.json' % (sandbox, tid)

            ru.write_json(args, fin)

            cud = rp.ComputeUnitDescription()
            cud.executable       = '/home/dgasmith/miniconda/envs/qcf/bin/qcengine'
            cud.arguments        = [prog, fin]
            cud.name             = tid
            cud.metadata         = {'fout' : fout}
            cud.input_staging    = [fin]
            cud.output_staging   = {'source': 'unit:///STDOUT', 
                                    'target': '%s' % fout,
                                    'action': rp.TRANSFER}
            cud.gpu_processes    = 0
            cud.cpu_processes    = 1
            cud.cpu_threads      = 1
            cud.cpu_process_type = rp.POSIX
            cud.cpu_thread_type  = rp.POSIX

            cuds.append(cud)
            report.progress()

        report.ok('>>ok\n')
        units = umgr.submit_units(cuds)
        report.header('gather results')
      # umgr.wait_units(uids=[u.uid for u in units])
        umgr.wait_units()

        push_tasks(bulk_id, units, sandbox)



except Exception as e:
    report.error('caught Exception: %s\n' % e)
    raise

except (KeyboardInterrupt, SystemExit) as e:
    report.warn('exit requested\n')

finally:
    report.header('finalize')
    session.close(download=True)

report.header()


# ----------------------------------------------------------------------------

