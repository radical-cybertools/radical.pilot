#!/usr/bin/env python

import radical.pilot as rp
import radical.utils as ru

import os
import requests


from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

address = "https://localhost:7777/"


# ------------------------------------------------------------------------------
#
def fetch_tasks(bulk_size):
    '''
    Fetch a bulk of tasks from the QCArchive service endpoint, convert them from
    json, and return them as a list.
    '''

    payload = {
        "meta": {
            "name" : "testing_manager",   # TODO: what is this used for?
            "tag"  : None,                # TODO: what is this used for?
            "limit": bulk_size
        },
        "data": {}
    }

    # TODO: auth
    data  = requests.get(address + "queue_manager", json=payload, verify=False)
    tasks = data.json()

    return tasks


# ------------------------------------------------------------------------------
#
def push_tasks(bulk_id, unit):
    '''
    Once a bulk of tasks has been executed, push the resulting jsons back to the
    QCArchive service endpoint.  The results are read from
    the unit's `stdout` file, which the executor needs to fetch back to
    localhost.

    Units which failed are marked and returned in a separate bulk, using the
    `shutdown` operation.
    '''

    data_ok  = dict()
    data_nok = list()

    for unit in units:

        if unit.state == rp.DONE:
            fout   = unit.metadata['fout']   # FIXME: implies data staging
            result = ru.read_json(fout)
            data_ok[unit.name] = (result, 'single', [])

        else:
            data_nok.apppend(unit.name)


    if data_ok:
        payload = {"meta": {"name": bulk_id},
                   "data": data_ok}
        r = requests.post(address + "queue_manager", json=payload, verify=False)
        print('%s ok: %s' % (bulk_id, r.json()))


    if data_nok:
        payload = {"meta": {"name"     : bulk_id,
                            "operation": 'shutdown'},
                   "data" : data_nok}
        r = requests.put(address + "queue_manager", json=payload, verify=False)
        print('%s nok: %s' % (bulk_id, r))


# ------------------------------------------------------------------------------
#
# create an RP session, start a pilot, and then pull bulks of tasks for
# execution.  Tasks are pushed back once a bulk has been completed.
#
# In this simple example, we use a sandbox dir for storing stdin and stdout
# files - in production we would need to use proper data staging.
#
report   = ru.Reporter(name='radical.pilot')
resource = 'local.localhost'
session  = rp.Session()
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

    umgr = rp.UnitManager(session=session)
    umgr.add_pilots(pilot)

    for bulk in range(5):

        bulk_size = 500
        bulk_id   = '%s.%04d' % (session.uid, bulk)

        report.info('handle bulk %s (%d)\n\t' % (bulk_id, bulk_size))
        cuds  = list()
        tasks = fetch_tasks(bulk_size=bulk_size)
        for task in tasks['data']:

            args  = task['spec']['args'][0]
            prog  = args['program']
            tid   = task['id']
            fin   = '%s/%s.in.json'  % (sandbox, tid)
            fout  = '%s/%s.out.json' % (sandbox, tid)

            ru.write_json(args, fin)

            cud = rp.ComputeUnitDescription()
            cud.executable    = '/home/dgasmith/miniconda/envs/qcf/bin/qcengine'
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
        umgr.wait_units()  # FIXME: this will glow slow over time.

        push_tasks(bulk_id, units)


except Exception as e:
    report.error('caught Exception: %s\n' % e)
    raise

except (KeyboardInterrupt, SystemExit):
    report.warn('exit requested\n')

finally:
    report.header('finalize')
    session.close(download=True)

report.header()


# ----------------------------------------------------------------------------

