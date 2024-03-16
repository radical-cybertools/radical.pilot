#!/usr/bin/env python3

'''
Demonstrate the "raptor" features for remote Task management.

This script and its supporting files may use relative file paths. Run from the
directory in which you found it.

Refer to the ``raptor.cfg`` file in the same directory for configurable run time
details.

By default, this example uses the ``local.localhost`` resource with the
``local`` access scheme where RP oversubscribes resources to emulate multiple
nodes.

In this example, we
  - Launch one or more raptor "master" task(s), which self-submits additional
    tasks (results are logged in the master's `result_cb` callback).
  - Stage scripts to be used by a raptor "Worker"
  - Provision a Python virtual environment with
    :py:func:`~radical.pilot.prepare_env`
  - Submit several tasks that will be routed through the master(s) to the
    worker(s).
  - Submit a non-raptor task in the same Pilot environment

'''

import os
import sys
import functools

import radical.utils as ru
import radical.pilot as rp


# To enable logging, some environment variables need to be set.
# Ref
# * https://radicalpilot.readthedocs.io/en/stable/overview.html#what-about-logging
# * https://radicalpilot.readthedocs.io/en/stable/developer.html#debugging
# For terminal output, set RADICAL_LOG_TGT=stderr or RADICAL_LOG_TGT=stdout
logger = ru.Logger('radical.raptor')
PWD    = os.path.abspath(os.path.dirname(__file__))
RANKS  = 2

# ------------------------------------------------------------------------------
#
@rp.pythontask
def func_mpi(comm, msg, sleep):
    import time
    print('hello %d/%d: %s' % (comm.rank, comm.size, msg))
    time.sleep(sleep)
    return 'func_mpi retval'


# ------------------------------------------------------------------------------
#
@rp.pythontask
def func_non_mpi(a, sleep):
    import math
    import random
    import time
    b = random.random()
    t = math.exp(a * b)
    print('func_non_mpi: %.1f' % t)
    time.sleep(sleep)
    return 'func_non_mpi retval'


# ------------------------------------------------------------------------------
#
def task_state_cb(task, state, session):

    logger.info('task %s: %s', task.uid, state)

    if state == rp.FAILED:
        logger.error('task %s failed', task.uid)

    if 'master' in task.uid:

        logger.info('master state update')
        if state in rp.FINAL:
            session.close()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if len(sys.argv) < 2:
        cfg_file = '%s/raptor.cfg' % PWD

    else:
        cfg_file = sys.argv[1]

    cfg              = ru.Config(cfg=ru.read_json(cfg_file))
    sleep            = int(cfg.sleep)
    cores_per_node   = cfg.cores_per_node
    gpus_per_node    = cfg.gpus_per_node
    n_masters        = cfg.n_masters
    n_workers        = cfg.n_workers
    masters_per_node = cfg.masters_per_node
    nodes_per_worker = cfg.nodes_per_worker
    nodes_rp         = cfg.nodes_rp
    tasks_raptor     = cfg.tasks_raptor
    tasks_rp         = cfg.tasks_rp
    nodes_agent      = cfg.nodes_agent

    cores_per_task   = cfg.cores_per_task

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Raptor example (RP version %s)' % rp.version)

    session = rp.Session()
    try:
        pd = rp.PilotDescription(cfg.pilot_descr)
        cores_per_master = cores_per_node // masters_per_node
        pd.cores   = n_masters * cores_per_master
        pd.gpus    = 0

        pd.cores  += n_masters * n_workers * cores_per_node * nodes_per_worker
        pd.gpus   += n_masters * n_workers * gpus_per_node * nodes_per_worker

        pd.cores  += nodes_agent * cores_per_node
        pd.gpus   += nodes_agent * gpus_per_node

        pd.cores  += nodes_rp * cores_per_node
        pd.gpus   += nodes_rp * gpus_per_node

        pmgr = rp.PilotManager(session=session)
        tmgr = rp.TaskManager(session=session)


        partial_cb = functools.partial(task_state_cb, session=session)
        tmgr.register_callback(partial_cb)

        pilot = pmgr.submit_pilots(pd)
        tmgr.add_pilots(pilot)

        pmgr.wait_pilots(uids=pilot.uid, state=[rp.PMGR_ACTIVE])

        report.info('Stage files for the worker `my_hello` command.\n')
        # See raptor_worker.py.
        pilot.stage_in({'source': ru.which('radical-pilot-hello.sh'),
                        'target': 'radical-pilot-hello.sh',
                        'action': rp.TRANSFER})

        report.info('done\n')

        # Launch a raptor master task, which will launch workers and self-submit
        # some additional tasks for illustration purposes.

        master_ids = [ru.generate_id('master.%(item_counter)06d',
                                     ru.ID_CUSTOM, ns=session.uid)
                      for _ in range(n_masters)]

        tds = list()
        for i in range(n_masters):
            td = rp.TaskDescription(cfg.master_descr)
            td.mode           = rp.RAPTOR_MASTER
            td.uid            = master_ids[i]
            td.arguments      = [cfg_file, i]
            td.cpu_processes  = 1
            td.cpu_threads    = cores_per_master
            td.input_staging  = [{'source': '%s/raptor_master.py' % PWD,
                                  'target': 'raptor_master.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': '%s/raptor_worker.py' % PWD,
                                  'target': 'raptor_worker.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': cfg_file,
                                  'target': os.path.basename(cfg_file),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS}
                                ]
            tds.append(td)

        if len(tds) > 0:
            report.info('Submit raptor master(s) %s\n'
                       % str([t.uid for t in tds]))
            task  = tmgr.submit_tasks(tds)
            if not isinstance(task, list):
                task = [task]

            states = tmgr.wait_tasks(
                uids=[t.uid for t in task],
                state=rp.FINAL + [rp.AGENT_EXECUTING],
                timeout=300
            )
            logger.info('Master states: %s', str(states))

        # submit some non-raptor tasks which will execute independently of the
        # raptor masters and workers. (ensure `cfg.nodes_rp > 0`)
        tds = list()
        for i in range(tasks_rp):
            tds.append(rp.TaskDescription({
                'uid'             : 'task.exe.c.%06d' % i,
                'mode'            : rp.TASK_EXECUTABLE,
                'raptor_id'       : None,
                'ranks'           : RANKS,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c',
                              'sleep %d;' % sleep +
                              'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"']}))
        report.info('Submit non-raptor task(s) %s \n'
                   % str([t.uid for t in tds]))

        # submit some tasks that will be routed through the raptor master and
        # which will execute in the named virtual environment we provisioned.
        report.info('Prepare raptor tasks.\n')
        for i in range(tasks_raptor):

            tds.append(rp.TaskDescription({
                'uid'             : 'task.call.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNC,
                'ranks'           : 1,
                'function'        : 'hello',
                'kwargs'          : {'msg': 'task.call.c.1.%06d' % i},
                'raptor_id'       : master_ids[i % n_masters]}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.call_mpi.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNC,
                'ranks'           : RANKS,
                'function'        : 'hello_mpi',
                'kwargs'          : {'msg': 'task.call.c.2.%06d' % i},
                'raptor_id'       : master_ids[i % n_masters]}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.call.c.3.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNC,
                'function'        : 'my_hello',
                'kwargs'          : {'uid': 'task.call.c.3.%06d' % i},
                'raptor_id'       : master_ids[i % n_masters]}))

            bson = func_mpi(None, msg='task.call.c.%06d' % i, sleep=sleep)
            tds.append(rp.TaskDescription({
                'uid'             : 'task.mpi_ser_func.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNC,
                'ranks'           : RANKS,
                'function'        : bson,
                'raptor_id'       : master_ids[i % n_masters]}))

            bson = func_non_mpi(i, sleep=sleep)
            tds.append(rp.TaskDescription({
                'uid'             : 'task.ser_func.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNC,
                'ranks'           : 1,
                'function'        : bson,
                'raptor_id'       : master_ids[i % n_masters]}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.eval.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_EVAL,
                'ranks'           : RANKS,
                'code'            :
                    'print("hello %%s/%%s: %%s" %% (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"])) and '
                    'time.sleep(%d)' % sleep,
                'raptor_id'       : master_ids[i % n_masters]}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.exec.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_EXEC,
                'ranks'           : RANKS,
                'code'            :
                    'import time\ntime.sleep(%d)\n' % sleep +
                    'import os\nprint("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'raptor_id'       : master_ids[i % n_masters]}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.proc.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_PROC,
                'ranks'           : RANKS,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c',
                                     'sleep %d; ' % sleep +
                                     'echo "hello $RP_RANK/$RP_RANKS: '
                                           '$RP_TASK_ID"'],
                'raptor_id'       : master_ids[i % n_masters]}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.shell.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_SHELL,
                'ranks'           : RANKS,
                'command'         : 'sleep %d; ' % sleep +
                                    'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"',
                'raptor_id'       : master_ids[i % n_masters]}))

        if len(tds) > 0:
            report.info('Submit tasks %s.\n' % str([t.uid for t in tds]))
            tasks = tmgr.submit_tasks(tds)

            logger.info('Wait for tasks %s', [t.uid for t in tds])
            tmgr.wait_tasks(uids=[t.uid for t in tasks], timeout=900)

            for task in tasks:
                report.info('id: %s [%s]:\n    out: %s\n    ret: %s\n'
                      % (task.uid, task.state, task.stdout, task.return_value))

    finally:
        session.close(download=True)

    report.info('Logs from the master task should now be in local files \n')
    report.info('like %s/%s/%s.log\n' % (session.uid, pilot.uid, master_ids[0]))

# ------------------------------------------------------------------------------
