#!/usr/bin/env python3

import os
import sys

import radical.utils as ru
import radical.pilot as rp

from radical.pilot import PythonTask

pytask = PythonTask.pythontask


# ------------------------------------------------------------------------------
#
@pytask
def func_mpi(comm, msg, sleep=0):
    import time
    print('hello %d/%d: %s' % (comm.rank, comm.size, msg))
    time.sleep(sleep)
  # raise RuntimeError('oops 3')


# ------------------------------------------------------------------------------
#
@pytask
def func_non_mpi(a):
    import math
    import random
    b = random.random()
    t = math.exp(a * b)
    print('func_non_mpi')
    return t


# ------------------------------------------------------------------------------
#
def task_state_cb(task, state):
    print('task %s: %s' % (task['uid'], state))
    if state == rp.FAILED:
        print('task %s failed' % task['uid'])
        sys.exit()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if len(sys.argv) < 2:
        cfg_file = './raptor.cfg'
    else:
        cfg_file = sys.argv[1]

    cfg         = ru.Config(cfg=ru.read_json(cfg_file))
    sleep       = int(cfg.sleep)
    cpn         = cfg.cpn
    gpn         = cfg.gpn
    n_masters   = cfg.n_masters
    n_workers   = cfg.n_workers
    masters_pn  = cfg.masters_pn
    nodes_pw    = cfg.nodes_pw
    nodes_rp    = cfg.nodes_rp
    workload    = cfg.workload
    tasks_rp    = cfg.tasks_rp
    nodes_agent = cfg.nodes_agent

    # each master uses a node, and each worker on each master uses a node
    # use 8 additional cores for non-raptor tasks
    session   = rp.Session()
    try:
        pd = rp.PilotDescription(cfg.pilot_descr)
        pd.cores   = n_masters * (cpn / masters_pn)
        pd.gpus    = 0

        pd.cores  += n_masters * n_workers * cpn * nodes_pw
        pd.gpus   += n_masters * n_workers * gpn * nodes_pw

        pd.cores  += nodes_agent * cpn
        pd.gpus   += nodes_agent * gpn

        pd.cores  += nodes_rp * cpn
        pd.gpus   += nodes_rp * gpn

        pd.runtime = cfg.runtime


        tds = list()

        for i in range(n_masters):
            td = rp.TaskDescription(cfg.master_descr)
            td.uid            = ru.generate_id('master.%(item_counter)06d',
                                               ru.ID_CUSTOM,
                                               ns=session.uid)
            td.arguments      = [cfg_file, i]
            td.cpu_threads    = int(cpn / masters_pn)
            td.input_staging  = [{'source': 'raptor_master.py',
                                  'target': 'raptor_master.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': 'raptor_worker.py',
                                  'target': 'raptor_worker.py',
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS},
                                 {'source': cfg_file,
                                  'target': os.path.basename(cfg_file),
                                  'action': rp.TRANSFER,
                                  'flags' : rp.DEFAULT_FLAGS}
                                ]
            tds.append(td)

        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)
        pilot = pmgr.submit_pilots(pd)
        task  = tmgr.submit_tasks(tds)

      # pmgr.wait_pilots(uid=pilot.uid, state=[rp.PMGR_ACTIVE])
        pilot.stage_in({'source': ru.which('radical-pilot-hello.sh'),
                        'target': 'radical-pilot-hello.sh',
                        'action': rp.TRANSFER})
        pilot.prepare_env(env_name='ve_raptor',
                          env_spec={'type'   : 'venv',
                                    'path'   : '/tmp/ve_raptor/',
                                    'setup'  : ['$HOME/j/rp/',
                                                '$HOME/j/ru/',
                                                'mpi4py']})

        # submit some test tasks
        tds = list()
        for i in range(tasks_rp):

            tds.append(rp.TaskDescription({
                'uid'             : 'task.exe.c.%06d' % i,
                'mode'            : rp.TASK_EXECUTABLE,
                'scheduler'       : None,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c',
                                     "echo 'hello $RP_RANK/$RP_RANKS: $RP_TASK_ID'"]}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.call.c.1.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'function'        : 'hello',
                'kwargs'          : {'msg': 'task.call.c.1.%06d' % i},
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.call.c.2.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'function'        : 'hello_mpi',
                'kwargs'          : {'msg': 'task.call.c.2.%06d' % i},
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.call.c.3.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'function'        : 'my_hello',
                'kwargs'          : {'uid': 'task.call.c.3.%06d' % i},
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            bson = func_mpi(None, msg='task.call.c.%06d' % i, sleep=0)
            tds.append(rp.TaskDescription({
                'uid'             : 'task.mpi_ser_func.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'function'        : bson,
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            bson = func_non_mpi(i)
            tds.append(rp.TaskDescription({
                'uid'             : 'task.ser_func.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_FUNCTION,
                'cpu_processes'   : 2,
                'function'        : bson,
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.eval.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_EVAL,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'code'            :
                    'print("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.exec.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_EXEC,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'code'            :
                    'import os\nprint("hello %s/%s: %s" % (os.environ["RP_RANK"],'
                    'os.environ["RP_RANKS"], os.environ["RP_TASK_ID"]))',
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.proc.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_PROC,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'executable'      : '/bin/sh',
                'arguments'       : ['-c', 'echo "hello $RP_RANK/$RP_RANKS: '
                                           '$RP_TASK_ID"'],
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

            tds.append(rp.TaskDescription({
                'uid'             : 'task.shell.c.%06d' % i,
              # 'timeout'         : 10,
                'mode'            : rp.TASK_SHELL,
                'cpu_processes'   : 2,
                'cpu_process_type': rp.MPI,
                'command'         : 'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"',
                'scheduler'       : 'master.%06d' % (i % n_masters)}))

        tasks = tmgr.submit_tasks(tds)

        tmgr.add_pilots(pilot)
        tmgr.register_callback(task_state_cb)
        tmgr.wait_tasks(uids=[t.uid for t in tasks])  # uids=[t.uid for t in tasks])

        for task in tasks:
            print('%s [%s]: %s' % (task.uid, task.state, task.stdout))

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------

