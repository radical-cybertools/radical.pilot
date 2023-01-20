#!/usr/bin/env python3

'''
Demonstrate the features of Agent Services.
'''

import os
import sys

import radical.utils as ru
import radical.pilot as rp
import platform
import socket
import re
import uuid
import logging


logger = ru.Logger('agent_services')



def get_system_info():
    try:
        info = {}
        info['platform'] = platform.system()
        info['platform-release'] = platform.release()
        info['platform-version'] = platform.version()
        info['architecture'] = platform.machine()
        info['hostname'] = socket.gethostname()
        info['ip-address'] = socket.gethostbyname(socket.gethostname())
        info['mac-address'] = ':'.join(re.findall('..', '%012x' % uuid.getnode()))
        info['processor'] = platform.processor()
        print(info)
    except Exception as e:
        logging.exception(e)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if len(sys.argv) < 2:
        cfg_file = '%s/%s' % (os.path.dirname(os.path.abspath(__file__)),
                              'agent_services.cfg')
    else:
        cfg_file = sys.argv[1]

    cfg = ru.Config(cfg=ru.read_json(cfg_file))
    sleep = int(cfg.sleep)
    cores_per_node = cfg.cores_per_node
    ranks_per_node = cfg.ranks_per_node
    gpus_per_node = cfg.gpus_per_node

    # we use a reporter class for nicer output
    report = ru.Reporter(name='radical.pilot')
    report.title('Agent Service example (RP version %s)' % rp.version)

    session = rp.Session()
    try:
        pd = rp.PilotDescription(cfg.pilot_descr)

        pd.cores = 2
        pd.gpus = 0

        pd.runtime = cfg.runtime

        pmgr = rp.PilotManager(session=session)
        tmgr = rp.TaskManager(session=session)
        # tmgr.register_callback(task_state_cb)

        pilot = pmgr.submit_pilots(pd)
        tmgr.add_pilots(pilot)

        pmgr.wait_pilots(uids=pilot.uid, state=[rp.PMGR_ACTIVE])

        td = rp.TaskDescription(cfg.master_descr)
        td.mode = rp.AGENT_EXECUTING
        # td.arguments = [cfg_file, i]
        td.cpu_processes = 1

        tds = list()
        tds.append(rp.TaskDescription({
            # 'uid': 'task.exe.c.%06d' % i,
            'mode': rp.TASK_EXECUTABLE,
            'scheduler': None,
            'ranks': 2,
            'executable': '/bin/sh',
            'arguments': ['-c',
                          'sleep %d;' % sleep +
                          'echo "hello $RP_RANK/$RP_RANKS: $RP_TASK_ID"']}))
        report.info('Submit non-raptor task(s) %s \n'
                    % str([t.uid for t in tds]))



    finally:
        session.close(download=True)

    report.info('Logs from the master task should now be in local files \n')
