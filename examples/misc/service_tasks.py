#!/usr/bin/env python3

import os
import sys
import time
import socket

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
def app():

    session = rp.Session()
    try:
        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)
        pdesc = rp.PilotDescription({'resource': 'local.localhost',
                                     'runtime' : 15,
                                     'nodes'   : 1})
        pilot = pmgr.submit_pilots(pdesc)
        tmgr.add_pilots(pilot)

        pilot.register_service('ext_service', 'tcp://localhost:12345')

        td = rp.TaskDescription({'executable': '/bin/date',
                                 'services'  : ['ext_service']})
        task = tmgr.submit_tasks(td)
        tmgr.wait_tasks(task.uid)

        # ---------------------------------------------------------------------
        # start SERVICE instance and wait for startup info
        sd = rp.TaskDescription(
             {'uid'          : 'my_service',
              'mode'         : rp.TASK_SERVICE,
              'executable'   : __file__,
              'arguments'    : ['service'],
              'info_pattern' : 'stdout:.*port: ([0-9]*).*',
              'timeout'      : 10,  # startup timeout
              'named_env'    : 'rp',
              })
        service = tmgr.submit_tasks(sd)

        # collect the endpoint port
        info = service.wait_info()
        print('found %s: %s' % (service.uid, info))


        # ---------------------------------------------------------------------
        # run the clients
        cds = list()
        for i in range(4):
            cd = rp.TaskDescription({'services'   : [service.uid],
                                     'executable' : __file__,
                                     'arguments'  : ['client']})
            cds.append(cd)

        clients = tmgr.submit_tasks(cds)
        tmgr.wait_tasks(uids=[client.uid for client in clients])

        for client in clients:
            print('%s: %s' % (client.uid, client.stdout))

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------
#
def service():

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(0.2)
    sock.bind(('localhost', 12345))

    sys.stdout.write('port: 12345\n')
    sys.stdout.flush()

    sock.listen(1)
    start = time.time()
    while True:
        try:
            client, _ = sock.accept()
            client.send(client.recv(1024))
            client.close()
        except socket.timeout:
            # run for 60 seconds, at most
            if time.time() - start > 10:
                break


# ------------------------------------------------------------------------------
#
def client():

    port = int(os.environ['RP_INFO_MY_SERVICE'])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', port))
    sock.send(b'foo')
    print(str(sock.recv(1024)))
    sock.close()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    mode = sys.argv[1] if len(sys.argv) > 1 else 'app'

    if   mode == 'app'    : app()
    elif mode == 'service': service()
    elif mode == 'client' : client()
    else: raise ValueError('unknown mode %s' % mode)


# ------------------------------------------------------------------------------

