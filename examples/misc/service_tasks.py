#!/usr/bin/env python3

import os
import sys
import time
import socket

import radical.pilot as rp


n_nodes = 1

# ------------------------------------------------------------------------------
#
def app():

    session = rp.Session()
    try:
        pmgr  = rp.PilotManager(session=session)
        tmgr  = rp.TaskManager(session=session)
        pdesc = rp.PilotDescription({'resource': 'local.localhost',
                                     'runtime' : 15,
                                     'nodes'   : n_nodes,
                                     'services': list()})

        for idx in range(n_nodes):
            pdesc['services'].append(
                rp.TaskDescription({
                    'uid'           : 'test_service_%02d' % idx,
                    'executable'     : __file__,
                    'arguments'      : ['service', 1024 + 1 + idx],
                  # 'stdout'         : '/tmp/watcher_%d.out' % idx,
                    'ranks'          : 1,
                    'cores_per_rank' : 1,
                    'named_env'      : 'rp',
                })
            )
        pilot = pmgr.submit_pilots(pdesc)
        tmgr.add_pilots(pilot)

        td = rp.TaskDescription({'executable': '/bin/date',
                                 'services'  : ['test_service_00',
                                                'test_service_01']})
        task = tmgr.submit_tasks(td)
        tmgr.wait_tasks(task.uid)
        print(' * %s: %s' % (task.uid, task.stdout))

      # pilot.register_service('ext_service', 'tcp://localhost:12345')
      #
      # td = rp.TaskDescription({'executable': '/bin/date',
      #                          'services'  : ['ext_service']})
      # task = tmgr.submit_tasks(td)
      # tmgr.wait_tasks(task.uid)

        # ---------------------------------------------------------------------
        # start SERVICE instance and wait for startup info
        sd = rp.TaskDescription(
             {'uid'          : 'my_service',
              'mode'         : rp.TASK_SERVICE,
              'executable'   : __file__,
              'arguments'    : ['service', 1024],
              'info_pattern' : 'stdout:.*port: ([0-9]*).*',
              'timeout'      : 10,  # startup timeout
              'named_env'    : 'rp',
              })
        service = tmgr.submit_tasks(sd)

        # collect the endpoint port
        info = service.wait_info()
        print(' * %s: %s' % (service.uid, info))


        # ---------------------------------------------------------------------
        # run the clients
        cds = list()
        for i in range(3):
            cd = rp.TaskDescription({'services'   : [service.uid],
                                     'executable' : __file__,
                                     'arguments'  : ['client']})
            cds.append(cd)

        clients = tmgr.submit_tasks(cds)
        tmgr.wait_tasks(uids=[client.uid for client in clients])

        for client in clients:
            print(' * %s: %s' % (client.uid, client.stdout))

    finally:
        session.close(download=True)


# ------------------------------------------------------------------------------
#
def service(port):

    port = int(port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(1.0)
    sock.bind(('localhost', port))

    sys.stdout.write('port: %d\n' % port)
    sys.stdout.write('pid : %d\n' % os.getpid())
    sys.stdout.flush()

    sock.listen(1)
    start = time.time()
    while True:

        print('%.2f: service loop' % time.time())

        try:
            client, _ = sock.accept()
            client.send(client.recv(port))
            client.close()
            print('%.2f: service served' % time.time())

        except socket.timeout:
            # run for 60 seconds, at most
            if time.time() - start > 3600:
                break


# ------------------------------------------------------------------------------
#
def client():

    port = int(os.environ['RP_INFO_MY_SERVICE'])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', port))
    sock.send(b'foo')
    print(str(sock.recv(port)))
    sock.close()


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    mode = sys.argv[1] if len(sys.argv) > 1 else 'app'
    port = sys.argv[2] if len(sys.argv) > 2 else 1024

    if   mode == 'app'    : app()
    elif mode == 'service': service(port)
    elif mode == 'client' : client()
    else: raise ValueError('unknown mode %s' % mode)


# ------------------------------------------------------------------------------

