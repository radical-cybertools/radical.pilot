#!/usr/bin/env python3

import os
import sys
import time
import json
import shlex

import subprocess as sp
import threading  as mt

import flux
import flux.job

# ------------------------------------------------------------------------------
#
spec = {'tasks'     : [{'slot'   : 'task',
                        'count'  : {'per_slot': 1},
                        'command': ['/bin/date']}],
        'attributes': {'system'  : {'duration': 10000}},
        'version'   : 1,
        'resources' : [{'count'  : 1,
                        'type'   : 'slot',
                        'label'  : 'task',
                        'with'   : [{'count': 1, 'type': 'core' }]}]
       }

# ------------------------------------------------------------------------------
#
def sh_callout(cmd):

    proc = sp.Popen(shlex.split(cmd), stdout=sp.PIPE, stderr=sp.PIPE)
    stdout, stderr = proc.communicate()
    ret            = proc.returncode
    return stdout.decode('utf-8'), stderr.decode('utf-8'), ret

# ------------------------------------------------------------------------------
#
done = 0
def client(uri):

    global done

    n_tasks = 1024
    exe     = flux.job.executor.FluxExecutor(handle_kwargs={'url': uri})
    handle  = flux.Flux(url=uri)
    print('=')

    def cb(fid, event):
        global done
        if 'finish' in str(event.name):
            sys.stdout.write('.')
            sys.stdout.flush()
            done += 1

    futures = list()
    for _ in range(n_tasks):
        futures.append(flux.job.submit_async(handle, json.dumps(spec)))
        sys.stdout.write('+')
        sys.stdout.flush()
    print('\n*')

    ids = []
    for fut in futures:
        ids.append(fut.get_id())

    for fid in ids:
        fut = exe.attach(fid)
        for ev in ['submit', 'alloc', 'start',
                   'finish', 'release', 'exception']:
            fut.add_event_callback(ev, cb)

    while done < n_tasks:
        time.sleep(1)
    print('\n#')


# ------------------------------------------------------------------------------
#
def server():

    f_cmd = 'echo URI:$FLUX_URI && sleep inf'
    cmd   = 'flux start bash -c "%s"' % f_cmd
    proc  = sp.Popen(shlex.split(cmd), encoding='utf-8',
                         stdin=sp.DEVNULL, stdout=sp.PIPE, stderr=sp.PIPE)

    flux_env = dict()
    while proc.poll() is None:
        line = proc.stdout.readline()
        if line and line.startswith('URI:'):
            uri = line.split(':', 1)[1].strip()
            break

    os.environ['FLUX_URI'] = uri
    out, err, ret = sh_callout('flux resource list')

    print(uri)
    print('flux resources [%d %s]:\n%s' % (ret, err, out))

    while True:
        time.sleep(1)
        out, err, ret = sh_callout('flux ping -c 1 kvs')
        print('flux ping [%d %s]:%s' % (ret, err, out))
        if ret:
            break

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    if   sys.argv[1] == 'server': server()
    elif sys.argv[1] == 'client': client(sys.argv[2])
    else: print('usage: %s [ server | client <url>]' % sys.argv[0])

# ------------------------------------------------------------------------------

