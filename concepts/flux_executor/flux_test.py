#!/usr/bin/env python3

import os
import sys
import time
import json
import shlex
import signal

import subprocess as sp
import threading  as mt

import flux
import flux.job

n_tasks = 10
n_tasks = 1024 * 32

# ------------------------------------------------------------------------------
#
spec = {'tasks'     : [{'slot'   : 'task',
                        'count'  : {'per_slot': 1},
                        'command': ['/bin/true']}],
        'attributes': {'system'  : {'duration': 1}},
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
    spec = flux.job.JobspecV1.from_command(["/bin/true"])
    for _ in range(n_tasks):
        fut = exe.submit(spec)
        sys.stdout.write('+')
        sys.stdout.flush()
        fut.add_event_callback('finish', cb)
        sys.stdout.write('-')
        sys.stdout.flush()
      # for ev in ['submit', 'alloc', 'start',
      #            'finish', 'release', 'exception']:
      #     f2.add_event_callback(ev, cb)

    while done < n_tasks:
        time.sleep(1)
    print('\n#')


# ------------------------------------------------------------------------------
#
def server():

    l_cmd = 'srun -N 2 --ntasks-per-node 1 ' \
            '--cpus-per-task=56 --gpus-per-task=8 --export=ALL' 
    s_cmd = 'echo URI:$FLUX_URI && sleep inf'
    f_cmd = 'flux start bash -c "%s"' % s_cmd

    cmd   = '%s %s' % (l_cmd, f_cmd)
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

    sys.stdout.write('%s\n' % uri)
    sys.stdout.write('flux resources [%d %s]:\n%s\n' % (ret, err, out))
    sys.stdout.flush()

    def watch():
        while True:
            time.sleep(1)
            out, err, ret = sh_callout('flux ping -c 1 kvs')
            sys.stdout.write('^')
            sys.stdout.flush()
            if ret:
                sys.exit(1)

    watcher = mt.Thread(target=watch)
    watcher.daemon = True
    watcher.start()

    return uri, proc.pid

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    uri, pid = server()
    client(uri)

    os.kill(pid, signal.SIGTERM)
    time.sleep(0.1)

# ------------------------------------------------------------------------------

