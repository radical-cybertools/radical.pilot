#!/usr/bin/env dragon

import os
import sys
import time
import dragon

import multiprocessing as mp
import threading       as mt

import radical.utils   as ru


# ------------------------------------------------------------------------------
#
class Req(ru.Message):

    _schema  = {'uid': str,
                'cmd': str}

    _default = {'_msg_type' : 'req',
                'cmd'       : ''}

ru.Message.register_msg_type('req', Req)


# ------------------------------------------------------------------------------
#
class Rep(ru.Message):

    _schema  = {'uid'       : str,
                'ret'       : int,
                'out'       : str,
                'err'       : str}

    _default = {'_msg_type' : 'rep',
                'ret'       : 0,
                'out'       : '',
                'err'       : ''}

ru.Message.register_msg_type('rep', Rep)

# ------------------------------------------------------------------------------
#
class Server(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self, addr_req, addr_rep):

        print('server', 1)

      # mp.set_start_method("dragon")

        pipe_req = ru.zmq.Pipe(ru.zmq.MODE_PULL, url=addr_req)
        pipe_rep = ru.zmq.Pipe(ru.zmq.MODE_PUSH, url=addr_rep)

        time.sleep(0.1)

        print('server', 2)
        while True:

            print('server', 3)
            req = pipe_req.get()
            print('server req', req)
            p = mp.Process(target=self.work, args=(req['uid'], req['cmd']))
            p.start()
            p.join()

            rep = Rep(uid=req['uid'])
            pipe_rep.put(rep)


    # --------------------------------------------------------------------------
    #
    def work(self, uid, cmd):
        out, err, ret = ru.sh_callout(cmd)
        print('====', uid, os.getpid(), out, err, ret)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    print('client', 1)

    pipe_req = ru.zmq.Pipe(ru.zmq.MODE_PUSH)
    pipe_rep = ru.zmq.Pipe(ru.zmq.MODE_PULL)

    time.sleep(0.1)

    print('client', 2)

    def serve(req_url, rep_url):
        s = Server(req_url, rep_url)

    t_server = mt.Thread(target=serve, args=[pipe_req.url, pipe_rep.url])
    t_server.daemon = True
    t_server.start()

    time.sleep(0.1)

    n = 1
    for idx in range(n):

        print('client req idx', idx)
        req = Req(uid=ru.generate_id('task'), cmd='/bin/date')
        d = req.as_dict()
        print(d)
        pipe_req.put(d)


    for idx in range(n):

        print('client rep idx', idx)
        rep = pipe_rep.get()
        print('=====', rep)


    time.sleep(0.1)

    print('done')


# ------------------------------------------------------------------------------

