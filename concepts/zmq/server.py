#!/usr/bin/env python3

import zmq
import msgpack

import threading as mt


# ------------------------------------------------------------------------------
#
class Server(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self) -> None:

        self._ctx    = zmq.Context()
        self._url    = None
        self._thread = None
        self._term   = mt.Event()


    # --------------------------------------------------------------------------
    #
    @property
    def url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    def listen(self, url: str = None):

        if not url:
            url = 'tcp://*:*'

        if self._url:
            raise RuntimeError('already connected at %s' % self._url)

        self._sock = self._ctx.socket(zmq.SERVER)
        self._sock.bind(url)

        self._url = self._sock.getsockopt(zmq.LAST_ENDPOINT)

        self._thread = mt.Thread(target=self._work)
        self._thread.daemon = True
        self._thread.start()


    # --------------------------------------------------------------------------
    #
    def _work(self):

        poller = zmq.Poller()
        poller.register(self._sock, zmq.POLLIN)

        while not self._term:

            info = poller.poll()
            if info:
                msg = msgpack.unpackb(self._sock.recv())
                print('< %s' % msg)
                msg['foo': 1]
                self._sock.send(msgpack.packb(msg))
                print('> %s' % msg)


# ------------------------------------------------------------------------------
#
class Client(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self) -> None:

        self._ctx = zmq.Context()
        self._url = None


    # --------------------------------------------------------------------------
    #
    @property
    def url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    def connect(self, url: str = None):

        if self._url:
            raise RuntimeError('already connected at %s' % self._url)

        self._sock = self._ctx.socket(zmq.CLIENT)
        self._sock.connect(url)

        self._url = self._sock.getsockopt(zmq.LAST_ENDPOINT)


    # --------------------------------------------------------------------------
    #
    def work(self):

        for i in range(3):

            msg = {'cnt': i}
            self._sock.send(msgpack.packb(msg))
            print('> %s' % msg)

            rep = msgpack.unpackb(self._sock.recv())
            print('> %s' % rep)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    server = Server()
    server.listen()

    client = Client()
    client.connect(server.url)


