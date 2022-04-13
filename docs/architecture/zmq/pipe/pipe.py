
# pylint: disable=no-member


import zmq
import msgpack


class Pipe(object):

    # --------------------------------------------------------------------------
    #
    def __init__(self) -> None:

        self._context = zmq.Context()
        self._push    = None
        self._pull    = None
        self._poller  = zmq.Poller()
        self._url     = None


    # --------------------------------------------------------------------------
    #
    @property
    def url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    def connect_push(self, url: str = None):

        if self._url:
            raise RuntimeError('already connected at %s' % self._url)

        if url:
            bind = False
        else:
            bind = True
            url  = 'tcp://*:*'

        print(bind)

        self._push = self._context.socket(zmq.PUSH)

        if bind: self._push.bind(url)
        else   : self._push.connect(url)

        self._url = self._push.getsockopt(zmq.LAST_ENDPOINT)


    # --------------------------------------------------------------------------
    #
    def connect_pull(self, url: str = None):

        if self._url:
            raise RuntimeError('already connected at %s' % self._url)

        if url:
            bind = False
        else:
            bind = True
            url  = 'tcp://*:*'

        print(bind)

        self._pull = self._context.socket(zmq.PULL)

        if bind: self._pull.bind(url)
        else   : self._pull.connect(url)

        self._url = self._pull.getsockopt(zmq.LAST_ENDPOINT)
        self._poller.register(self._pull, zmq.POLLIN)


    # --------------------------------------------------------------------------
    #
    def put(self, msg):

        self._push.send(msgpack.packb(msg))


    # --------------------------------------------------------------------------
    #
    def get(self):

        return msgpack.unpackb(self._pull.recv())


    # --------------------------------------------------------------------------
    #
    def get_nowait(self, timeout: int = 0):

        socks = dict(self._poller.poll(timeout=timeout))

        if self._pull in socks:
            return msgpack.unpackb(self._pull.recv())


# ------------------------------------------------------------------------------

if __name__ == '__main__':

    pipe_1 = Pipe()
    pipe_1.connect_push()
    print(2, pipe_1.url)

    url = pipe_1.url
    print(url)

    pipe_2 = Pipe()
    pipe_2.connect_pull(url)
    print(4, pipe_2.url)

    for i in range(1000):
        pipe_1.put('foo %d' % i)

    for i in range(1000):
        print(5, pipe_2.get())


