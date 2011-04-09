from gevent import Greenlet, Timeout
from gevent.pool import Pool
from gevent.server import StreamServer
from gevent.socket import create_connection

from radiator import RadiatorTimeout
from stomp import StompServer, StompClient

class GeventReactor(object):

    def __init__(self, host, port, pool_size=5000, client_timeout=None):
        self.host = host
        self.port = port
        self.pool_size = pool_size
        self.client_timeout = client_timeout

    def start_server(self, broker):
        def on_connect(sock, addr):
            conn = GeventConnection(sock)
            StompServer(conn, broker).drain()
            
        def start():
            pool   = Pool(self.pool_size)
            server = StreamServer((self.host, self.port), on_connect, spawn=pool)
            server.serve_forever()

        Greenlet.spawn(start)

    def start_client(self, cb):
        def start():
            sock = create_connection((self.host, self.port))
            conn = GeventConnection(sock, self.client_timeout)
            cb(StompClient(conn))
            
        return Greenlet.spawn(start)

class GeventConnection(object):

    def __init__(self, socket, timeout=None):
        self.timeout = timeout
        self.s = socket
        self.f = self.s.makefile()

    def close(self):
        self.s.close()
        self.s = None
        self.f = None

    def write(self, data):
        self.f.write(data)

    def flush(self):
        self.f.flush()

    def read(self, num):
        return self.f.read(num)

    def readline(self):
        if self.timeout > 0:
            with Timeout(self.timeout, False):
                line = self.f.readline()
                if not line:
                    raise RadiatorTimeout
                else:
                    return line
        else:
            line = self.f.readline()
            if not line:
                raise BufferError
            else:
                return line
