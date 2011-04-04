
import os
import base64
import time

from radiator.stomp import StompClient
from gevent import Greenlet

import radiator

class ScenarioRunner(object):

    host = '127.0.0.1'
    port = 61614

    def __init__(self, dest_name, msg_count, on_msg,
                 dir="/tmp", consumers=1, auto_ack=False,
                 consumer_timeout=.1, delay_consumers=False):
        self.dest_name = dest_name
        self.dir = dir
        self.consumers = consumers
        self.msg_count = msg_count
        self.on_msg = on_msg
        self.auto_ack = auto_ack
        self.consumer_timeout = consumer_timeout
        self.delay_consumers = delay_consumers

    def eq(self, a, b):
        assert a == b, "%s != %s" % (str(a), str(b))

    def lt(self, a, b):
        assert a < b, "%s >= %s" % (str(a), str(b))

    def gt(self, a, b):
        assert a > b, "%s <= %s" % (str(a), str(b))

    def run(self):
        Greenlet.spawn(lambda: self.start_server())
        gl = []
        start = time.time()
        gl.append(Greenlet.spawn(lambda: self.start_producer()))
        if self.delay_consumers:
            gl.pop().join()
        for i in range(self.consumers):
            gl.append(Greenlet.spawn(lambda: self.start_consumer(i)))
        for g in gl:
            g.join()
        self.millis = int((time.time() - start) * 1000)

    def reset_files(self):
        basename = base64.urlsafe_b64encode(self.dest_name)
        self.pending_filename = os.path.join(self.dir,
                                             "%s.pending.dat" % basename)
        self.in_use_filename  = os.path.join(self.dir,
                                             "%s.in-use.dat" % basename)
        if os.path.exists(self.pending_filename):
            os.remove(self.pending_filename)
        if os.path.exists(self.in_use_filename):
            os.remove(self.in_use_filename)
        return self

    def start_server(self):
        radiator.start_server(self.host, self.port,
                              dir=self.dir, fsync_millis=20)

    def start_producer(self):
        msg = ""
        for i in range(0, 1024):
            msg += "z"
        self.base_msg = msg

        c = StompClient(self.host, self.port)
        for i in range(0, self.msg_count):
            body = "%d - %s" % (i, msg)
            c.send(self.dest_name, body)
        c.disconnect()

    def start_consumer(self, c_id):
        c = StompClient(self.host, self.port)
        c.subscribe(self.dest_name,
                    lambda c, msg_id, body: self.on_msg(c_id, c, msg_id, body),
                    auto_ack=self.auto_ack)
        c.drain(timeout=self.consumer_timeout, max=5001)
        c.disconnect()

    def success(self, name):
        print "SUCCESS: %s millis=%d consumers=%d msg_count=%d" % \
              (name, self.millis, self.consumers, self.msg_count)
