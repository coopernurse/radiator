
import sys
import logging
import os
import base64
import time

from radiator import Broker
from radiator.reactor import GeventReactor

class ScenarioLogHandler(logging.Handler):

    errors = []

    def handle(self, record):
        errno = record.levelno
        if errno == logging.ERROR or errno == logging.CRITICAL:
            print "ERROR: %s" % record.msg
            self.errors.append(record)

class BaseScenarioRunner(object):

    reactor = GeventReactor('127.0.0.1', 61614)
    fsync_millis = 20
    rewrite_interval_secs = 10
    dir  = '/tmp'

    def eq(self, a, b):
        assert a == b, "%s != %s" % (str(a), str(b))

    def lt(self, a, b):
        assert a < b, "%s >= %s" % (str(a), str(b))

    def gt(self, a, b):
        assert a > b, "%s <= %s" % (str(a), str(b))

    def reset_files(self):
        basename = base64.urlsafe_b64encode(self.dest_name)
        self.filename = os.path.join(self.dir, "%s.msg.dat" % basename)
        if os.path.exists(self.filename):
            os.remove(self.filename)
        return self

    def start_server(self):
        broker = Broker(self.dir,
                        self.fsync_millis,
                        self.rewrite_interval_secs)
        self.reactor.start_server(broker)
        
class ScenarioRunner(BaseScenarioRunner):

    def __init__(self, dest_name, msg_count, on_msg,
                 dir="/tmp",
                 consumers=1,
                 auto_ack=False,
                 client_timeout=.1,
                 delay_consumers=False,
                 rewrite_interval_secs=10):
        self.dest_name = dest_name
        self.dir = dir
        self.consumers = consumers
        self.msg_count = msg_count
        self.on_msg = on_msg
        self.auto_ack = auto_ack
        self.client_timeout = client_timeout
        self.delay_consumers = delay_consumers
        self.consumer_success = 0
        self.rewrite_interval_secs = rewrite_interval_secs
        self.log_handler = ScenarioLogHandler()
        self.reactor.client_timeout = client_timeout

    def run(self):
        logging.basicConfig()
        logging.getLogger('radiator').addHandler(self.log_handler)
        self.start_server()

        reactor = self.reactor

        gl = []
        start = time.time()

        t = reactor.start_client(lambda c: self.start_producer(c))
        gl.append(t)
        if self.delay_consumers:
            reactor.join(gl.pop())

        total = self.consumers
        while total > 0:
            gl = []
            part = min(total, 200)
            for i in range(part):
                t = reactor.start_client(lambda c: self.start_consumer(c, i))
                gl.append(t)
            for t in gl:
                self.reactor.join(t)
            total -= part
        self.millis = int((time.time() - start) * 1000)

    def start_producer(self, c):
        msg = ""
        for i in range(0, 1024):
            msg += "z"
        self.base_msg = msg

        for i in range(0, self.msg_count):
            body = "%d - %s" % (i, msg)
            c.send(self.dest_name, body)
        c.disconnect()

    def start_consumer(self, c, c_id):
        try:
            c.subscribe(self.dest_name,
                        lambda c, m_id, body: self.on_msg(c_id, c, m_id, body),
                        auto_ack=self.auto_ack)
            msg_count = c.drain(timeout=self.client_timeout)
            while msg_count > 0:
                msg_count = c.drain(timeout=self.client_timeout)
            c.disconnect()
            self.consumer_success += 1
        except:
            print "Except"
            raise

    def success(self, name):
        if len(self.log_handler.errors) == 0:
            print "SUCCESS: %s millis=%d consumers=%d msg_count=%d" % \
                  (name, self.millis, self.consumers, self.msg_count)
        else:
            print "FAILURE: %s millis=%d consumers=%d msg_count=%d" % \
                  (name, self.millis, self.consumers, self.msg_count)
            sys.exit(1)
            
