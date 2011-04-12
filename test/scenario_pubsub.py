#!/usr/bin/env python
#
# Basic: 1 topic, 50 consumer/producer clients
#   - Each consumer writes 10 messages
#   - Each consumer gets all 10 messages
#   - 1 consumer unsubs
#   - 1 more message sent
#   - Only the remaining 9 clients get the msg
#

import time

import helper
from radiator import Broker

consumers   = 100
msg_to_send = 5
dest_name   = "/topic/scenario_pubsub"

msgs_recvd = []
c_id = 0

def append(msg_count, msg):
    msg_count.append(msg)

def pubsub_client(scenario, c):
    global c_id
    my_id = c_id
    c_id += 1
    
    msg_count = []

    c.subscribe(dest_name,
                lambda a,b,c: append(msg_count, "%d %s" % (my_id, c)),
                auto_ack=True)

    # allow all clients a chance to subscribe
    scenario.reactor.sleep(.3)
    
    for i in range(msg_to_send):
        c.send(dest_name, "message %d from %d" % (i, my_id))
        while c.drain(timeout=.1) > 0: pass

    if my_id == 0:
        c.unsubscribe(dest_name)

    if my_id == 1:
        c.send(dest_name, "last message!")

    while c.drain(timeout=5) > 0: pass
    c.disconnect()
    msgs_recvd.append(len(msg_count))

class PubSubScenario(helper.BaseScenarioRunner):

    def run(self):
        broker = Broker('', 0, 10)
        self.reactor.client_timeout = .2
        self.reactor.start_server(broker)
        self.gl = []
        for i in range(0, consumers):
            t = self.reactor.start_client(lambda c: pubsub_client(self, c))
            self.gl.append(t)
        start = time.time()
        for t in self.gl:
            t.join()
        self.millis = int((time.time() - start) * 1000)

    def success(self, name):
        total_msg = 0
        for m in msgs_recvd: total_msg += m
        print "SUCCESS: %s millis=%d consumers=%d msg_to_send=%d total_msg=%d" \
              % (name, self.millis, consumers, msg_to_send, total_msg)

scenario = PubSubScenario()
scenario.run()

scenario.eq(len(msgs_recvd), consumers)
i = 0
for m in msgs_recvd:
    expected = msg_to_send*consumers + 1
    if i == 0:
        expected -= 1
    scenario.eq(expected, m)
    i += 1
scenario.success("scenario_pubsub")
