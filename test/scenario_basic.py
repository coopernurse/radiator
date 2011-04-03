#!/usr/bin/env python
#
# Basic: 1 queue, 2 consumers, 1 producer.
#   - Producer sends 10,000 messages
#   - Consumers read all 10,000 in one pass
#   - Each consumer got the same number of messages
#   - All messages read
#   - No message read twice
#

import time
import radiator
from radiator.stomp import StompClient
from gevent import Greenlet

host = '127.0.0.1'
port = 61614

dest_name = "/queue/scenario_basic"
consumer_count = 1
message_count  = 1000

def server():
    radiator.start_server(host, port, dir="/tmp", fsync_millis=20)

def producer(dest_name, message_count):
    c = StompClient(host, port)
    for i in range(0, message_count):
        msg = "message %d" % i
        c.send(dest_name, msg)
    c.disconnect()

def consumer(consumer_id, dest_name, msgs_recvd):
    def on_msg(client, id, body):
        print "%d: got body: %s" % (consumer_id, body)
        msgs_recvd.append((time.time(), body))
        client.ack(id)

    c = StompClient(host, port)
    c.subscribe(dest_name, on_msg, auto_ack=False)
    c.drain(timeout=.1)
    c.disconnect()

##############################

s1 = Greenlet.spawn(server)
p1 = Greenlet.spawn(producer, dest_name, message_count)

consumers  = []
msgs_recvd = []
for i in range(0, consumer_count):
    gl = Greenlet.spawn(consumer, i, dest_name, msgs_recvd)
    consumers.append((gl, msgs_recvd))

p1.join()
for (consumer, msgs_recvd) in consumers:
    consumer.join()

msgs_recvd.sort()

print "%d %d" % (len(msgs_recvd), message_count)
assert len(msgs_recvd) == message_count
for i in range(0, len(msgs_recvd)):
    #print "%s" % (msgs_recvd[i][1])
    assert msgs_recvd[i][1] == "message %d" % i
