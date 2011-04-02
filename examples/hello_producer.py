#!/usr/bin/env python

import time
from radiator.stomp import StompClient

# connect to broker
client = StompClient('127.0.0.1', 61613)

# send messages
start = time.time()
for i in range(1000):
    client.send("/queue/testing", "%d: This is the body of the frame." % i)
    client.send("/queue/testing", '%d: {"key": "Another frame example."}' % i)
print "elapsed: %.2f" % (time.time() - start)

# read any errors
client.drain(timeout=0.05)

# hang up
client.disconnect()
