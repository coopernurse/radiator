#!/usr/bin/env python

import time
from radiator.reactor import GeventReactor

# connect to broker
r = GeventReactor('127.0.0.1', 61613)
client = r.start_client_sync()

# send messages
start = time.time()
for i in range(10000):
    client.send("/queue/testing", "%d: This is the body of the frame." % i)
    client.send("/queue/testing", '%d: {"key": "Another frame example."}' % i)
print "elapsed: %.2f" % (time.time() - start)

# read any errors
client.drain(timeout=0.05)

# hang up
client.disconnect()
