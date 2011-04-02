#!/usr/bin/env python

import threading
import time
from stompclient import PublishSubscribeClient
    
client = PublishSubscribeClient('127.0.0.1', 61613)

listener = threading.Thread(target=client.listen_forever)
listener.start()
client.listening_event.wait()

client.connect()
start = time.time()
for i in range(5000):
    client.send("/queue/testing", "This is the body of the frame.")
    client.send("/queue/testing", '{"key": "Another frame example."}')
print "elapsed: %.2f" % (time.time() - start)
client.disconnect()
