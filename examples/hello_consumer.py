#!/usr/bin/env python

import threading
import time

from stompclient import PublishSubscribeClient

def frame_received(frame):
    # Do something with the frame!
    print "----Received Frame----\n%s" % frame
    client.ack(frame.message_id)
    print "Acked message: %s" % frame.message_id
    print ""
    
client = PublishSubscribeClient('127.0.0.1', 61613)

listener = threading.Thread(target=client.listen_forever)
listener.start()
client.listening_event.wait()

client.connect()
client.subscribe("/queue/testing", frame_received, ack='client')

time.sleep(5)
client.unsubscribe("/queue/testing")

time.sleep(1)

client.disconnect()
