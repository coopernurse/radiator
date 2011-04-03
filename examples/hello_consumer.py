#!/usr/bin/env python
from radiator.stomp import StompClient

def on_test_queue_message(client, message_id, body):
    #print "on_message: %s %s" % (message_id, str(body))
    client.ack(message_id)

def on_error(err_message, body):
    print "on_error: %s %s" % str(err_message, body)

# connect to broker
client = StompClient('127.0.0.1', 61613, on_error=on_error)
client.subscribe("/queue/testing", on_test_queue_message, auto_ack=False)

while True:
    if client.drain(timeout=15) == 0:
        break

# disconnect will call drain automatically before
# removing callback to pickup any last messages sent
# since last ack
client.unsubscribe("/queue/testing")

client.disconnect()

