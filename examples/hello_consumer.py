#!/usr/bin/env python
from radiator.reactor import GeventReactor

total_msgs = 0

def on_test_queue_message(client, message_id, body):
    global total_msgs
    #print "on_message: %s %s" % (message_id, str(body))
    client.ack(message_id)
    total_msgs += 1

def on_error(err_message, body):
    print "on_error: %s %s" % str(err_message, body)

# connect to broker
r = GeventReactor('127.0.0.1', 61613)
client = r.start_client_sync()
client.on_error=on_error
client.subscribe("/queue/testing", on_test_queue_message, auto_ack=False)

while True:
    if client.drain(timeout=1) == 0:
        break

# disconnect will call drain automatically before
# removing callback to pickup any last messages sent
# since last ack
client.unsubscribe("/queue/testing")

client.disconnect()

print "Total messages: %d" % total_msgs
