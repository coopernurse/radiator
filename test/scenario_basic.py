#!/usr/bin/env python
#
# Basic: 1 queue, 2 consumers, 1 producer.
#   - Producer sends 10,000 messages
#   - Consumers read all 10,000 in one pass
#   - Each consumer got the same number of messages
#   - All messages read
#   - No message read twice
#

import helper
import time

msgs_recvd = []
def on_msg(consumer_id, client, msg_id, body):
    #print "%d: got body: %s" % (consumer_id, body)
    msgs_recvd.append((time.time(), body))
    client.ack(msg_id)

dest_name = "/queue/scenario_basic"
msg_count = 1000

scenario = helper.ScenarioRunner(dest_name, msg_count, on_msg)
scenario.reset_files().run()

#print "%d %d" % (len(msgs_recvd), msg_count)
scenario.eq(len(msgs_recvd), msg_count)
for i in range(0, len(msgs_recvd)):
    #print "%s" % (msgs_recvd[i][1])
    scenario.eq(msgs_recvd[i][1],  "%d - %s" % (i, scenario.base_msg))

scenario.success("scenario_basic")
