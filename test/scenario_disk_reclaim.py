#!/usr/bin/env python
#
# Disk space reclaim
#   - Producer sends 10,000 messages
#   - Consumer reads 5,001.  Acks 4900 of them.
#   - Broker should reclaim disk space
#       - Pending file should only have ~5000 messages
#       - In use file should only have ~101 messages

import helper
import time
import os
from gevent import Greenlet

msgs_recvd = [ ]
def on_msg(consumer_id, client, msg_id, body):    
    msgs_recvd.append((time.time(), body))
    if len(msgs_recvd) < 4999:
        client.ack(msg_id)

dest_name = "/queue/scenario_disk_reclaim"
msg_count = 10000
consumers = 1

scenario = helper.ScenarioRunner(dest_name, msg_count, on_msg,
                                 consumers=consumers,
                                 consumer_timeout=3,
                                 delay_consumers=True)
scenario.reset_files().run()

del msgs_recvd[:]
gl = []
for i in range(0, 10):
    gl.append(Greenlet.spawn(lambda: scenario.start_consumer(i)))
for g in gl:
    g.join()

scenario.lt(os.path.getsize(scenario.pending_filename), (msg_count*1050*.51))
scenario.lt(os.path.getsize(scenario.in_use_filename), 12000)
    
scenario.success("scenario_disk_reclaim")
