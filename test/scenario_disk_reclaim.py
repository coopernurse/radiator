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

msgs_recvd = [ ]
def on_msg(consumer_id, client, msg_id, body):    
    msgs_recvd.append((time.time(), body))
    if len(msgs_recvd) < 5001:
        client.ack(msg_id)

dest_name = "/queue/scenario_disk_reclaim"
msg_count = 10000
consumers = 1

scenario = helper.ScenarioRunner(dest_name, msg_count, on_msg,
                                 consumers=consumers,
                                 client_timeout=3,
                                 rewrite_interval_secs=10,
                                 delay_consumers=True)
scenario.reset_files().run()

del msgs_recvd[:]
gl = []
for i in range(0, 10):
    t = scenario.reactor.start_client(lambda c: scenario.start_consumer(c, i))
    gl.append(t)
for g in gl:
    g.join()

fsize = os.path.getsize(scenario.filename)
scenario.lt(fsize, (msg_count*1072*.11))
    
scenario.success("scenario_disk_reclaim")
