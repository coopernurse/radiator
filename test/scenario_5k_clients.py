#!/usr/bin/env python
#
# Basic: 1 queue, 5000 consumers, 1 producer.
#   - Producer sends 5,000 messages
#   - Consumers read all 5,000 in one pass
#   - Each consumer got one messages
#

import helper
import time

msgs_recvd = []
def on_msg(consumer_id, client, msg_id, body):
    msgs_recvd.append((time.time(), body))

dest_name = "/queue/scenario_5k_clients"
msg_count = 5000

scenario = helper.ScenarioRunner(dest_name, msg_count, on_msg,
                                 consumers=msg_count, consumer_timeout=5)
scenario.reset_files().run()

scenario.eq(len(msgs_recvd), msg_count)
scenario.eq(msg_count, scenario.consumer_success)
scenario.success("scenario_5k_clients")
