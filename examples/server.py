#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__) + os.sep + "..")
import radiator

radiator.start_server('127.0.0.1', 61613, dir="/tmp", fsync_millis=20)
