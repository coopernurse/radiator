#!/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(__file__) + os.sep + "..")

import radiator
from radiator import reactor

r = reactor.GeventReactor('127.0.0.1', 61613)
radiator.start_server(r, dir="/tmp", fsync_millis=20)
