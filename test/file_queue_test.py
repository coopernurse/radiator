#!/usr/bin/env python

import unittest
import random
import sys, os
sys.path.insert(0, os.path.dirname(__file__) + os.sep + "..")

import radiator

class FileQueueTest(unittest.TestCase):

    def setUp(self):
        self.q = radiator.FileQueue("test")

    def tearDown(self):
        self.q._destroy()

    def test_deferred_ack(self):
        received = [ ]
        msg = "0123456789"
        q = self.q
        q.add_listener(lambda msg: received.append(msg), auto_ack=False)
        id = q.send(msg)
        self.assertTrue(q.msg_in_use(id))

    def test_single_message(self):
        received = [ ]
        msg = "abcabcabc"
        q = self.q
        q.add_listener(lambda msg: received.append(msg))
        id = q.send(msg)
        self.assertTrue(id != None)
        self.assertEquals(1, len(received))
        self.assertEquals((q, id, msg), received[0])
        self.assertFalse(q.msg_in_use(id))

    def test_reload_state(self):
        q = self.q
        q.send("abcd")
        id = q.send("1234")
        q2 = radiator.FileQueue("test")
        q2.add_listener(lambda msg: 1+2, auto_ack=True)
        self.assertEquals(1, q2.pending_messages())
        self.assertEquals(0, q2.in_use_messages())
        received = [ ]
        q2.add_listener(lambda msg: received.append(msg), auto_ack=False)
        self.assertEquals(1, len(received))
        self.assertEquals((q2, id, "1234"), received[0])
        self.assertEquals(0, q2.pending_messages())
        self.assertEquals(1, q2.in_use_messages())
        q2.ack(received[0][1])
        self.assertEquals(0, q2.in_use_messages())
        q3 = radiator.FileQueue("test")
        self.assertEquals(0, q3.pending_messages())
        self.assertEquals(0, q3.in_use_messages())

    def test_queue_name_validation(self):
        valid = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_.0123456789"
        for i in range(200):
            s = ''
            for x in range(8):
                c = chr(random.randint(0,255))
                if valid.find(c) == -1: s += c
            self.assertRaises(ValueError, lambda: radiator.FileQueue(s))
        self.assertRaises(ValueError, lambda: radiator.FileQueue(" "))
        self.assertRaises(ValueError, lambda: radiator.FileQueue(""))
        self.assertRaises(ValueError, lambda: radiator.FileQueue(None))

if __name__ == "__main__":
    unittest.main()
