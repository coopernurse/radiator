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
        msg = "0123456789"
        q = self.q
        id = q.send(msg)
        msg2 = q.receive(False)
        self.assertTrue(q.msg_in_use(id))
        self.assertEquals(msg, msg2[1].body)

    def test_single_message(self):
        msg = "abcabcabc"
        q = self.q
        id = q.send(msg)
        (msg_q, msg2) = q.receive(True)
        self.assertTrue(id != None)
        self.assertEquals(q, msg_q)
        self.assertEquals(msg, msg2.body)
        self.assertEquals(id, msg2.id)
        self.assertFalse(q.msg_in_use(id))

    def test_reload_state(self):
        q = self.q
        q.send("abcd")
        id = q.send("1234")
        
        q2 = radiator.FileQueue("test")
        self.assertEquals(2, q2.pending_messages())
        self.assertEquals(0, q2.in_use_messages())
        q2.receive(True)
        self.assertEquals(1, q2.pending_messages())
        self.assertEquals(0, q2.in_use_messages())
        self.assertEquals(64, os.path.getsize(q2.filename))
        msg = q2.receive(False)
        self.assertEquals((id, "1234"), (msg[1].id, msg[1].body))
        self.assertEquals(0, q2.pending_messages())
        self.assertEquals(64, os.path.getsize(q2.filename))
        self.assertEquals(1, q2.in_use_messages())
        q2.ack(msg[1].id)
        self.assertEquals(0, q2.in_use_messages())
        self.assertEquals(0, q2.pending_messages())
        self.assertEquals(64, os.path.getsize(q2.filename))

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
