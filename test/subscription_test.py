#!/usr/bin/env python

import unittest
import sys, os
sys.path.insert(0, os.path.dirname(__file__) + os.sep + "..")

import radiator

class SubscriptionTest(unittest.TestCase):

    def test_session_notifies_dest(self):
        # create broker with 1 subscriber
        b = radiator.Broker()
        b.subscribe("d1", True, "s1", lambda a,b,c: self.assertTrue(True))
        b.send("d1", "my mesg")

        # verify subscriber knows about dest
        s1 = b.session_dict["s1"]
        self.assertEquals(1, len(s1.subscriptions))
        self.assertTrue(s1.subscriptions["d1"].auto_ack)

        # verify dest knows about broker
        d1 = s1.subscriptions["d1"].dest
        self.assertEquals(1, len(d1.subscribers))
        self.assertEquals(s1, d1.subscribers["s1"])

        # unsubscribe - session and dest should both be notified
        b.unsubscribe("d1", "s1")
        self.assertEquals(0, len(s1.subscriptions))
        self.assertFalse(s1.subscriptions.has_key("d1"))
        self.assertEquals(0, len(d1.subscribers))
        
    def test_destroy_session_unsubs_from_dests(self):
        b = radiator.Broker()
        b.subscribe("d1", True, "s1", lambda a,b,c: self.assertTrue(True))
        b.send("d1", "my mesg")
        s1 = b.session_dict["s1"]
        d1 = s1.subscriptions["d1"].dest
        s1.destroy()
        self.assertEquals(0, len(s1.subscriptions))
        self.assertEquals(0, len(d1.subscribers))

    def test_destroy_dest_notifies_session(self):
        b = radiator.Broker()
        b.subscribe("d1", True, "s1", lambda a,b,c: self.assertTrue(True))
        b.send("d1", "my mesg")
        s1 = b.session_dict["s1"]
        d1 = s1.subscriptions["d1"].dest
        d1.destroy()
        self.assertEquals(0, len(s1.subscriptions))
        self.assertEquals(0, len(d1.subscribers))

    def test_new_dest_notifies_session(self):
        # subscriber subscribes with wildcard
        b = radiator.Broker()
        b.subscribe("/topic/logs.>", True, "s1",
                    lambda a,b,c: self.assertTrue(True))
        s1 = b.session_dict["s1"]
        self.assertEquals(1, len(s1.subscriptions))

        # publisher writes to topic that matches wildcard
        b.send("/topic/logs.email", "my mesg")
        
        # subscriptions are automatically wired up
        d1 = b.dest_dict["/topic/logs.email"]
        self.assertEquals(s1, d1.subscribers["s1"])
        self.assertEquals(d1, s1.subscriptions["/topic/logs.email"].dest)

    def test_dest_wildcard_matching(self):
        s = radiator.Subscription("/topic/foo.>", True, None)
        matches = [ "/topic/foo.1", "/topic/foo.bar.baz", "/topic/foo.z" ]
        for m in matches:
            self.assertTrue(s.matches(m))
        non_matches = [ "/topic/foo", "/topic/blah", "/queue/foo.2" ]
        for m in non_matches:
            self.assertFalse(s.matches(m))
        

if __name__ == "__main__":
    unittest.main()
