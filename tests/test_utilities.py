import unittest
import time
import logging

import dsf.utilities

class TestUtilities(unittest.TestCase):

    def setUp(self):
        #logging.disable(logging.CRITICAL)
        pass
        
    def test_match_topic_pattern(self):
        # (topic,pattern,case_sensitive,should_match)
        test_matches = [
            ("exact.match.test","exact.match.test",False,True),
            ("wildcard.long.match","wildcard.#",False,True),
            ("wildcard.long.match","wildcard.#",False,True),
            ("singleword","singleword.#",False,True),
            ("wrongword","anotherword.*",False,False),
            ("singleword","singleword.*.longmatch",False,False),
            ("singleword","*",False,True),
            ("singleword","singleword",False,True),
            ("numerous.words","numerous.words.also",False,False),
            ("case.sensitive","cASE.sensitive",True,False),
            ("case.SEnsitive","case.sensitive",False,True),
            ("many.topic.terms","*",False,True),
            ("many.topic.terms","#",False,True),
        ]
        
        for test_match in test_matches:
            failures = []
            (topic,pattern,case_sensitive,expected) = test_match
            result = dsf.utilities.match_topic_pattern(topic,pattern,case_sensitive)
            msg = "match_topic_pattern(\"%s\",\"%s\",case_sensitive=%s) returned %s; expected %s" % (topic,pattern,case_sensitive,result,expected)
            self.assertEqual(result,expected,msg)

        self.assertRaises(Exception, dsf.utilities.match_topic_pattern, "topic.with.illegal.#.characters","random.pattern")

        return True

    def tearDown(self):
        pass