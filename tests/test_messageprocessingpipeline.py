import unittest
import time
import logging

#from dsf.messageprocessingpipeline import MessageProcessingPipeline
#
#class TestMessageProcessingPipeline(unittest.TestCase):
#
#    def setUp(self):
#        logging.disable(logging.CRITICAL)
#        pass
#
#    def test_startup_shutdown_no_configuration(self):
#        
#        start_time = time.time()
#
#        mpp = MessageProcessingPipeline()
#        mpp.start()
#        mpp.join() # wait for thread to genuinely exit
#
#        print("test execution time: %s ms" % round(((time.time()-start_time)*1000),2))
#
#    def tearDown(self):
#        
#        pass