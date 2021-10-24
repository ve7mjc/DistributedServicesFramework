# tests/runner.py
import unittest

import sys
sys.path.append('./')

# import your test modules
import messageprocessingpipeline

# initialize the test suite
loader = unittest.TestLoader()
suite  = unittest.TestSuite()

# add tests to the test suite
suite.addTests(loader.loadTestsFromModule(messageprocessingpipeline))

# initialize a runner, pass it your suite and run it
runner = unittest.TextTestRunner(verbosity=3)
result = runner.run(suite)



"""
.assertEqual(a, b) 	a == b
.assertTrue(x) 	bool(x) is True
.assertFalse(x) 	bool(x) is False
.assertIs(a, b) 	a is b
.assertIsNone(x) 	x is None
.assertIn(a, b) 	a in b
.assertIsInstance(a, b) 	isinstance(a, b)
can add Not to all as well, eg. assetIsNotNone
"""