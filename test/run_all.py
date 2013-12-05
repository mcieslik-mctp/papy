#!/usr/bin/env python
"""
Discovers all test files within the directory of the script (not the current 
working directory and) and runs the test suites.
"""

import glob
import os
import sys
import unittest

start_path = os.getcwd()
# need to find files
script_path = os.path.dirname(os.path.realpath(__file__))
# need to find nubox
root_path = script_path[:-5]
work_path = root_path[:-5]
sys.path.insert(0, root_path + '/src')
sys.path.insert(0, root_path + '/src')
sys.path.insert(1, work_path + '/numap/src')
os.chdir(script_path)
test_file_strings = glob.glob('test_*.py')
module_strings = [str[0:len(str) - 3] for str in test_file_strings]
suites = [unittest.defaultTestLoader.loadTestsFromName(str) for str
          in module_strings]
testSuite = unittest.TestSuite(suites)
text_runner = unittest.TextTestRunner().run(testSuite)
