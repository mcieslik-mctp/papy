# -*- coding: utf-8 -*-
"""
tests for ``papy.util.config``.
"""
import unittest
import tempfile
import os
from papy.util.config import start_logger, get_defaults
from logging import getLogger


class test_logger(unittest.TestCase):

    def test_start_logger(self):
        fd, name = tempfile.mkstemp()
        os.close(fd)
        defl = start_logger(log_to_file=True, log_filename=name)
        assert defl is None
        log = getLogger()
        log.error("at error level")
        fh = open(name)
        fh.seek(0)
        assert fh.read().endswith('[root] - at error level\n')
        log.info("at info level")
        fh.seek(0)
        lines = fh.readlines()
        assert lines[1].endswith('[root] - at info level\n')

    def test_get_defaults(self):
        defs = get_defaults()
        assert 'TCP_SNDBUF' in defs
        assert 'PIPE_BUF' in defs
        assert isinstance(defs['TCP_RCVBUF'], int)



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
