# -*- coding: utf-8 -*-
"""
tests for ``papy.util.codefile``.
"""
import unittest
from papy.util.codefile import I_SIG, L_SIG, P_LAY, P_SIG, W_SIG


class test_codefile(unittest.TestCase):

    def test_parts(self):
        assert I_SIG
        assert L_SIG
        assert P_LAY
        assert P_SIG
        assert W_SIG


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
