# -*- coding: utf-8 -*-
"""
tests for ``papy.util.config``.
"""
import unittest
import tempfile
import os
from papy.util.func import *



class test_func(unittest.TestCase):

    def test_plugger(self):
        none = plugger([1])
        assert none is None


    def test_dumpload_file(self):
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = dump_item([i])
            ii = load_item([file])
            assert ii == i

    def test_dumpload_fifo(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = dump_item([i], type='fifo')
            ii = load_item([file])
            assert ii == i

    def test_dumpload_socket(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = dump_item([i], type='socket')
            ii = load_item([file])
            assert ii == i

    def test_dumpload_file_mmap(self):
        import os
        a = ['aaa\n', 'b_b_b', 'abc\n', 'ddd']
        for i in a:
            file = dump_item([i])
            ii = load_item([file], type='mmap')
            assert ii.read(10000) == i

    def testpickle(self):
        a = ['aaaaaa', 'bbbbbbbm\n', 'ccccccccccc']
        for i in a:
            b = pickle_dumps([i])
            c = pickle_loads([b])
            assert i == c

    def testjson(self):
        a = ['aaaaaa', 'bbbbbbbm\n', 'ccccccccccc']
        for i in a:
            b = json_dumps([i])
            c = json_loads([b])
            assert i == c

    def test_dumpload_pickle_stream(self):
        a = ['aa\naaaa', 'bbbbbbbm\n', 'cccccc\nccccc']
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        fh = open(fn, 'wb')
        for i in a:
            dump_pickle_stream([i], fh)
        fh.close()
        fh = open(fn, 'rb')
        b = load_pickle_stream(fh)
        os.remove(fn)
        assert a == list(b)

    def test_dumpload_stream(self):
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        fh = open(fn, 'wb')
        fn1 = dump_stream(['first1\nfirst2\n'], fh)
        dump_stream(['second1\nsecond2\n'], fh)
        dump_stream(['third\n'], fh,)
        assert fn1 is None
        fh.close()
        fh = open(fn, 'rb')
        #print fh.read()
        assert list(load_stream(fh)) == ['first1\nfirst2\n',
                                        'second1\nsecond2\n',
                                        'third\n']
        fd, fn = tempfile.mkstemp()
        os.close(fd)
        fh = open(fn, 'wb')
        fn1 = dump_stream(['first1\nfirst2\n'], fh, 'XYZ')
        dump_stream(['second1\nsecond2\n'], fh, 'XYZ')
        dump_stream(['third\n'], fh, 'XYZ')
        assert fn1 is None
        fh.close()
        fh = open(fn, 'rb')
        assert list(load_stream(fh, 'XYZ')) == ['first1\nfirst2\n',
                                        'second1\nsecond2\n',
                                        'third\n']


#        dump_stream(['a'], handle, delimiter)

#    def testdump_stream(self):
#        fh = open('test_dump_stream', 'wb')
#        dump_work = Worker(workers.io.dump_stream, (fh, ''))
#        dumper = Piper(dump_work)
#        inbox = [['first1\nfirst2\n', 'second1\nsecond2\n', 'third\n']]
#        dumper(inbox)
#        assert list(dumper) == ['test_dump_stream', 'test_dump_stream', 'test_dump_stream']
#        fh.close()
#        fh = open('test_dump_stream', 'rb')
#        assert fh.read() == "\n\n".join(inbox[0]) + "\n\n"
#
#    def testload_stream(self):
#        fh = open('test_load_stream', 'wb')
#        dump_work = Worker(workers.io.dump_stream, (fh, 'SOME_STRING'))
#        dumper = Piper(dump_work)
#        inbox = [['first1\nfirst2\n', 'second1\nsecond2\n', 'third\n']]
#        dumper(inbox)
#        assert list(dumper)
#        fh.close()
#        fh = open('test_load_stream', 'rb')
#        load_work = workers.io.load_stream(fh, 'SOME_STRING')
#        assert list(load_work) == inbox[0]

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
