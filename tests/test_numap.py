# -*- coding: utf-8 -*-
"""
tests for ``imap.IMap``.
"""

import os
import unittest
import numap
from numap import *
import math
import time
from random import randint
from multiprocessing import TimeoutError
from itertools import izip

#import logging
#LOG_FILENAME = '/tmp/logging_example.out'
#logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

def adder(inbox, *args, **kwargs):
    return inbox + 1
def miner(inbox, *args, **kwargs):
    return inbox - 1
def diver(inbox, *args, **kwargs):
    return 1 / inbox
def passer(inbox, *args, **kwargs):
    return inbox
def real_waiter(inbox):
    time.sleep(inbox)
    return inbox
def waiter(inbox, *args, **kwargs):
    time.sleep(0.1 * 1 / inbox)
    return inbox

class Test_numap(unittest.TestCase):

    heavy_repeats = 10
    repeats = 2
    short = range(1, 3)
    medium = range(1, 10)
    long = range(1, 500)

    def testimports(self):
        @imports(['re', 'sys'])
        def pr(i):
            return (re, sys)
        pr(1) # should not raise

    def test_rename(self):

        @imports(['ysy,sys'])
        def ysy_sys(i):
            return ysy

        @imports(['X.Y,multiprocessing.forking'])
        def X_Y(i):
            return Y

        @imports(['xml.dom.domreg'])
        def xml_dom_domreg(i):
            return domreg

        @imports(['re'])
        def retre_yes(i):
            return re

        def retre_no(i):
             return re

        assert ['ysy,sys'] == ysy_sys.imports
        assert ysy_sys.func_globals.get('ysy')
        import sys
        assert ysy_sys.func_globals.get('ysy') is sys

        import multiprocessing.forking
        assert multiprocessing.forking is X_Y([1])
        assert multiprocessing.forking is Y
        assert domreg is xml_dom_domreg([1])


    def _test_random(self):
        for wt in ('process', 'thread'):
            for wn in (1, 3, 5, 7, 9, 11):
                for st in (1, 2, 3, 4, 5, 8, 13, 27):
                    ends = []
                    end_tasks = []
                    end_tasks_len = []
                    pipe_num = randint(1, 15)       # random number of pipes
                    pipe_inp = [randint(0, 100) for i in range(randint(1, 100))]
                    imap = NuMap(worker_type=wt, stride=st, buffer=pipe_num * st, \
                                     worker_num=wn, ordered=True)
                    for pipe in xrange(pipe_num):
                        pipe_len = randint(0, 13)   #random pipe-lenght
                        # add first
                        res = imap.add_task(adder, pipe_inp)
                        for rank in reversed(xrange(pipe_len)):
                            res = imap.add_task(adder, res)
                        ends.append(len(imap._tasks) - 1)
                        end_tasks.append(res)
                        end_tasks_len.append(pipe_len + 1)
                    imap.start()
                    for r, i, k in izip(izip(*end_tasks), pipe_inp, xrange(randint(0, len(pipe_inp)))):
                        self.assertEqual(list(r), [l + i for l in end_tasks_len])
                    imap.stop(ends=ends)

    def test_numap(self):
        assert numap
        assert numap.NuMap

    def test_init(self):
        # empty init
        nm = NuMap()
        assert nm.worker_type == 'process'
        assert nm.ordered == True
        assert nm.skip == False
        assert nm.name.startswith("numap_")
        assert nm.worker_remote == []
        assert nm.buffer == None
        self.assertRaises(ValueError, NuMap, math.log)

    def test_behaviour(self):
        nm = NuMap(math.pow, (1, 2, 3), (2,))
        #nm.next()
        #res = list(nm)
        #assert res == [1.0, 4.0, 9.0]
        #self.assertRaises(RuntimeError, nm.add_task, math.pow, (1, 2, 3), (2,))

    def test_initempty(self):
        for i in range(self.repeats):
            for wt in ('thread', 'process'):
                self.assertRaises(ValueError, NuMap, passer, [], worker_type=wt)
                empty_gen = (i for i in [])
                imap = NuMap(passer, empty_gen, worker_type=wt)
                self.assertRaises(StopIteration, imap.next)
                imap2 = NuMap(worker_type=wt)
                imap2.add_task(passer, empty_gen)
                imap2.start()
                self.assertRaises(StopIteration, imap2.next)

    def test_iterinit_eat(self):
        for i in range(self.repeats):
            for wt in ('thread', 'process'):
                ilong = iter(list(xrange(1, 100)))
                imap = NuMap(passer, ilong, worker_type=wt)
                for i in imap:
                    pass
                self.assertRaises(StopIteration, imap.next)
                imap.stop(ends=[0])
                self.assertRaises(RuntimeError, imap.next)

    def test_add_pop(self):
        for wt in ('thread', 'process'):
            imap = NuMap()
            imap.add_task(passer, [1, 2, 3])
            imap.pop_task(1)
            assert imap._tasks == []
            imap.add_task(passer, [1, 2, 3])
            imap.add_task(passer, [1, 2, 3])
            imap.pop_task(2)
            assert imap._tasks == []
            imap.add_task(passer, [1, 2, 3])
            imap.add_task(passer, [1, 2, 3])
            imap.add_task(passer, [1, 2, 3])
            imap.add_task(passer, [1, 2, 3])
            imap.pop_task(True)
            assert imap._tasks == []
            imap.add_task(passer, [1, 2, 3])
            imap.start()
            self.assertRaises(RuntimeError, imap.pop_task, 1)
            list(imap)
            imap.stop(ends=[0])

    def test_exception(self):
        for i in range(self.heavy_repeats):
            for w_num in (1, 2, 4, 8):
                for wt in ('process', 'thread'):
                    inp = [1, 'a', 3]
                    imap = NuMap(adder, inp, worker_type=wt)
                    self.assertEqual(imap.next(), 2)
                    self.assertRaises(TypeError, imap.next)
                    self.assertEqual(imap.next(), 4)
                    imap.stop(ends=[0])
                    self.assertRaises(RuntimeError, imap.next)
                    inp = [1.0, 0.0, 3.0]
                    imap = NuMap(diver, inp, worker_type=wt)
                    self.assertEqual(imap.next(), 1.0)
                    self.assertRaises(ZeroDivisionError, imap.next)
                    self.assertEqual(imap.next(), 1.0 / 3.0)
                    imap.stop(ends=[0])
                    self.assertRaises(RuntimeError, imap.next)

    def test_unsorted_sorted(self):
        for wt in ('process', 'thread'):
            for i in (1, 2, 10):
                for j in (1, 2, 10):
                    for sorted in [True, False]:
                        inp1 = [0.5, 1., 2.0, 3.0, 4.0] # will run 0.2, 0.1, 0.05 ...
                        imap = NuMap(worker_type=wt, ordered=sorted, stride=i, worker_num=j)
                        imap.add_task(waiter, inp1)
                        imap.start()
                        output = list(imap)
                        if sorted or j == 1 or i == 1:
                            self.assertEqual(inp1, output)
                        else:
                            self.assertNotEqual(inp1, output)

    def test_call_stride(self):
        for wt in ('process', 'thread'):
            for i in (1, 2, 4, 8):
                for inp in [self.medium]:
                    imap = NuMap(worker_type=wt, stride=i)
                    imap.add_task(passer, inp)
                    imap.start()
                    self.assertRaises(RuntimeError, imap.add_task, passer, inp)
                    for i in imap:
                        pass
                    self.assertRaises(StopIteration, imap.next)

    def test_call_worker_num_stride(self):
        for wt in ('process', 'thread'):
            for j in (1, 2, 4, 8, 16, 32):
                for i in (1, 2, 4, 8, 16, 32):
                    for inp in [self.short, self.medium]:
                        imap = NuMap(worker_type=wt, worker_num=i, stride=j)
                        imap.add_task(passer, inp)
                        imap.start()
                        for i in imap:
                            pass
                        self.assertRaises(RuntimeError, imap.add_task, passer, inp)
                        self.assertRaises(StopIteration, imap.next)

    def test_init_eat(self):
        for wt in ('process', 'thread'):
            for inp in [self.medium, self.long]:
                imap = NuMap(passer, inp, worker_type=wt)
                for i in imap:
                    pass
                self.assertRaises(StopIteration, imap.next)
                imap.stop(ends=[0])
                self.assertRaises(RuntimeError, imap.next)

    def test_init_add_eat(self):
        for wt in ('process', 'thread'):
            for bs in (1, 2, 3, 5, 8):
                for inp in [self.medium, self.long]:
                    imap = NuMap(worker_type=wt, stride=bs)
                    imap.add_task(passer, inp)
                    imap.start()
                    self.assertRaises(RuntimeError, imap.add_task, passer, inp)
                    for i in imap:
                        pass
                    self.assertRaises(StopIteration, imap.next)
                    imap.stop(ends=[0])
                    self.assertRaises(RuntimeError, imap.next)

    def test_start_stop(self):
        for rep in xrange(self.heavy_repeats):
            for bs in (1, 2, 3, 5, 7, 9, 22):
                for inp in [self.long]:
                    imap = NuMap(stride=bs)
                    imap.add_task(passer, inp)
                    imap.start()
                    imap.stop(ends=[0])
                    self.assertRaises(RuntimeError, imap.next)

    def test_start_stop_forced(self):
        for rep in xrange(self.heavy_repeats):
            for bs in (1, 2, 3, 5, 7, 9, 22):
                for inp in [self.long]:
                    imap = NuMap(stride=bs)
                    imap.add_task(passer, inp)
                    imap.start()
                    imap.stop(forced=True)
                    for i in imap:
                        pass
                    imap._stop()
                    self.assertRaises(RuntimeError, imap.next)

    def test_start_next_stop_forced(self):
        for rep in xrange(self.heavy_repeats):
            for bs in (1, 2, 3, 5, 7, 9, 22):
                for inp in [self.long]:
                    imap = NuMap(stride=bs)
                    imap.add_task(passer, inp)
                    imap.start()
                    imap.next()
                    imap.stop(forced=True)
                    for i in imap:
                        pass
                    imap._stop()
                    self.assertRaises(RuntimeError, imap.next)

    def test_start_next_stop(self):
        for wt in ('process', 'thread'):
            for bs in (1, 2, 3, 5, 8):
                for w_num in (1, 4):
                    for inp in [self.medium]:
                        imap = NuMap(worker_type=wt, worker_num=w_num, stride=bs)
                        imap.add_task(passer, inp)
                        imap.start()
                        imap.next()
                        imap.stop(ends=[0])
                        self.assertRaises(RuntimeError, imap.next)

    def test_2_seperate_start_stop(self):
        for wt in ('process', 'thread'):
            for bs in (1, 2, 3, 5, 8):
                for w_num in (1, 2, 4, 8):
                    inp1 = [1, 17, 23, 6, 0]
                    inp2 = [8, 4, 24, 45, 2]
                    imap = NuMap(worker_type=wt, worker_num=w_num, stride=bs)
                    imap.add_task(adder, inp1)
                    imap.add_task(miner, inp2)
                    imap.start()
                    imap.stop(ends=[0, 1])
                    self.assertRaises(RuntimeError, imap.next, task=0)
                    self.assertRaises(RuntimeError, imap.next, task=1)

    def test_2_seperate_start_init_stop(self):
        for wt in ('process', 'thread'):
            for bs in (1, 2, 3, 5, 8):
                for w_num in (1, 2, 4, 8):
                    inp1 = [1, 17, 23, 6, 0]
                    inp2 = [8, 4, 24, 45, 2]
                    imap = NuMap(worker_type=wt, worker_num=w_num, stride=bs)
                    imap.add_task(adder, inp1)
                    imap.add_task(miner, inp2)
                    imap.start()
                    imap.next(task=0)
                    imap.next(task=1)
                    imap.stop(ends=[0, 1])
                    self.assertRaises(RuntimeError, imap.next, task=0)
                    self.assertRaises(RuntimeError, imap.next, task=1)

    def test_2_chained(self):
        for wt in ('process', 'thread'):
            for inp in ([4, 17, 23, 4, 3, 2, 1], [4, 17, 23, 4, 3, 2, 1, 2]):
                for bs in (1, 2, 4, 5, 7, 8, 23):
                    for wn in (1, 2, 3, 4, 5, 6, 7, 8):
                        imap = NuMap(worker_type=wt, stride=bs, worker_num=wn)
                        out = imap.add_task(adder, inp)
                        imap.add_task(miner, out)
                        imap.start()
                        for ii in inp:
                            a = imap.next(task=1)
                            self.assertEqual(ii, a)


                        self.assertRaises(StopIteration, imap.next, task=1)
                        self.assertRaises(StopIteration, imap.next, task=0)
                        imap.stop(ends=[1])
                        self.assertRaises(RuntimeError, imap.next)
                        self.assertRaises(RuntimeError, imap.next, task=2)

    def test_2_chained_2_chained(self):
        for wt in ('process', 'thread'):
            for inp in ([4, 17, 23, 4, 3, 2, 1], [4, 17, 23, 4, 3, 2, 1, 2]):
                for bs in (1, 2, 3, 4, 7, 8):
                    for wn in (1, 2, 3, 4, 7, 8):
                        imap = NuMap(worker_type=wt, stride=bs, worker_num=wn)
                        imap.add_task(adder, inp,) # 0
                        imap.add_task(miner, imap.get_task(task=0)) # 1
                        imap.add_task(adder, inp,) # 2
                        imap.add_task(miner, imap.get_task(task=1)) # 3
                        imap.add_task(miner, imap.get_task(task=2)) # 4
                        imap.start()
                        for ii in inp:
                            self.assertEqual(ii - 1, imap.next(task=3))
                            self.assertEqual(ii, imap.next(task=4))
                        self.assertRaises(StopIteration, imap.next, task=4)
                        self.assertRaises(StopIteration, imap.next, task=3)
                        self.assertRaises(StopIteration, imap.next, task=2)
                        self.assertRaises(StopIteration, imap.next, task=1)
                        self.assertRaises(StopIteration, imap.next, task=0)
                        imap.stop(ends=[3, 4])
                        self.assertRaises(RuntimeError, imap.next)

    def test_4_chained(self):
        for bf in (0, 1, 2, 3, 4, 5):
            for wt in ('process', 'thread'):
                inp = [1, 17, 23, 6, 0]
                imap = NuMap(worker_type=wt, stride=4, buffer=4 + bf, worker_num=4)
                imap.add_task(adder, inp)
                imap.add_task(miner, imap.get_task(0))
                imap.add_task(adder, imap.get_task(1))
                imap.add_task(miner, imap.get_task(2))
                imap.start()
                for i, j in izip(imap.get_task(task=3), inp):
                    self.assertEqual(j, i)
                self.assertRaises(StopIteration, imap.next, task=0)
                self.assertRaises(StopIteration, imap.next, task=1)
                self.assertRaises(StopIteration, imap.next, task=2)
                self.assertRaises(StopIteration, imap.next, task=3)
                imap.stop(ends=[3])
                self.assertRaises(RuntimeError, imap.next)

    def test_3_zip_1(self):
        for wt in ('process', 'thread'):
            for st in (1, 2, 3, 7, 8, 9):
                inp1 = range(0, 100)
                inp2 = range(200, 300)
                inp3 = range(300, 400)
                min_buf = st * 3
                imap = NuMap(worker_type=wt, stride=st, buffer=min_buf, worker_num=2)
                res1 = imap.add_task(adder, inp1)
                res2 = imap.add_task(adder, inp2)
                res3 = imap.add_task(adder, inp3)
                res4 = imap.add_task(passer, izip(res1, res2, res3))
                imap.start()
                for i, j in izip(res4, izip(inp1, inp2, inp3)):
                    self.assertEqual((i[0] - 1, i[1] - 1, i[2] - 1), (j[0], j[1], j[2]))
                self.assertRaises(StopIteration, imap.next, task=0)
                self.assertRaises(StopIteration, imap.next, task=1)
                self.assertRaises(StopIteration, imap.next, task=2)

    def test_2_seperate_get_task(self):
        for wt in ('process', 'thread'):
            for w_num in [1, 2, 4, 8]:
                for func in [zip, izip]:
                    ii = [1, 17, 23, 6, 0]
                    jj = [8, 4, 24, 45, 2]
                    imap = NuMap(worker_type=wt, worker_num=w_num)
                    imap.add_task(adder, ii)
                    imap.add_task(miner, jj)
                    imap.start()
                    iter1 = imap.get_task(task=0)
                    iter2 = imap.get_task(task=1)
                    for i, j, iii, jjj in func(iter1, iter2, ii, jj):
                        self.assertEqual(iii + 1, i)
                        self.assertEqual(jjj - 1, j)
                    imap.stop(ends=[0, 1])
                    self.assertRaises(RuntimeError, imap.next, task=0)
                    self.assertRaises(RuntimeError, imap.next, task=1)

    def test_2_seperate(self):
        for k in range(self.repeats):
            for wt in ('process', 'thread'):
                for j in (1, 2, 3, 4, 5, 8, 16, 32):
                    for i in (1, 2, 3, 4, 5, 8, 16, 32):
                        inp1 = [1, 17, 23, 6, 0]
                        inp2 = [8, 4, 24, 45, 2]
                        imap = NuMap(worker_type=wt, stride=i, worker_num=j)
                        imap.add_task(adder, inp1)
                        imap.add_task(miner, inp2)
                        imap.start()
                        for ii, jj in zip(inp1, inp2):
                            self.assertEqual(ii + 1, imap.next(task=0))
                            self.assertEqual(jj - 1, imap.next(task=1))




    def test_iterinit_stop(self):
        for i in range(self.repeats):
            for wt in ('thread', 'process'):
                ilong = iter(list(xrange(1, 100)))
                imap = NuMap(passer, ilong, worker_type=wt, stride=17)
                for i in [1, 2, 3]:
                    imap.next(task=0)
                imap.stop(ends=[0])
                self.assertRaises(RuntimeError, imap.next)

    def test_multistop(self):
        for i in range(self.heavy_repeats):
            for w_num in (1, 2, 4, 8):
                for wt in ('thread', 'process'):
                    ilong = iter(list(xrange(1, 3)))
                    imap = NuMap(passer, ilong, worker_type=wt, worker_num=w_num)
                    imap.next()
                    imap.next()
                    self.assertRaises(StopIteration, imap.next)
                    self.assertRaises(StopIteration, imap.next)
                    self.assertRaises(StopIteration, imap.next)
                    imap.stop(ends=[0])
                    self.assertRaises(RuntimeError, imap.next)

    def test_start_init_stop_start_init(self):
        for wn in (1, 2, 4, 8):
            for sl in (0.0, 0.01, 0.1):
                for wt in ('thread', 'process'):
                    inp = [17, 19, 21, 25, 27, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
                    imap = NuMap(worker_type=wt, worker_num=wn)
                    imap.add_task(adder, inp)
                    imap.start()
                    time.sleep(sl)
                    self.assertAlmostEqual(imap.next(), 18)
                    imap.stop([0])
                    imap.add_task(adder, inp)
                    imap.start()
                    time.sleep(sl)
                    self.assertAlmostEqual(imap.next(), 18)
                    imap.stop([0])

    def test_timeout_2_chained_moreandmore(self):
        ## 2 chain unorodered timeout and skipping and 3 workers
        for wt in ('process', 'thread'):
            wait = [1.0, 0.5, 0.25]
            imap = NuMap(worker_type=wt, stride=3, worker_num=3, buffer=3, ordered=False, skip=True)
            imap.add_task(real_waiter, wait)
            imap.add_task(real_waiter, imap)
            imap.start()
            start = time.time()
            assert imap.next(task=1) == 0.25
            assert 0.5 < time.time() - start < 0.6
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.25)
            assert imap.next(task=1) == 1.0
            assert 1.9 < time.time() - start < 2.1
            self.assertRaises(StopIteration, imap.next, task=1)

    def test_timeout_2_chained(self):
        for wt in ('process', 'thread'):
            wait = [1.0, 0.5, 0.25]
            imap = NuMap(worker_type=wt, stride=1, worker_num=1, ordered=True)
            imap.add_task(real_waiter, wait)
            imap.add_task(real_waiter, imap)
            imap.start()
            start = time.time()
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=1.5)
            self.assertEqual(imap.next(task=1), 1.0)
            self.assertEqual(imap.next(task=1), 0.5)
            stop = time.time()
            self.assertTrue(2.9 < stop - start < 3.1)
            imap.stop(ends=[1])
            self.assertRaises(RuntimeError, imap.next, task=1)

    def test_timeout_1_seperate_input_unordered(self):
        for wt in ('process', 'thread'):
            wait = [3, 2, 0.5]
            imap = NuMap(worker_type=wt, stride=2, worker_num=2, ordered=False)
            imap.add_task(real_waiter, wait)
            imap.start()
            start = time.time()
            self.assertRaises(TimeoutError, imap.next, timeout=1.5)
            self.assertEqual(imap.next(timeout=1), 2)
            self.assertEqual(imap.next(timeout=1), 0.5)
            assert imap.next() == 3
            t = time.time() - start
            self.assertTrue(2.9 < t < 3.1)

    def test_timeout_1_seperate_input_ordered(self):
        for wt in ('process', 'thread'):
            wait = [3, 2, 0.5]
            imap = NuMap(worker_type=wt, stride=2, worker_num=2, ordered=True)
            imap.add_task(real_waiter, wait)
            imap.start()
            start = time.time()
            self.assertRaises(TimeoutError, imap.next, timeout=1.5)
            self.assertEqual(imap.next(timeout=2), 3)
            self.assertEqual(imap.next(timeout=0.01), 2)
            assert imap.next() == 0.5
            t = time.time() - start
            self.assertTrue(3.4 < t < 3.6)

    def test_timeout_2_seperate_skip_11(self):
        ##2 ordered True, skip True
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=1, ordered=True, skip=True)
            wait = [0.500, 0.300, 0.500, 0.500, 0.100]
            wait2 = [0.500, 0.300, 0.500, 0.500, 0.100]
            imap.add_task(real_waiter, wait)
            imap.add_task(real_waiter, wait2)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100) # skip0-0
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.100) # skip1-0
            self.assertEqual(imap.next(task=0), 0.300) # get 1
            self.assertEqual(imap.next(task=1), 0.300)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.001) #skip 2
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.001)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.001) # skip 3
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.001)
            self.assertEqual(imap.next(task=0), 0.100)
            self.assertEqual(imap.next(task=1), 0.100)
            imap.stop(ends=[0, 1])

    def test_timeout_1_skip_23(self):
        ## ordered True, skip True
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=1, ordered=True, skip=True)
            wait = [0.500, 0.300, 0.500, 0.500, 0.100]
            imap.add_task(real_waiter, wait)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100) # skip 0
            self.assertEqual(imap.next(), 0.300) # get 1
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100) # skip 2
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100) # skip 3
            self.assertEqual(imap.next(), 0.100) # get 4
            self.assertRaises(StopIteration, imap.next)
            imap.stop(ends=[0])

    def test_timeout_2_skip_1(self):
        ##2 ordered True, skip False
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=1, ordered=True, skip=False)
            wait = [0.500, 0.300, 0.500, 0.001]
            wait2 = [0.500, 0.300, 0.500, 0.001]
            imap.add_task(real_waiter, wait)
            imap.add_task(real_waiter, wait2)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.100)
            self.assertEqual(imap.next(task=0), 0.500)
            self.assertEqual(imap.next(task=1), 0.500)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.001)
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.001)
            self.assertEqual(imap.next(task=0), 0.300)
            self.assertEqual(imap.next(task=1), 0.300)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.100)
            self.assertEqual(imap.next(task=0), 0.500)
            self.assertEqual(imap.next(task=1), 0.500)
            imap.stop(ends=[0, 1])

    def test_timeout_1(self):
        ##1 ordered True, skip False
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=1, ordered=True, skip=False)
            wait = [0.500, 0.300, 0.500, 0.001]
            imap.add_task(real_waiter, wait)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertEqual(imap.next(), 0.500)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.001)
            self.assertEqual(imap.next(), 0.300)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertEqual(imap.next(), 0.500)
            self.assertEqual(imap.next(), 0.001)
            imap.stop(ends=[0])

    def test_timeout_2(self):
        ##1 ordered True, skip False
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=2, ordered=True, skip=False)
            wait = [0.500, 0.300, 0.500, 0.001]
            imap.add_task(real_waiter, wait)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertEqual(imap.next(), 0.500)
            self.assertEqual(imap.next(timeout=0.01), 0.300) # should be there
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100)
            self.assertEqual(imap.next(), 0.500)
            self.assertEqual(imap.next(), 0.001)
            imap.stop(ends=[0])

    def test_timeout_2_chain(self):
        ###1 ordered True, skip False, chained
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=1, ordered=True, skip=False)
            wait = [0.500, 0.300, 0.400, 0.001]
            imap.add_task(real_waiter, wait)
            imap.add_task(real_waiter, imap)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.750)
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.100)
            self.assertEqual(imap.next(task=1), 0.500)
            imap.stop(ends=[1])

    def test_timeout_2_chain_skip(self):
        ##1 ordered True, skip False, chained
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=1, ordered=True, skip=True)
            wait = [0.500, 0.300, 0.400, 0.001]
            imap.add_task(real_waiter, wait)
            imap.add_task(real_waiter, imap)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.750)
            self.assertRaises(TimeoutError, imap.next, task=1, timeout=0.100)
            self.assertEqual(imap.next(task=1), 0.400)
            imap.stop(ends=[1])

    def test_timeout_1_skip_2(self):
        ##1 ordered True, skip true
        for wt in ('process', 'thread'):
            imap = NuMap(worker_type=wt, worker_num=2, ordered=True, skip=True)
            wait = [0.500, 0.300, 0.400]
            imap.add_task(real_waiter, wait)
            imap.start()
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100) # skip 0
            self.assertRaises(TimeoutError, imap.next, task=0, timeout=0.100) # skip 1
            self.assertEqual(imap.next(), 0.400)
            imap.stop(ends=[0])



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
