# -*- coding: utf-8 -*-
"""
tests for ``papy.util.codefile``.
"""
import unittest
import gc
from logging import Logger
from exceptions import StopIteration
import papy.util.func
import papy.core as pc
from papy.core import *
from papy.core import _comp_task, _Produce, _Repeat, _Consume, _Chain
from math import sqrt, pow
from numap import NuMap
from time import sleep
from random import randint

def power2(i):
    return i[0] * i[0]

def power2_make2(i):
    try:
        return (i[0] * i[0],) * 2
    except TypeError, e:
        we = WorkerError(e)
        return (we,) * 2

def sleeper(i):
    sleep(i[0])
    return i

def power(i, n):
    return pow(i[0], n)

def mul2(i):
    return 2 * i[0]

def sqrt2(i):
    return sqrt(i[0])

def mul_m_min_n(inbox, m, n):
    return inbox[0] * m - n

def join_sum2(i):
    return i[0][0] + i[1][0]

def sum2(i):
    a, b = i
    return a[0] + b[0]

def sumof2(i):
    return i[0] + i[1]

def sumof3(i):
    return i[0] + i[1] + i[2]

def sum_of_sqr(i):
    res = sum([sqrt(j[0]) for j in i])
    return res

class test_Worker(unittest.TestCase):

    def test_single(self):
        pwr = Worker(power2)
        res = pwr([2])
        assert res == 4

    def test_chain1(self):
        pwr = Worker((power2,))
        res = pwr([2])
        assert res == 4

    def test_chain2(self):
        m2p2 = Worker((power2, mul2))
        p2m2 = Worker((mul2, power2))
        p2m2_r = p2m2([3])
        assert p2m2_r == (2 * 3) * (2 * 3)
        m2p2_r = m2p2([3])
        assert m2p2_r == (3 * 3) * 2

    def test_nested2(self):
        p2 = Worker(power2)
        m2 = Worker(mul2)
        m2p2 = p2([m2([3])])
        assert m2p2 == (2 * 3) * (2 * 3)
        p2m2 = m2([p2([3])])
        assert p2m2 == (3 * 3) * 2

    def test_init(self):
        p2_1 = Worker(power2)
        p2_2 = Worker(p2_1)
        p2_3 = Worker([power2])
        assert p2_1 == p2_2 == p2_3
        assert p2_1 is not p2_2
        p2_1 = Worker(power, (3,))
        p2_2 = Worker(p2_1)
        assert p2_1 == p2_2
        assert p2_1 is not p2_2
        assert p2_1([3]) == p2_2([3]) == 27


    def test_args(self):
        p2 = Worker(power, (2,))
        assert p2([2]) == 4
        p1 = Worker(power, (1,))
        assert p1([2]) == 2
        p3p2 = Worker((power, power), ((2,), (3,)))
        assert p3p2([2]) == (2 * 2 * 2) * (2 * 2 * 2)

    def test_kwargs(self):
        p3 = Worker(power, (), {'n':3})
        assert p3([2]) == 8
        p3p2 = Worker((power, power), ((), ()), ({'n':2}, {'n':3}))
        assert p3p2([2]) == (2 * 2 * 2) * (2 * 2 * 2)
        m3n2 = Worker(mul_m_min_n, (), {'m':3, 'n':3})
        assert m3n2([4]) == 9

    def test_callability(self):
        numap = NuMap()
        p2 = Worker(power2)
        numap.add_task(p2, [[1], [2]])
        numap.start()
        assert list(numap) == [1, 4]
        p3p2 = Worker((power, power), ((), ()), ({'n':2}, {'n':3}))
        numap = NuMap()
        numap.add_task(p3p2, [[1], [2]])
        numap.start()
        assert list(numap) == [1, 64]

    def test_hash(self):
        p2_1 = Worker(power2)
        self.assertRaises(TypeError, hash, p2_1)

    def test_equality(self):
        p2_1 = Worker(power2)
        p2_2 = Worker(power2)
        m2_1 = Worker(mul2)
        assert p2_1 == p2_2
        assert p2_1 != 'abcd'
        assert p2_1 != power2
        assert p2_1 is not p2_2
        assert p2_1 != m2_1
        p2m2_1 = Worker([power2, mul2])
        p2m2_2 = Worker([power2, mul2])
        m2p2_1 = Worker([mul2, power2])
        assert p2m2_1 == p2m2_2
        assert p2m2_2 != m2p2_1
        assert p2m2_1 is not p2m2_2

    def test_exceptions(self):
        m2 = Worker(mul2)
        excp = m2([None])
        assert isinstance(excp, WorkerError)
        excp = m2([PiperError('')])
        assert isinstance(excp, PiperError)

        self.assertRaises(TypeError, Worker, 1)
        self.assertRaises(TypeError, Worker, [1])


class test_Piper(unittest.TestCase):

    def test_init(self):
        piper = Piper(power2)
        assert piper.tees == []
        assert piper.tee_locks[0].locked() is False
        assert len(piper.tee_locks) == 1
        assert piper.started is False
        assert piper.connected is False
        assert piper.finished is False
        assert piper.imap is imap

    def test_sort(self):
        p2 = Piper(papy.util.func.ipasser, branch=2)
        p1 = Piper(papy.util.func.ipasser, branch=1)
        a = [p1, p2]
        dgr = Dagger()
        dgr.add_piper(p1)
        dgr.add_piper(p2)
        a.sort(cmp=dgr.cmp_branch)
        assert a == [p2, p1]

    def test_logger(self):
        pwr = Piper(power)
        self.assertTrue(isinstance(pwr.log, Logger))

    def test_equality(self):
        p2_1 = Piper(power2)
        p2_2 = Piper(power2)
        self.assertNotEqual(p2_1, p2_2)
        self.assertEqual(p2_1.worker, p2_2.worker)

    def test_pool(self):
        p2 = Piper(power2)
        m2 = Piper(mul2)
        assert p2 is not m2
        assert p2.imap is imap
        assert p2.imap is m2.imap
        pool1 = NuMap()
        pool2 = NuMap()
        p2 = Piper(power2, parallel=pool1)
        m2 = Piper(mul2, parallel=pool2)
        assert p2.imap is not m2.imap
        assert p2.imap is not imap
        assert isinstance(m2.imap, NuMap)

    def test_exceptions(self):
        for par in (None, NuMap()):
            piper = Piper(power2, parallel=par)
            self.assertRaises(PiperError, piper.start)
            self.assertRaises(PiperError, piper.stop)
            piper.connect([[1, 2, 3]])
            self.assertRaises(PiperError, piper.connect, [[1, 2, 3, 4]])
            self.assertRaises(PiperError, piper.stop)
            piper.start((0,)) # stage 0 start
            if par is not None:
                assert piper.started is False
                self.assertRaises(PiperError, piper.next)
                self.assertRaises(PiperError, piper.stop)
                assert piper.started is False
                piper.start((1,))
                self.assertRaises(PiperError, piper.stop)
                assert piper.imap._started.isSet() is True
                assert piper.started is False
                piper.start((2,))
                assert piper.started is True
                assert piper.finished is False
                assert list(piper) == [1, 4, 9]
            else:
                assert piper.finished is False
                assert list(piper) == [1, 4, 9]

    def test_call(self):
        pool = NuMap()
        for i in range(100):
            ppr_instance = Piper(power2, parallel=pool)
            ppr_busy = ppr_instance([[1, 2, 3, 4]])
            assert ppr_instance is ppr_busy
            self.assertRaises(PiperError, ppr_busy.next)
            ppr_busy.start(stages=(0,))
            self.assertRaises(PiperError, ppr_busy.next)
            ppr_busy.start(stages=(1, 2))
            assert ppr_busy.started is True
            assert ppr_busy.imap._started.isSet()
            for i in izip(ppr_busy, [1, 2, 3, 4]):
                self.assertEqual(i[0], i[1] * i[1])
            self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
            self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
            assert ppr_busy.imap._started.isSet()
            ppr_busy.stop(ends=[0])
            assert ppr_busy.started is False
            assert ppr_busy.finished is True
            assert ppr_busy.connected is True
            ppr_busy.disconnect()
            assert ppr_busy.imap._tasks == []
            assert ppr_busy.connected == False
            assert not ppr_busy.imap._started.isSet()
            self.assertRaises(PiperError, ppr_busy.next)

        ppr_instance = Piper(power)
        ppr_busy = ppr_instance([[1, 2, 3, 4]])
        assert ppr_instance is ppr_busy
        assert ppr_busy.started == False
        assert ppr_busy.connected == True
        assert ppr_busy.finished == False
        ppr_busy.start()
        assert ppr_busy.started == True
        for i in ppr_busy:
            pass
        ppr_busy.stop()
        assert ppr_busy.started == False
        assert ppr_busy.connected == True
        self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
        self.assertRaises(StopIteration, ppr_busy.next) # it. protocol
        ppr_busy.disconnect()
        assert ppr_busy.connected == False
        assert ppr_busy.finished == True
        self.assertRaises(PiperError, ppr_busy.next) # it. protocol
        self.assertRaises(PiperError, ppr_busy.next) # it. protocol

    def test_connect(self):
        pool = NuMap()
        for i in range(100):
            ppr_instance = Piper(power2, parallel=pool)
            ppr_busy = ppr_instance([[7, 2, 3]])
            assert ppr_instance is ppr_busy
            self.assertRaises(PiperError, ppr_busy, [[7, 2, 3]]) # second connect
            self.assertRaises(PiperError, ppr_busy.next)      # not started
            ppr_busy.disconnect()
            self.assertRaises(PiperError, ppr_busy.start)
            assert not ppr_busy.imap._tasks
            ppr_busy.connect([[1, 2, 3]])
            ppr_busy.start(stages=(0, 1, 2))
            assert ppr_busy.next() == 1
            ppr_busy.stop(ends=[0], forced=True) # not finished
            self.assertRaises(RuntimeError, ppr_busy.imap.next)
            assert not pool._started.isSet()
            assert not hasattr(pool, 'pool')

    def test_connect2(self):
        pool = NuMap()
        ppr_1 = Piper(power2, parallel=pool)
        ppr_2 = Piper(mul2, parallel=pool)
        ppr_1busy = ppr_1([[7, 2, 3]])
        ppr_2busy = ppr_2([[7, 2, 3]])
        assert (ppr_1busy, ppr_2busy) == (ppr_1, ppr_2)
        self.assertRaises(PiperError, ppr_1busy, [[7, 2, 3]]) # second connect
        self.assertRaises(PiperError, ppr_1busy, [[7, 2, 3]]) # second connect
        self.assertRaises(PiperError, ppr_1busy.next)     # not started
        self.assertRaises(PiperError, ppr_2busy.next)     # not started
        self.assertRaises(PiperError, ppr_1busy.disconnect) # not last

        ppr_2busy.disconnect()
        ppr_1busy.disconnect()
        self.assertRaises(PiperError, ppr_1busy.start)
        self.assertRaises(PiperError, ppr_2busy.start)
        assert pool._tasks == []
        ppr_2busy.connect([[7, 2, 3]])
        ppr_1busy.connect([[1, 1, 1]])
        ppr_1busy.start(stages=(0, 1))
        ppr_2busy.start(stages=(0, 1))
        ppr_1busy.start(stages=(2,))
        ppr_2busy.start(stages=(2,))
        assert ppr_1busy.next() == 1
        assert ppr_2busy.next() == 14
        ppr_1busy.stop(ends=[0, 1], forced=True)
        ppr_2busy.stop(ends=[0, 1], forced=True)
        self.assertRaises(RuntimeError, ppr_1busy.imap.next)
        self.assertRaises(RuntimeError, ppr_2busy.imap.next)

    def test_connect3(self):
        for i, j in ((None, None), (NuMap(), NuMap())):
            passer = Piper(papy.util.func.ipasser, parallel=i)
            passer([[1]])
            passer.start(stages=(0, 1, 2))
            passer.next()
            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            passer.stop(ends=[0])
            assert passer.finished is True
            assert passer.started is False
            passer = Piper(papy.util.func.ipasser, parallel=j)
            passer([[]])
            assert passer.connected is True
            assert passer.started is False
            passer.start(stages=(0, 1, 2))
            assert passer.started is True
            assert passer.connected is True
            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            self.assertRaises(StopIteration, passer.next)
            passer.stop(ends=[0])
            assert passer.started is False
            assert passer.connected is True
            assert passer.finished is True
            passer.disconnect()
            assert passer.connected is False

    def test_connect4(self):
        for inp in ([[1]],): # [[]]):      
            imap = NuMap()
            for i, j in ((None, None), (imap, imap), (NuMap(), NuMap()), \
                         (None, NuMap()), (NuMap(), None)):
                passer1 = Piper(papy.util.func.ipasser, parallel=i)
                passer2 = Piper(papy.util.func.ipasser, parallel=j)
                passer1(inp)
                passer2([passer1])
                passer1.start(stages=(0, 1))
                passer2.start(stages=(0, 1))
                passer1.start(stages=(2,))
                passer2.start(stages=(2,))
                passer2.next()
                self.assertRaises(StopIteration, passer2.next)
                passer2.stop(ends=[0])

    def test_usage1(self):
        for par in (False, NuMap()):
            gen10 = (i for i in xrange(1, 10))
            gen15 = (i for i in xrange(1, 15))
            ppr = Piper(power2, parallel=par)
            ppr = ppr([gen15])
            ppr.start(stages=(0, 1, 2))
            for i, j in izip(ppr, xrange(1, 15)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])
            ppr = Piper(power2, parallel=par)
            ppr = ppr([gen10])
            ppr.start(stages=(0, 1, 2))
            for i, j in izip(ppr, xrange(1, 10)):
                self.assertEqual(i, j * j)

    def test_usage2(self):
        for par in (False, NuMap()):
            # imap reuse!
            gen10 = (i for i in xrange(1, 10))
            gen15 = (i for i in xrange(1, 15))
            gen20 = (i for i in xrange(1, 20))
            ppr = Piper(power2, parallel=par)
            ppr = ppr([gen10])
            ppr.start(stages=(0, 1, 2))
            for i, j in izip(ppr, xrange(1, 20)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])
            ppr = Piper([power2], parallel=par)
            ppr = ppr([gen15])
            ppr.start(stages=(0, 1, 2))
            for i, j in izip(ppr, xrange(1, 15)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])
            pwr = Worker(power2)
            ppr = Piper(pwr, parallel=par)
            ppr = ppr([gen20])
            ppr.start(stages=(0, 1, 2))
            for i, j in izip(ppr, xrange(1, 20)):
                self.assertEqual(i, j * j)
            ppr.stop(ends=[0])

    def test_usage3(self):
        for par in (False, NuMap()):
            gen10 = (i for i in xrange(1, 10))
            gen15 = (i for i in xrange(1, 15))
            gen20 = (i for i in xrange(1, 20))
            ppr = Piper([power2, mul2], parallel=par)
            ppr = ppr([gen20])
            ppr.start()
            for i, j in izip(ppr, xrange(1, 20)):
                self.assertEqual(i, 2 * j * j)
            ppr.stop(ends=[0])
            pwrdbl = Worker([power2, mul2])
            ppr = Piper(pwrdbl, parallel=par)
            ppr = ppr([gen15])
            ppr.start()
            for i, j in izip(ppr, xrange(1, 15)):
                self.assertEqual(i, 2 * j * j)
            ppr.stop(ends=[0])
            dblpwr = Worker([mul2, power2])
            ppr = Piper(dblpwr, parallel=par)
            ppr = ppr([gen10])
            ppr.start()
            for i, j in izip(ppr, xrange(1, 10)):
                self.assertEqual(i, (2 * j) * (2 * j))
            ppr.stop(ends=[0])

    def test_linked1(self):
        for par in (False, NuMap()):
            gen10 = (i for i in xrange(10))
            pwr = Piper(power2, parallel=par)
            dbl = Piper(mul2, parallel=par)
            dbl([gen10])
            ppr = pwr([dbl])
            dbl.start(stages=(0, 1))
            ppr.start(stages=(0, 1))
            dbl.start(stages=(2,))
            ppr.start(stages=(2,))
            for i, j in izip(ppr, (i for i in xrange(10))):
                self.assertEqual(i, (2 * j) * (2 * j))
            ppr.stop(ends=[1])

    def test_linked2(self):
        for par in (False, NuMap()):
            gen10 = (i for i in xrange(10))
            pwr = Piper(power2, parallel=par)
            dbl = Piper(mul2, parallel=par)
            ppr = pwr([dbl([gen10])])
            dbl.start(stages=(0, 1))
            ppr.start(stages=(0, 1))
            dbl.start(stages=(2,))
            ppr.start(stages=(2,))
            for i, j in izip(ppr, (i for i in xrange(10))):
                self.assertEqual(i, (2 * j) * (2 * j))
            ppr.stop(ends=[1])

    def test_linear(self):
        for par in (False, NuMap()):
            gen20 = (i for i in xrange(1, 20))
            piper = Piper(power2, parallel=par)
            piper([gen20])
            piper.start()
            for i, j in izip(piper, xrange(1, 20)):
                self.assertEqual(i, j * j)
            self.assertRaises(StopIteration, piper.next)
            piper.stop(ends=[0])

    def test_2_running(self):
        p = NuMap()
        for pool1, pool2 in ((NuMap(), NuMap()),
                             (p, p),
                             (NuMap(), None),
                             (None, NuMap())):
            data = [1, 2, 3]
            piper1 = Piper(papy.util.func.ipasser, parallel=pool1)
            piper2 = Piper(papy.util.func.ipasser, parallel=pool2)
            piper1.connect([data])
            piper2.connect([piper1])
            piper1.start(stages=(0, 1))
            piper2.start(stages=(0, 1))
            piper1.start(stages=(2,))
            piper2.start(stages=(2,))
            assert list(piper2) == [1, 2, 3]

    def _test_fork_join(self):
        for stride in (1, 2, 3, 4, 5):
            for r in range(13):
                after1 = NuMap(stride=stride)
                after2 = NuMap(stride=stride)
                after3 = NuMap(stride=stride)
                before1 = NuMap(stride=stride)
                before2 = NuMap(stride=stride)

                combinations = [(None, None, None, None),
                                (NuMap(stride=stride), NuMap(stride=stride), NuMap(stride=stride), NuMap(stride=stride)),
                                (NuMap(stride=stride), None, None, NuMap(stride=stride)),
                                (None, NuMap(stride=stride), NuMap(stride=stride), NuMap(stride=stride)),
                                (None, after1, after1, after1),
                                (None, after2, after2, NuMap(stride=stride)),
                                (NuMap(stride=stride), after3, after3, after3),
                                (before1, before1, before1, before1),
                                (before2, before2, NuMap(stride=stride), before2)
                                ]
                for pool1, pool2, pool3, pool4 in combinations:
                    data = range(r)
                    piper1 = Piper(papy.util.func.ipasser, parallel=pool1)
                    piper2 = Piper(papy.util.func.ipasser, parallel=pool2)
                    piper3 = Piper(papy.util.func.ipasser, parallel=pool3)
                    piper4 = Piper(papy.util.func.npasser, parallel=pool4)
                    piper1.connect([data])
                    piper2.connect([piper1])
                    piper3.connect([piper1])
                    piper4.connect([piper2, piper3])
                    for piper in (piper1, piper2, piper3, piper4):
                        piper.start(stages=(0, 1))
                    #print 'started 1,',
                    for piper in (piper1, piper2, piper3, piper4):
                        piper.start(stages=(2,))
                    #print '2',
                    res = list(piper4)
                    #print 'finished',
                    assert res == [list(i) for i in zip(range(r), range(r))]
                    for p in (pool1, pool2, pool3, pool4):
                        try:
                            p._pool_getter.join()
                            p._pool_putter.join()
                            for pp in p.pool:
                                pp.join()
                        except AttributeError:
                            pass
                gc.collect()

    def _test_fork_3(self):
        for stride in (1, 2, 3, 4, 5):
            for r in range(24):
                after1 = NuMap(stride=stride)
                after2 = NuMap(stride=stride)
                before1 = NuMap(stride=stride)
                before2 = NuMap(stride=stride)

                combinations = [(None, None, None),
                                (NuMap(), NuMap(), NuMap()),
                                (NuMap(), None, None),
                                (None, NuMap(), NuMap()),
                                (None, after1, after1),
                                (NuMap(), after2, after2),
                                (before1, before1, before1)
                                ]
                for pool1, pool2, pool3 in combinations:
                    print '.',
                    data = range(r)
                    if hasattr(pool3, 'stride'):
                        stride = pool3.stride
                    else:
                        stride = 1
                    piper1 = Piper(papy.util.func.ipasser, parallel=pool1)
                    piper2 = Piper(papy.util.func.ipasser, parallel=pool2)
                    piper3 = Piper(papy.util.func.ipasser, parallel=pool3)
                    piper1.connect([data])
                    piper2.connect([piper1])
                    piper3.connect([piper1])
                    piper1.start(stages=(0, 1))
                    piper2.start(stages=(0, 1))
                    piper3.start(stages=(0, 1))
                    piper1.start(stages=(2,))
                    piper2.start(stages=(2,))
                    piper3.start(stages=(2,))
                    a = []
                    for result in range((r / stride) + stride):
                        for s in range(stride):
                            try:
                                piper2.next()
                            except Exception, excp:
                                pass
                        for s in range(stride):
                            try:
                                piper3.next()
                            except:
                                pass
                    assert piper1.finished
                    assert piper2.finished
                    assert piper3.finished
                    for p in (pool1, pool2, pool3):
                        try:
                            p._pool_getter.join()
                            p._pool_putter.join()
                            for pp in p.pool:
                                pp.join()
                        except AttributeError:
                            pass
                gc.collect()

    def test_function_exceptions(self):
        pwr = Piper(power2)
        pwr = pwr([[1, 'a', 3]])
        pwr.start() # should not raise even if not needed
        self.assertEqual(pwr.next(), 1)
        self.assertTrue(isinstance(pwr.next(), PiperError))
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)
        pwr = Piper(power2, debug=True)
        pwr = pwr([[1, 'a', 3]])
        pwr.start()
        self.assertEqual(pwr.next(), 1)
        self.assertRaises(PiperError, pwr.next)
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)
        pool = NuMap()
        pwr = Piper(power2, parallel=pool)
        pwr = pwr([[1, 'a', 3]])
        pwr.start(stages=(0, 1, 2)) # should work
        self.assertEqual(pwr.next(), 1)
        self.assertTrue(isinstance(pwr.next(), PiperError))
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)
        pwr = Piper(power2, debug=True)
        pwr = pwr([[1, 'a', 3]])
        pwr.start()
        self.assertEqual(pwr.next(), 1)
        self.assertRaises(PiperError, pwr.next)
        self.assertEqual(pwr.next(), 9)
        self.assertRaises(StopIteration, pwr.next)

    def test_internal_failure_chain(self):
        pwr = Piper(power2)
        dbl = Piper(mul2)
        pwr = pwr([[1, 'a', 3]])
        dbl = dbl([pwr])
        pwr.start()
        dbl.start()
        self.assertEqual(dbl.next(), 2)
        a = dbl.next()
        self.assertTrue(isinstance(a, PiperError))  # this is what dbl return (it wrapped what it got)
        self.assertTrue(isinstance(a[0], PiperError))       # wrapped in the workers piper
        self.assertTrue(isinstance(a[0][0], WorkerError))   # wrapped in the worker
        self.assertTrue(isinstance(a[0][0][0], TypeError))  # raised in the worker
        self.assertEqual(dbl.next(), 18)
        self.assertRaises(StopIteration, dbl.next)

        pool = NuMap()
        pwr = Piper(power2, parallel=pool)
        dbl = Piper(mul2, parallel=pool)
        pwr = pwr([[1, 'a', 3]])
        dbl = dbl([pwr])
        pwr.start(stages=(0, 1))
        dbl.start(stages=(0, 1))
        pwr.start(stages=(2,))
        dbl.start(stages=(2,))
        self.assertEqual(dbl.next(), 2)
        a = dbl.next()
        self.assertTrue(isinstance(a, PiperError))  # this is what dbl return (it wrapped what it got)
        self.assertTrue(isinstance(a[0], PiperError))       # wrapped in the workers piper
        self.assertTrue(isinstance(a[0][0], WorkerError))   # wrapped in the worker
        self.assertTrue(isinstance(a[0][0][0], TypeError))  # raised in the worker
        self.assertEqual(dbl.next(), 18)
        self.assertRaises(StopIteration, dbl.next)
        self.assertRaises(StopIteration, pwr.next)
        dbl.stop(ends=[1])
        self.assertRaises(PiperError, pwr.next)


    def test_2_stopping(self):
        p = NuMap()
        for pool1, pool2 in ((NuMap(), NuMap()),
                             (p, p),
                             (NuMap(), None),
                             (None, NuMap())):

            data = [1, 2, 3]
            piper1 = Piper(papy.util.func.ipasser, parallel=pool1)
            piper2 = Piper(papy.util.func.ipasser, parallel=pool2)
            piper1.connect([data])
            piper2.connect([piper1])
            assert piper1.connected is True
            assert piper2.connected is True
            piper1.start(stages=(0, 1))
            piper2.start(stages=(0, 1))
            piper1.start(stages=(2,))
            piper2.start(stages=(2,))

            assert piper1.started is True
            assert piper2.started is True
            assert list(piper2) == [1, 2, 3]
            assert piper1.finished is True
            assert piper2.finished is True
            assert piper1.started is True
            assert piper2.started is True

            try:
                piper1.stop(ends=[len(piper1.imap._tasks) - 1])
            except AttributeError:
                piper1.stop()
            try:
                piper2.stop(ends=[len(piper2.imap._tasks) - 1])
            except AttributeError:
                piper2.stop()

            assert piper1.started is False
            assert piper2.started is False
            if not pool1 is None:
                assert not pool1._started.isSet()
            if not pool2 is None:
                assert not pool2._started.isSet()

#    def xtestdump_items(self):
#        for typ in ('tcp', 'udp'):
#            for typ2 in ('string',):
#                imap1 = IMap()
#                imap2 = IMap()
#                imap3 = IMap()
#                for i1, i2 in ((imap1, imap2), (imap3, imap3), (None, None)):
#                    data = xrange(10)
#                    pickler = Worker(workers.io.pickle_dumps)
#                    dumper = Worker(workers.io.dump_item, (typ,))
#                    loader = Worker(workers.io.load_item)
#                    unpickler = Worker(workers.io.pickle_loads)
#
#                    p_pickler = Piper(pickler)
#                    p_dumper = Piper(dumper, parallel=i1, debug=True)
#                    p_loader = Piper(loader, parallel=i2)
#                    p_unpickler = Piper(unpickler)
#
#                    p_pickler([data])
#                    p_dumper([p_pickler])
#                    p_loader([p_dumper])
#                    p_unpickler([p_loader])
#
#                    p_pickler.start()
#                    p_dumper.start(stages=(0, 1))
#                    p_loader.start(stages=(0, 1))
#                    p_dumper.start(stages=(2,))
#                    p_loader.start(stages=(2,))
#                    p_unpickler.start()
#                    assert list(data) == list(p_unpickler)
#                    try:
#                        p_dumper.stop(ends=[len(p_dumper.imap._tasks) - 1])
#                    except AttributeError:
#                        pass
#                    try:
#                        p_loader.stop(ends=[len(p_loader.imap._tasks) - 1])
#                    except AttributeError:
#                        pass
#
#    def xtestdump_itmes_thread(self):
#        for typ in ('tcp', 'udp'):
#            for typ2 in ('string',):
#                imap1 = IMap(worker_type='thread')
#                imap2 = IMap(worker_type='thread')
#                imap3 = IMap(worker_type='thread')
#                for i1, i2 in ((imap1, imap2), (imap3, imap3), (None, None)):
#                    data = xrange(10)
#                    pickler = Worker(workers.io.pickle_dumps)
#                    dumper = Worker(workers.io.dump_item, (typ,))
#                    loader = Worker(workers.io.load_item)
#                    unpickler = Worker(workers.io.pickle_loads)
#
#                    p_pickler = Piper(pickler)
#                    p_dumper = Piper(dumper, parallel=i1, debug=True)
#                    p_loader = Piper(loader, parallel=i2)
#                    p_unpickler = Piper(unpickler)
#
#                    p_pickler([data])
#                    p_dumper([p_pickler])
#                    p_loader([p_dumper])
#                    p_unpickler([p_loader])
#
#                    p_pickler.start()
#                    p_dumper.start(stages=(0, 1))
#                    p_loader.start(stages=(0, 1))
#                    p_dumper.start(stages=(2,))
#                    p_loader.start(stages=(2,))
#                    p_unpickler.start()
#                    assert list(data) == list(p_unpickler)
#                    try:
#                        p_dumper.stop(ends=[len(p_dumper.imap._tasks) - 1])
#                    except AttributeError:
#                        pass
#                    try:
#                        p_loader.stop(ends=[len(p_loader.imap._tasks) - 1])
#                    except AttributeError:
#                        pass

    def test_track(self):
        inpt = xrange(100)
        pool = NuMap()
        ppr_instance = Piper(power2, parallel=pool, track=True)
        ppr_instance([inpt])
        ppr_instance.start(stages=(0, 1, 2))
        list(ppr_instance)
        assert ppr_instance.imap._tasks_tracked[0].values() == [i * i for i in
        ppr_instance.imap._tasks_tracked[0].keys()]

#    def xtestoutput_pickle(self):
#        import os
#        handle = os.tmpfile()
#        data = [{1:1}, {2:2}]
#        pickler = Worker(workers.io.pickle_dumps)
#        dumper = Worker(workers.io.dump_stream, (handle,))
#        pickle_piper = Piper(pickler)
#        dump_piper = Piper(dumper)
#        pickle_piper([data])
#        dump_piper([pickle_piper])
#        pickle_piper.start()
#        dump_piper.start()
#        list(dump_piper)
#        handle.seek(0)
#        input = workers.io.load_stream(handle)
#        depickler = Piper(workers.io.pickle_loads)
#        depickler([input])
#        depickler.start()
#        a = list(depickler)
#        assert a == [{1: 1}, {2: 2}]
#        handle.close()
#        dump_piper.stop()
#        pickle_piper.stop()
#        depickler.stop()
#        assert dump_piper.started is False
#        assert pickle_piper.started is False
#        assert depickler.started is False
#        dump_piper.disconnect()
#        pickle_piper.disconnect()
#        depickler.disconnect()

#    def xtestconnect_pickle(self):
#        handle = open('test_pick', 'rb')
#        input = workers.io.load_pickle_stream(handle)
#        passer = Piper(workers.core.ipasser)
#        passer([input])
#        passer.start()
#        assert list(passer) == [[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 2, 3], \
#        [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], \
#        [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6], [4, 5, 6]]


class test_Dagger(unittest.TestCase):

    def setUp(self):
        # pipers
        self.sm2 = Piper(sum2)
        self.pwr = Piper(power2)
        self.dbl = Piper(mul2)
        self.spr = Piper(sleeper)
        self.pwrdbl = Piper([power2, mul2])
        self.dblpwr = Piper([mul2, power2])
        self.dbldbl = Piper([mul2, mul2])
        self.pwrpwr = Piper([power2, power2])
        # workers
        self.w_sm2 = Worker(sum2)
        self.w_pwr = Worker(power2)
        self.w_dbl = Worker(mul2)
        self.w_spr = Worker(sleeper)
        self.w_pwrdbl = Worker([power2, mul2])
        self.w_dblpwr = Worker([mul2, power2])
        self.w_dbldbl = Worker([mul2, mul2])
        self.w_pwrpwr = Worker([power2, power2])
        self.dag = Dagger()

    def test_logger(self):
        from logging import Logger
        self.assertTrue(isinstance(self.dag.log, Logger))

    def testresolve(self):
        self.dag = Dagger((self.dbl, self.pwr))
        self.assertEqual(len(self.dag.nodes()), 2)
        assert self.dbl is self.dag.resolve(self.dbl)
        assert self.pwr is self.dag.resolve(id(self.pwr))
        assert self.pwr != self.dag.resolve(id(self.dbl))
        self.assertRaises(DaggerError, self.dag.resolve, self.w_pwr)
        dbl = Piper(mul2)
        self.assertRaises(DaggerError, self.dag.resolve, dbl)
        self.assertRaises(DaggerError, self.dag.resolve, id(dbl))
        w_dbl = Worker(mul2)
        self.assertRaises(DaggerError, self.dag.resolve, self.w_dbl)

    def test_addwithbranch(self):
        ppr = Piper(self.w_pwr, branch='the_new_b')
        self.dag.add_piper(ppr)
        assert ppr.branch == 'the_new_b'
        assert self.dag[ppr].branch == 'the_new_b'

    def test_make_piper(self):
        assert self.pwr is not Piper(self.pwr)
        assert self.pwr is not Piper(power)
        assert self.pwr is not Piper(self.w_pwr)
        self.dag.add_piper(self.pwr)
        assert self.pwr is self.dag.resolve(self.pwr)

    def test_add_piper(self):
        self.dag.add_piper(self.pwr)
        self.dag.add_piper(self.pwr)
        self.assertEqual(len(self.dag), 1)
        self.assertEqual(len(self.dag.nodes()), 1)
        self.dag.add_pipers([self.dbl])
        self.assertEqual(len(self.dag), 2)
        self.assertRaises(DaggerError, self.dag.add_piper, [1])
        self.assertRaises(DaggerError, self.dag.add_piper, [self.dblpwr])
        self.assertRaises(DaggerError, self.dag.add_piper, 1)
        self.assertRaises(DaggerError, self.dag.add_pipers, [1])
        self.assertRaises(TypeError, self.dag.add_pipers, 1)
        self.dag.add_pipers([self.dblpwr, self.pwrdbl])
        self.assertEqual(len(self.dag), 4)

    def xtest_incoming(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrdbl))
        self.assertRaises(DaggerError, self.dag.del_piper, self.pwr)
        self.assertRaises(DaggerError, self.dag.del_piper, self.dbl)
        self.dag.del_piper(self.pwrdbl)
        self.dag.del_piper(self.dbl)
        self.dag.del_piper(self.pwr)


    def test_del_piper(self):
        self.dag.add_piper(self.pwr)
        self.dag.del_piper(self.pwr)
        self.dag.add_piper(self.pwr)
        self.dag.del_piper(id(self.pwr))
        self.assertEqual(len(self.dag), 0)
        self.assertEqual(len(self.dag.nodes()), 0)
        self.assertRaises(DaggerError, self.dag.add_piper, self.w_pwr, create=False)
        self.assertRaises(DaggerError, self.dag.add_piper, [1], create=True)
        self.dag.add_piper(self.w_pwr)
        self.assertEqual(len(self.dag), 1)
        self.dag.add_piper(self.dbl)
        self.dag.add_piper(self.dbl)
        self.assertEqual(len(self.dag), 2)
        dbl = Piper(mul2)
        self.dag.add_piper(dbl)
        self.assertEqual(len(self.dag), 3)
        self.assertEqual(len(self.dag.nodes()), 3)
        self.dag.del_piper(self.dbl)
        self.assertEqual(len(self.dag), 2)
        self.assertRaises(DaggerError, self.dag.del_piper, self.dbl)
        self.assertRaises(DaggerError, self.dag.del_piper, Piper(self.w_pwr))
        self.assertRaises(DaggerError, self.dag.del_piper, self.w_pwr)
        self.assertRaises(DaggerError, self.dag.del_piper, 77)
        self.assertRaises(DaggerError, self.dag.del_piper, [77])
        self.dag.add_piper(self.pwr)
        self.dag.del_piper(self.pwr)
        self.dag.add_piper(self.pwr)
        self.assertEqual(len(self.dag), 3)
        self.assertRaises(DaggerError, self.dag.del_piper, [self.pwr])
        self.dag.del_pipers([self.pwr])
        self.assertEqual(len(self.dag), 2)
        self.dag.add_piper(self.pwr)
        ids = [id(i) for i in self.dag]
        self.dag.del_pipers(ids)
        self.assertEqual(len(self.dag), 0)
        self.assertEqual(len(self.dag.nodes()), 0)
        self.dag.add_piper(self.pwr)
        pwr = Piper(self.w_pwr)
        self.dag.add_piper(pwr)
        self.assertEqual(len(self.dag), 2)
        pipers = self.dag.nodes()
        self.dag.del_pipers(pipers)
        self.assertEqual(len(self.dag), 0)
        self.assertEqual(len(self.dag.nodes()), 0)
        self.assertRaises(DaggerError, self.dag.del_piper, self.dbl)
        self.assertRaises(DaggerError, self.dag.del_pipers, \
                                               [self.dbl, self.pwrpwr])
        self.assertRaises(DaggerError, self.dag.del_piper, (1, 2))

    def test_incoming2(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrdbl))
        self.assertTrue(self.pwr in self.dag)
        self.assertRaises(DaggerError, self.dag.del_piper, self.pwr)
        self.dag.del_piper(self.pwr, forced=True)
        self.assertFalse(self.pwr in self.dag)

    def test_add_workers(self):
        self.dag.add_piper(self.w_pwr)
        self.dag.add_pipers([self.w_pwr])
        self.assertEqual(len(self.dag), 2)
        self.assertEqual(len(self.dag.nodes()), 2)
        self.assertRaises(DaggerError, self.dag.add_piper, [1])
        self.assertRaises(DaggerError, self.dag.add_piper, 1)
        self.dag.add_pipers([self.w_pwr, self.w_dbl])
        self.dag.add_piper(self.w_pwrdbl)
        self.assertEqual(len(self.dag), 5)
        self.dag.add_piper(self.w_pwrdbl)
        self.assertRaises(DaggerError, self.dag.add_piper, self.w_pwrdbl, create=False)
        self.assertEqual(len(self.dag), 6)

    def test_add_pipe(self):
        self.dag = Dagger()
        self.dag.add_pipe((self.pwr, self.dbl))
        self.assertEqual(len(self.dag.nodes()), 2)
        self.assertEqual(len(self.dag.edges()), 1)
        self.dag.add_pipe((self.dbl, self.pwrdbl))
        self.assertEqual(len(self.dag.nodes()), 3)
        self.assertEqual(len(self.dag.edges()), 2)
        self.assertEqual(len(self.dag.deep_nodes(self.pwrdbl)), 2)

    def test_del_pipe(self):
        self.dag.add_pipe((self.pwr, self.sm2))
        self.dag.add_pipe((self.pwr, self.dbl))
        self.dag.add_pipe((self.dbl, self.sm2))
        self.assertEqual(len(self.dag.edges()), 3)
        self.assertEqual(len(self.dag.nodes()), 3)
        self.assertEqual(len(self.dag.incoming_edges(self.pwr)), 2)
        self.dag.del_pipe((self.dbl, self.sm2))
        self.assertEqual(len(self.dag.nodes()), 1)

    def test_del_pipe2(self):
        self.dag.add_pipe((self.pwr, self.dbl, self.pwrpwr, self.dblpwr))
        self.dag.add_pipe((self.pwr, self.spr))
        self.dag.del_piper(self.dblpwr)
        self.assertRaises(DaggerError, self.dag.del_piper, self.pwr)
        self.dag.del_pipe((self.pwr, self.spr))
        assert self.pwr in self.dag
        assert self.spr not in self.dag

    def test_circular_prevention(self):
        self.dag.add_pipe((self.dbl, self.dbldbl, self.sm2))
        self.assertRaises(DaggerError, self.dag.add_pipe, (self.sm2, self.dbl))
        self.dag.add_pipe((self.dbl, self.sm2))

    def test_connect_simple(self):
        for par in (False, NuMap()):
            self.dag = Dagger()
            inp = [1, 2, 3, 4]
            pwr = Piper(power2, parallel=par)
            dbl = Piper(mul2, parallel=par)
            self.dag.add_pipe((pwr, dbl))
            pwr([inp])
            self.dag.connect()
            pwr.start(stages=(0, 1))
            dbl.start(stages=(0, 1))
            pwr.start(stages=(2,))
            dbl.start(stages=(2,))
            self.assertEqual(list(dbl), [2, 8, 18, 32])
            pwr.stop(ends=[1])

    def test_connect_disconnect(self):
        for par in (NuMap(), None):
            self.dag = Dagger()
            pwr = Piper(power2, parallel=par)
            dbl = Piper(mul2, parallel=par)
            pwrdbl = Piper([power2, mul2])
            self.dag.add_pipe((pwr, dbl))
            self.dag.add_piper(pwrdbl)
            self.dag.connect_inputs([[1, 2, 3], [4, 5, 6]])
            self.dag.connect()
            self.dag.disconnect()
            assert pwr.connected is False
            assert dbl.connected is False
            assert pwrdbl.connected is False
            if pwrdbl.imap is not imap:
                assert pwrdbl.imap._tasks == []

    def test_connect_disconnect2(self):
        for par in (NuMap(), None):
            self.dag = Dagger()
            pwr = Piper(power2, parallel=par)
            dbl = Piper(mul2, parallel=par)
            pwrdbl = Piper([power2, mul2])
            pwrpwr = Piper([power2, power2])
            self.dag.add_pipe((pwr, dbl))
            self.dag.add_pipe((pwrdbl, pwrpwr))
            self.dag.connect([[1, 2, 3], [4, 5, 6]])
            self.dag.disconnect()
            assert pwr.connected is False
            assert dbl.connected is False
            assert pwrdbl.connected is False
            assert pwrpwr.connected is False
            if pwrdbl.imap is not imap:
                assert pwrdbl.imap._tasks == []

    def test_startstop(self):
        for i in range(12):
            data = range(i)
            for stride in (1, 2, 3, 4, 5, 6, 7):
                # pipers
                imaps = [NuMap(worker_num=stride), NuMap(worker_num=stride), \
                         NuMap(worker_num=stride), NuMap(worker_num=stride)]
                pwr = Piper(power2, parallel=imaps[randint(0, 3)])
                dbl_3 = Piper(mul2, parallel=imaps[randint(0, 3)])
                dbl_4 = Piper(mul2, parallel=imaps[randint(0, 3)])
                sum_2 = Piper(sumof2, parallel=imaps[randint(0, 3)])
                # topology
                self.dag = Dagger()
                self.dag.add_pipe((pwr, dbl_3))
                self.dag.add_pipe((pwr, dbl_4))
                self.dag.add_pipe((dbl_3, sum_2))
                self.dag.add_pipe((dbl_4, sum_2))
                # runit
                self.dag.connect([data])
                self.dag.start()
                assert list(sum_2) == [(i ** 2) * 4 for i in data]
                self.dag.stop()

    def test_childparentsort(self):
        for i in range(100):
            dag = Dagger()
            dbl1 = Piper(mul2)
            dbl2 = Piper(mul2)
            sum = Piper(sumof2)
            dag.add_pipe((dbl1, dbl2))
            dag.add_pipe((dbl1, sum))
            dag.add_pipe((dbl2, sum))
            dag.connect()
            inputs = dag[sum].nodes()
            sorted_inputs = [p for p in dag.postorder() if p in inputs]
            sorted_inputs.sort(cmp=dag.children_after_parents)
            assert sorted_inputs == [dbl2, dbl1]

        for i in range(100):
            dag = Dagger()
            dbl1 = Piper(mul2, name='1')
            dbl2a = Piper(mul2, name='2a')
            dbl2b = Piper(mul2, name='2b')
            sum = Piper(sumof3, name='sum')
            dag.add_pipe((dbl1, dbl2a))
            dag.add_pipe((dbl1, dbl2b))
            dag.add_pipe((dbl1, sum))
            dag.add_pipe((dbl2a, sum))
            dag.add_pipe((dbl2b, sum))
            postorder = dag.postorder()
            assert postorder[0] == dbl1
            assert postorder[-1] == sum
            inputs = dag[sum].nodes()
            sorted_inputs = [p for p in postorder if p in inputs]
            sorted_inputs.sort(cmp=dag.children_after_parents)
            assert sorted_inputs[0:2] == postorder[1:3]

        for i in range(100):
            dag = Dagger()
            dbl1 = Piper(mul2, name='1')
            dbl2a = Piper(mul2, name='2a', branch='a')
            dbl2b = Piper(mul2, name='2b', branch='b')
            sum = Piper(sumof3, name='sum')
            dag.add_pipe((dbl1, dbl2a))
            dag.add_pipe((dbl1, dbl2b))
            dag.add_pipe((dbl1, sum))
            dag.add_pipe((dbl2a, sum))
            dag.add_pipe((dbl2b, sum))
            postorder = dag.postorder()
            assert postorder[0] == dbl1
            assert postorder[1] == dbl2a
            assert postorder[2] == dbl2b
            assert postorder[-1] == sum
            inputs = dag[sum].nodes()
            sorted_inputs = [p for p in postorder if p in inputs]
            sorted_inputs.sort(cmp=dag.children_after_parents)
            assert sorted_inputs[0:2] == postorder[1:3]


    def test_inputoutput(self):
        for par in (False, NuMap()):
            self.dag = Dagger()
            pwr = Piper(power2, parallel=par)
            dbl = Piper(mul2, parallel=par)
            pwr2 = Piper(power2, parallel=par)
            self.dag.add_piper(pwr)
            assert pwr is self.dag.get_inputs()[0]
            assert pwr is self.dag.get_outputs()[0]
            self.dag.add_pipe((pwr, dbl))
            assert len(self.dag) == 2
            assert pwr is self.dag.get_inputs()[0]
            assert dbl is self.dag.get_outputs()[0]
            self.dag.add_pipe((pwr, pwr2))
            assert len(self.dag) == 3
            assert pwr is self.dag.get_inputs()[0]
            assert len(self.dag.get_outputs()) == 2
            assert pwr not in self.dag.get_outputs()
            assert pwr2  in self.dag.get_outputs()
            assert dbl  in self.dag.get_outputs()
            self.assertRaises(DaggerError, self.dag.add_pipe, (pwr2, pwr))

class test_Plumber(unittest.TestCase):

    def setUp(self):
        self.sm2 = Piper(sumof2)

        i = NuMap()
        self.pwrp = Piper(power2, parallel=i)
        self.dblp = Piper(mul2, parallel=i)
        self.dbl = Piper(mul2)
        self.spr = Piper(sleeper)
        self.pwrdbl = Piper([power2, mul2])
        self.dblpwr = Piper([mul2, power2])
        self.dbldbl = Piper([mul2, mul2])
        self.pwrpwr = Piper([power2, power2])
        self.plum = Plumber()

    def test_init(self):
        assert isinstance(self.plum, Dagger)

    def test_start_run_finish_stop(self):
        self.pwr_linear = Piper(power)
        self.pwr_parallel = Piper(power, parallel=NuMap(buffer=2), track=True, name='power')
        self.plum.add_piper(self.pwr_linear)
        self.plum.start([[2]])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.stop()
        self.plum.del_piper(self.pwr_linear)
        self.plum.add_piper(self.pwr_parallel)
        self.plum.start([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.stop()

    def test_start_run_finish_stop2(self):
        pwr_l1 = Piper(power2)
        pwr_l2 = Piper(power2)
        pwr_l3 = Piper(power2)
        pwr_l4 = Piper(power2)
        pwr_p1 = Piper(power, parallel=NuMap(), track=True, name='power1')
        pwr_p2 = Piper(power, parallel=NuMap(), track=True, name='power2')
        pwr_p3 = Piper(power, parallel=NuMap(), track=True, name='power3')
        pwr_p4 = Piper(power, parallel=NuMap(), track=True, name='power4')

        # linear
        self.plum.add_pipe((pwr_l1, pwr_l2))
        data = iter(xrange(100000000))
        self.plum.start([data])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.stop()

        #clean
        self.plum.del_pipe((pwr_l1, pwr_l2))

        # parallel
        self.plum.add_pipe((pwr_p1, pwr_p2))
        data = iter(xrange(100000000))
        self.plum.start([data])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.stop()
        assert len(self.plum.stats['pipers_tracked'][pwr_p1][0]) == len(self.plum.stats['pipers_tracked'][pwr_p2][0]) == data.next()

        # clean
        self.plum.del_pipe((pwr_p1, pwr_p2))

        # linear -> parallel
        self.plum.add_pipe((pwr_l3, pwr_p3))
        data = iter(xrange(10000))
        self.plum.start([data])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.stop()
        assert len(self.plum.stats['pipers_tracked'][pwr_p3][0]) == data.next()

        # clean
        self.plum.del_pipe((pwr_l3, pwr_p3))

        # parallel -> linear
        self.plum.add_pipe((pwr_p4, pwr_l4))
        data = iter(xrange(10000))
        self.plum.start([data])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.stop()
        assert len(self.plum.stats['pipers_tracked'][pwr_p4][0]) == data.next()

    def test_start_run_pause_run_stop(self):
        pwr_linear = Piper(power2)
        self.plum.add_piper(pwr_linear)
        self.plum.start([xrange(10000000)])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.assertRaises(PlumberError, self.plum.start, [[1, 2, 3]])
        self.plum.pause()
        self.plum.run()
        self.plum.pause()
        self.plum.stop()
        pwr_parallel = Piper(power2, parallel=NuMap(buffer=2), track=True, name='power')
        self.plum.del_piper(pwr_linear)
        self.plum.add_piper(pwr_parallel)
        self.plum.start([xrange(10000000)])
        self.assertRaises(PlumberError, self.plum.pause)
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.run()
        self.assertRaises(PlumberError, self.plum.stop)
        self.plum.pause()
        self.plum.stop()

#    def test_track(self):
#        inpt = xrange(100)
#        imap_ = NuMap()
#        wrkr = Worker((power, workers.io.json_dumps, workers.io.dump_item), ((), (), ('shm',)))
#        pipr = Piper(wrkr, parallel=imap_, track=True)
#        self.plum.add_piper(pipr)
#        self.plum.start([inpt])
#        self.plum.run()
#        self.plum._finished.wait()
#        dct = self.plum.stats['pipers_tracked'].values()[0][0]
#        assert len(dct) == 100
#        for i in dct.values():
#            os.unlink('/dev/shm/%s' % i[0])

#    def test_load_save(self):
#        imap1 = IMap(name='imap1')
#        imap2 = IMap(name='imap2')
#        pwr_p1 = Piper(power, parallel=imap1, track=True, name='power_p1', debug=True)
#        pwr_p2 = Piper(power, parallel=IMap(), track=True, name='power_p2', debug=True)
#        pwr_p3 = Piper(power, parallel=imap2, track=True, name='power_p3', debug=True)
#        sum_p4 = Piper(sum2, parallel=imap2, track=True, name='sum_p4', debug=True)
#        self.plum.add_piper(pwr_p1, xtra={'name':'power_p1'})
#        self.plum.add_piper(pwr_p2, xtra={'name':'power_p2'})
#        self.plum.add_piper(pwr_p3, xtra={'name':'power_p3'})
#        self.plum.add_piper(sum_p4, xtra={'name':'sum_p4'})
#        self.plum.add_pipe((pwr_p1, pwr_p2, pwr_p3, sum_p4))
#        self.plum.add_pipe((pwr_p2, sum_p4))
#        self.plum.start([range(10)])
#        self.plum.run()
#        self.plum._finished.wait()
#        self.plum.pause()
#        self.plum.stop()
#        self.plum.save('remove_me.py')
#        print self.plum.stats
#        plum = Plumber()
#        plum.load('remove_me.py')
#        plum.start([range(10)])
#        plum.run()
#        plum._finished.wait()
#        plum.pause()
#        plum.stop()
#        plum.save('remove_me2.py')
#        os.unlink('remove_me.py')
#        os.unlink('remove_me2.py')
#        print self.plum.stats

class test_Internal(unittest.TestCase):

    def test__comp_task(self):

        def plus(i):
            return i[0] + 1
        def minus(i):
            return i[0] - 1

        pc.TASK = (plus, minus)
        assert 3 == _comp_task([3], ((), ()), ({}, {}))

    def test_Repeat(self):
        product = _Repeat(iter([0, 1, 2, 3, 4, 5, 6]), n=2, stride=3)
        result = []
        for i in range(24):
            try:
                result.append(product.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [0, 1, 2, 0, 1, 2, 3, 4, 5, 3, 4, 5, 6, 's', 's', 6, 's', 's', 's', 's', 's', 's', 's', 's'])

    def test__Produce(self):
        product = _Produce(iter([(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62), (71, 72)]), n=2, stride=3)
        result = []
        for i in range(24):
            try:
                result.append(product.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [11, 21, 31, 12, 22, 32, 41, 51, 61, 42, 52, 62, 71, 's', 's', 72, 's', 's', 's', 's', 's', 's', 's', 's'])

    def test_Consume(self):
        consumpt = \
        _Consume(iter([0, 1, 2, 0, 1, 2, 3, 4, 5, 3, 4, 5, 6, 's', 's', 6, 's', 's', 's', 's', 's', 's', 's', 's']), stride=3, n=2)
        result = []
        for i in range(12):
            result.append(consumpt.next())
        self.assertEqual(result, [[0, 0], [1, 1], [2, 2], [3, 3],
                                [4, 4], [5, 5], [6, 6], ['s', 's'],
                                ['s', 's'], ['s', 's'], ['s', 's'], ['s', 's']])

    def test_produce_piper_from_sequence(self):
        inp = [(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62), (71, 72)]
        par = NuMap(stride=3)
        w_p2 = Worker(papy.util.func.ipasser)
        p_p2 = Piper(w_p2, parallel=par, produce=2)
        p_p2 = p_p2([inp])
        p_p2.start()
        result = []
        for i in range(24):
            try:
                result.append(p_p2.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [11, 21, 31, 12, 22, 32, 41, 51, 61, 42,
         52, 62, 71, 's', 's', 72, 's', 's', 's', 's', 's', 's', 's', 's'])
        p_p2.stop(ends=[0])

    def test_produce_piper_repeat(self):
        inp = [0, 1, 2, 3, 4, 5, 6]
        par = NuMap(stride=3)
        w_p2 = Worker(power2_make2)
        p_p2 = Piper(w_p2, parallel=par, produce=2)
        p_p2 = p_p2([inp])
        p_p2.start()
        result = []
        for i in range(25):
            try:
                result.append(p_p2.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result,
        [0, 1, 4, 0, 1, 4, 9, 16, 25, 9, 16, 25, 36, 's', 's', 36, 's', 's',
         's', 's', 's', 's', 's', 's', 's'])

    def test_produce_exception(self):
        inp = [0, 1, 'z', 3, 4, 5, 6]
        p2m2 = Worker(power2_make2)
        for i in inp:
            if i == 2:
                assert type(p2m2([i])) == WorkerError
        par = NuMap(stride=3)
        w_p2 = Worker(power2_make2)
        p_p2 = Piper(w_p2, parallel=par, produce=2, debug=True)
        p_p2 = p_p2([inp])
        p_p2.start()
        result = []
        for i in range(25):
            try:
                result.append(p_p2.next())
            except StopIteration:
                result.append('s')
            except Exception, e:
                result.append('e')
        self.assertEqual(result,
        [0, 1, 'e', 0, 1, 'e', 9, 16, 25, 9, 16, 25, 36, 's', 's', 36, 's', 's',
          's', 's', 's', 's', 's', 's', 's'])

    def test_produce_repeat_exception(self):
        inp = [0, 1, 'z', 3, 4, 5, 6]
        p2m2 = Worker(power2)
        par = NuMap(stride=3)
        w_p2 = Worker(power2)
        p_p2 = Piper(w_p2, parallel=par, produce=2, repeat=True , debug=True)
        p_p2 = p_p2([inp])
        p_p2.start()
        result = []
        for i in range(25):
            try:
                result.append(p_p2.next())
            except StopIteration:
                result.append('s')
            except Exception, e:
                result.append('e')
        self.assertEqual(result,
        [0, 1, 'e', 0, 1, 'e', 9, 16, 25, 9, 16, 25, 36, 's', 's', 36, 's', 's',
          's', 's', 's', 's', 's', 's', 's'])

    def test__Chain(self):
        inp1 = [0, 1, 2, 3, 4, 5, 6]
        inp2 = [1, 2, 3, 4, 5, 6, 7]
        par = NuMap(stride=3)
        w_p2 = Worker(power2)
        p_p2 = Piper(w_p2, parallel=par, produce=2, repeat=True)
        p_p3 = Piper(w_p2, parallel=par, produce=2, repeat=True)
        p_p2 = p_p2([inp1])
        p_p3 = p_p3([inp2])
        p_p2.start()
        chainer = _Chain([p_p2, p_p3], stride=3)
        result = []
        for i in range(36):
            try:
                result.append(chainer.next())
            except StopIteration:
                result.append('s')
        self.assertEqual(result, [0, 1, 4, 1, 4, 9, 0, 1, 4, 1, 4, 9, 9, 16, 25, 16, 25, 36,
                                  9, 16, 25, 16, 25, 36, 36, 's', 's', 49, 's', 's', 36,
                                  's', 's', 49, 's', 's', ])

    def test_consume(self):
        inp = [0, 1, 4, 0, 1, 4, 9, 16, 25, 9, 16, 25, 36, 's', 's', 36, 's',
               's', 's', 's', 's', 's', 's', 's']
        par = NuMap(stride=3)
        w_s2 = Worker(join_sum2)
        p_s2 = Piper(w_s2, parallel=par, consume=2)
        p_s2 = p_s2([inp])
        p_s2.start()
        result = list(p_s2)
        self.assertEqual([0, 2, 8, 18, 32, 50, 72, 'ss', 'ss', 'ss', 'ss', 'ss'], result)
        p_s2.stop(ends=[0])

    def test_produceconsume_repeat(self):
        for par in (None, NuMap(stride=1)):
            inp = [0, 1, 2, 3, 4, 5, 6]
            w_p2 = Worker(power2)
            p_p2 = Piper(w_p2, parallel=par, produce=2, debug=True, repeat=True)
            p_p2 = p_p2([inp])
            w_s2 = Worker(sum2)
            p_s2 = Piper(w_s2, parallel=par, consume=2, debug=True)
            p_s2 = p_s2([p_p2])
            p_p2.start()
            p_s2.start()
            self.assertEqual(list(p_s2), [0, 2, 8, 18, 32, 50, 72])

    def test_produceconsume(self):
        inp = [(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62)]
        par = NuMap(stride=3)
        w_p2 = Worker(papy.util.func.ipasser)
        p_p2 = Piper(w_p2, parallel=par, produce=2)
        p_p2 = p_p2([inp])
        w_s2 = Worker(sum2)
        p_s2 = Piper(w_s2, parallel=par, consume=2, debug=True)
        p_s2 = p_s2([p_p2])
        p_p2.start()
        p_s2.start()
        self.assertEqual(list(p_s2), [23, 43, 63, 83, 103, 123])

    def test_producespawnconsume(self):
        for i in range(20):
            inp = [(11, 12), (21, 22), (31, 32), (41, 42), (51, 52), (61, 62)]
            par = NuMap(stride=3)
            w_p2 = Worker(papy.util.func.ipasser)
            p_p2 = Piper(w_p2, parallel=par, produce=2)
            p_p2 = p_p2([inp])
            w_m2 = Worker(mul2)
            p_m2 = Piper(w_m2, parallel=par, spawn=2)
            p_m2 = p_m2([p_p2])
            w_s2 = Worker(sum2)
            p_s2 = Piper(w_s2, parallel=par, consume=2)
            p_s2 = p_s2([p_m2])
            p_p2.start(stages=(0, 1))
            p_m2.start(stages=(0, 1))
            p_s2.start()
            self.assertEqual(list(p_s2), [46, 86, 126, 166, 206, 246])

    def test_timeout(self):
        par = NuMap(worker_num=1)
        piper = Piper(sleeper, parallel=par, timeout=0.75)
        inp = [0.5, 1.0, 0.5]
        piper([inp])
        piper.start()
        assert piper.next()[0] == 0.5 # get 1
        a = piper.next() # get timeout
        self.assertTrue(isinstance(a, PiperError))
        self.assertTrue(isinstance(a[0], TimeoutError))
        assert piper.next()[0] == 1.0
        assert piper.next()[0] == 0.5
        piper.stop(ends=[0], forced=True) # really did not finish

    def test_tee(self):
        for i in range(20):
            inp = iter([1, 2, 3, 4, 5, 6])
            w_ip = Worker(papy.util.func.ipasser)
            w_p2 = Worker(power2)
            w_m2 = Worker(mul2)
            p_ip = Piper(w_ip)
            p_p2 = Piper(w_p2)
            p_m2 = Piper(w_m2)
            p_ip([inp])
            p_p2([p_ip])
            p_m2([p_ip])
            p_ip.start()
            p_p2.start()
            p_m2.start()
            assert zip(p_p2, p_m2) == [(1, 2), (4, 4), (9, 6), \
                                       (16, 8), (25, 10), (36, 12)]

#    def test_tee_produce(self):
#        inp = iter([1, 2, 3, 4, 5, 6])
#        w_ip = Worker(workers.core.ipasser)
#        w_p2 = Worker(workers.core.npasser, (2,))
#        w_m2 = Worker(workers.core.npasser, (2,))
#        p_ip = Piper(w_ip, produce=2, debug=True)
#        p_p2 = Piper(w_p2, consume=2, debug=True)
#        p_m2 = Piper(w_m2, consume=2, debug=True)
#        p_ip([inp])
#        p_p2([p_ip])
#        p_m2([p_ip])
#        p_ip.start(forced=True)
#        p_p2.start(forced=True)
#        p_m2.start(forced=True)
#        assert list(p_p2) == [[(1,), (1,)], [(2,), (2,)], [(3,), (3,)], [(4,), \
#                                              (4,)], [(5,), (5,)], [(6,), (6,)]]
#        assert list(p_m2) == [[(1,), (1,)], [(2,), (2,)], [(3,), (3,)], [(4,), \
#                                              (4,)], [(5,), (5,)], [(6,), (6,)]]


#    def test_produceconsume2(self):
#        for st in (1, 2, 3, 4, 5):
#            for par in (NuMap(stride=st), None):
#                inp = [1, 2, 3, 4, 5]
#                w_p2 = Worker(power2)
#                p_p2 = Piper(w_p2, parallel=par, produce=200, repeat=True)
#                p_p2 = p_p2([inp])
#                w_ss = Worker(sum_of_sqr)
#                p_ss = Piper(w_ss, parallel=par, consume=200)
#                p_ss([p_p2])
#                p_p2.start()
#                p_ss.start()
#                for j in [1, 2, 3, 4, 5]:
#                    self.assertAlmostEqual(p_ss.next(), 200 * j)
#                self.assertRaises(StopIteration, p_ss.next)
#                p_ss.stop(ends=[1])

#    def test_producespawnconsume_repeat(self):
#        for i in range(20):
#            inp = [0, 1, 2, 3, 4, 5, 6]
#            par = NuMap(worker_num=3)
#            par = None
#
#            w_p2 = Worker(power2)
#            p_p2 = Piper(w_p2, parallel=NuMap(stride=1), produce=2, debug=True, repeat=True)
#            p_p2 = p_p2([inp])
#
#            w_sq = Worker(sqrt2)
#            p_sq = Piper(w_sq, parallel=par, spawn=2, debug=True)
#            p_sq = p_sq([p_p2])
#
#            w_ip = Worker(papy.util.func.ipasser)
#            p_ip = Piper(w_ip, parallel=None, spawn=2, debug=True)
#            p_ip = p_ip([p_sq])
#
#            w_s2 = Worker(sum2)
#            p_s2 = Piper(w_s2, parallel=par, consume=2, debug=True)
#            p_s2 = p_s2([p_sq])
#
#
#            p_p2.start(stages=(0, 1))
#            p_sq.start(stages=(0, 1))
#            p_ip.start(stages=(0, 1))
#            p_s2.start(stages=(0, 1))
#
#            p_p2.start(stages=(2,))
#            p_sq.start(stages=(2,))
#            p_ip.start(stages=(2,))
#            p_s2.start(stages=(2,))
#
#            print 'x'
#            self.assertEqual(list(p_s2), [0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
