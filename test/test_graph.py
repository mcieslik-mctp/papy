# -*- coding: utf-8 -*-
"""
tests for ``papy.graph``.
"""
import unittest
from random import randint
from papy.graph import DictNode, DictGraph

class test_Graph(unittest.TestCase):

    def setUp(self):
        self.graph = DictGraph()
        self.repeats = 20

    def test_Node(self):
        node = DictNode(1)
        self.assertEqual(node, {1:{}})
        node = DictNode('1')
        self.assertEqual(node, {'1':{}})
        node = DictNode()
        self.assertEqual(node, {})
        node = DictNode(())
        self.assertEqual(node, {():{}})
        self.assertRaises(TypeError, DictNode, {})
        self.assertRaises(TypeError, DictNode, [])

    def test_node(self):
        self.graph.add_node(1)
        self.assertTrue(isinstance(self.graph[1], DictNode))
        self.assertEqual(self.graph[1], {})
        self.graph.del_node(1)
        for i in xrange(self.repeats):
            node = randint(0, 100)
            self.graph.add_node(node)
            self.graph.del_node(node)
            self.assertFalse(self.graph)
            self.assertFalse(self.graph.nodes())
            self.assertFalse(self.graph.edges())
        self.assertEqual(self.graph, {})

    def test_nodes(self):
        for i in xrange(self.repeats):
            nodes = set([])
            for i in xrange(self.repeats):
                nodes.add(randint(1, 10))
            self.graph.add_nodes(nodes)
            self.assertEqual(nodes, set(self.graph.nodes()))
            self.graph.del_nodes(nodes)
            self.assertFalse(self.graph)

    def test_edge(self):
        for i in xrange(self.repeats):
            edge = (randint(0, 49), randint(50, 100))
            double = randint(0, 1)
            self.graph.add_edge(edge, double)
            self.assertTrue(edge in self.graph.edges())
            if double:
                self.assertTrue((edge[1], edge[0]) in self.graph.edges())
            self.graph.del_edge(edge, double)
            assert bool(self.graph[edge[0]]) is False
            assert bool(self.graph[edge[1]]) is False
            self.assertFalse(edge in self.graph.edges())
            self.assertFalse((edge[1], edge[0]) in self.graph.edges())

    def test_edges_random(self):
        for i in xrange(self.repeats):
            self.graph = DictGraph()
            edges = set()
            for i in xrange(self.repeats):
                edges.add((randint(0, 49), randint(50, 100)))
            self.graph.add_edges(edges)
            gotedges = set(self.graph.edges())
            self.assertEqual(edges, gotedges)
            self.graph.del_edges(edges)
            gotedges = set(self.graph.edges())
            self.assertEqual(set([]), gotedges)

        for i in xrange(self.repeats):
            edges = set()
            alledges = set()
            for i in xrange(self.repeats):
                edge = (randint(0, 49), randint(50, 100))
                edges.add(edge)
                alledges.add(edge)
                alledges.add((edge[1], edge[0]))
            self.graph.add_edges(edges, double=True)
            gotedges = set(self.graph.edges())
            self.assertEqual(alledges, gotedges)
            self.graph.del_edges(edges, double=True)
            gotedges = set(self.graph.edges())
            self.assertEqual(set([]), gotedges)

    def test_edges_manual(self):
        self.graph.add_edge(('sum2', 'pwr'))
        self.graph.add_edge(('sum2', 'dbl'))
        self.graph.add_edge(('dbl', 'pwr'))
        self.graph.del_node('pwr')
        self.assertEqual({'sum2': {'dbl': {}}, 'dbl': {}}, self.graph)
        self.graph.del_node('dbl')
        self.assertRaises(KeyError, self.graph.del_node, 'pwr')
        self.assertEqual({'sum2': {}}, self.graph)
        self.graph.del_node('sum2')
        self.assertFalse(self.graph)
        self.graph.add_edge(('sum2', 'pwr'))
        self.graph.del_edge(('sum2', 'pwr'))
        self.assertEqual({'sum2': {}, 'pwr': {}}, self.graph)
        self.assertTrue(self.graph)

    def test_dfs(self):
        edges = [(1, 2), (3, 4), (5, 6), (1, 3), (1, 5), (1, 6), (2, 5)]
        self.graph.add_edges([(1, 2), (3, 4), (5, 6), (1, 3), (1, 5), (1, 6), (2, 5)])
        self.assertEqual(len(self.graph.dfs(1, [])), 6)
        self.graph.clear_nodes()
        self.assertEqual(len(self.graph.dfs(4, [])), 1)
        self.assertEqual(len(self.graph.dfs(1, [])), 5) # 4 is not clear
        self.graph.clear_nodes()
        self.assertEqual(len(self.graph.dfs(2, [])), 3)
        self.graph.clear_nodes()
        self.graph.add_edge((4, 1))
        a = []
        self.assertEqual(len(self.graph.dfs(1, a)), 6)
        self.assertEqual(a, [6, 5, 2, 4, 3, 1])

    def test_postorder1(self):
        edges = [(1, 2), (3, 4), (5, 6), (1, 5), (1, 6), (2, 5), (4, 6)]
        self.graph.add_edges(edges)
        self.graph.postorder()
        self.graph.clear_nodes()
        po1 = self.graph.postorder()
        self.graph.clear_nodes()
        po2 = self.graph.postorder()
        assert po1 == po2
        assert po2
        assert len(po2) == 6

    def test_postorder2(self):
        edges = [(1, 2), (2, 3), (3, 4), (1, 5), (5, 6), (5, 7), (1, 8), (8, 9), (8, 10)]
        # 1 - 2 - 3 - 4  
        #   - 5 - 6
        #       - 7
        # 1 - 8 - 9
        #       - 10
        self.graph.add_edges(edges)
        po = self.graph.postorder()
        assert po[-1] == 1
        assert po.index(10) < po.index(8)
        assert po.index(9) < po.index(8)
        assert po.index(7) < po.index(5)
        assert po.index(6) < po.index(5)
        assert po.index(4) < po.index(3)
        assert po.index(3) < po.index(2)


    def test_postorder3(self):
        for i in range(100):
            graph = DictGraph()
            edges = [(1, 2)]
            graph.add_node(3)
            graph.add_edges(edges)
            assert graph.postorder() == [2, 1, 3]

    def test_postorder3(self):
        for i in range(100):
            graph = DictGraph()
            edges1 = [(1, 'j'), ('j', 3), (3, 'z')]
            edges2 = [(1, 8), (8, 'k')]
            graph.add_edges(edges2)
            graph.add_edges(edges1)
            assert graph.postorder() == ['k', 8, 'z', 3, 'j', 1]

    def test_postorder4(self):
        edges = [(2, 1), (3, 1), (4, 3), (5, 3)]
        #     2
        # 4     - 1 
        #  - 3 
        # 5
        self.graph.add_edges(edges)
        po = self.graph.postorder()
        assert po[0] == 1
        assert po[-1] == 2
        assert po.index(4) > po.index(3)
        assert po.index(5) > po.index(3)

    def test_postorder5(self):
        for i in range(1000):
            edges = [(2, 1), (3, 1), (4, 3), (5, 3), (6, 4), (7, 4), (6, 5)]
            self.graph.add_edges(edges)
            self.graph[2].branch = 'C'
            self.graph[3].branch = 'D'
            self.graph[4].branch = 'A'
            self.graph[5].branch = 'B'
            self.graph[7].branch = 0
            self.graph[6].branch = 1
            assert self.graph.postorder() == [1, 2, 3, 4, 7, 5, 6]

    def test_postorder6(self):
        for i in range(1000):
            edges = [(2, 1), (3, 1), (4, 3), (5, 3), (6, 4), (7, 4), (6, 7), (6, 5)]
            self.graph.add_edges(edges)
            self.graph[2].branch = 'C'
            self.graph[3].branch = 'D'
            self.graph[4].branch = 'A'
            self.graph[5].branch = 'B'
            assert self.graph.postorder() == [1, 2, 3, 4, 7, 5, 6]

    def test_postorder7(self):
        for i in range(1000):
            edges = [(2, 1), (3, 1), (4, 3), (5, 3), (7, 4), (6, 7), (6, 5)]
            self.graph.add_edges(edges)
            self.graph[2].branch = 'C'
            self.graph[3].branch = 'D'
            self.graph[4].branch = 'A'
            self.graph[5].branch = 'B'
            assert self.graph.postorder() == [1, 2, 3, 4, 7, 5, 6]

    def test_node_rank1(self):
        edges = [(1, 2), (3, 4), (5, 6), (1, 5), (1, 6), (2, 5), (4, 6)]
        self.graph.add_edges(edges)
        assert self.graph.node_rank() == {1: 3, 2: 2, 3: 2, 4: 1, 5: 1, 6: 0}

    def test_node_rank2(self):
        edges = [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (1, 6)]
        self.graph.add_edges(edges)
        assert self.graph.node_rank() == {1: 5, 2: 4, 3: 3, 4: 2, 5: 1, 6: 0}

    def test_node_width1(self):
        edges = [(1, 2), (1, 3), (1, 4), (2, 5), (3, 5), (3, 6), \
                 (2, 7), (4, 7), (5, 7), (6, 7)]
        self.graph.add_edges(edges)
        self.graph.node_width()

    def test_rank_width1(self):
        edges = [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (1, 7), (7, 6)]
        self.graph.add_edges(edges)
        assert self.graph.rank_width() == {0: 1, 1: 2, 2: 1, 3: 1, 4: 1, 5: 1}

    def test_rank_width2(self):
        edges = [(1, 2), (1, 3), (1, 4), (1, 5), (5, 7), \
                 (7, 6), (2, 6), (3, 6), (4, 6)]
        self.graph.add_edges(edges)
        assert self.graph.rank_width() == {0: 1, 1: 4, 2: 1, 3: 1}




if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
