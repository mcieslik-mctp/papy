# -*- coding: utf-8 -*-
"""
:mod:`papy.graph`
=================

This module implements a graph data structure without explicit edges, using 
nested Python dictionaries. It provides ``DictNode`` and ``DictGraph``.

"""
from collections import defaultdict
from itertools import repeat, izip


class DictNode(dict):
    """ 
    ``DictNode`` is the **topological node** of a ``DictGraph``. Please note 
    that the  **node object** is not the same as the **topological node**. The 
    **node  object** is any hashable Python ``object``. The **topological node**
    is defined for each **node object** and is a dictionary of other **node 
    objects** with incoming **edges** from a single **node object**.
    
    A node has: ``"discovered"``, ``"examined"`` and ``"branch"`` attributes.
    
    Arguments:
    
      - entity (``object``) [default: ``None``] Any hashable ``object`` is a 
        valid **node object**.
      - xtra (``dict``) [default: ``None``] A dictionary of arbitrary properties
        of the **topological node**.
      
    """
    def __init__(self, entity=None, xtra=None):
        self.discovered = False
        self.examined = False
        self.branch = None
        try:
            if entity is not None:
                dict.__init__(self, {entity:DictNode(xtra=xtra)})
            else:
                self.xtra = (xtra or {})
        except Exception, excp:
            raise excp

    def clear(self):
        """
        Sets the ``"discovered"`` and ``"examined"`` attributes to ``False``.
        
        """
        self.discovered = False
        self.examined = False

    def nodes(self):
        """
        Returns a list of **node objects** directly connected from this 
        **topological node**.
        
        """
        return self.keys()

    def iternodes(self):
        """
        Returns an iterator of **node objects** directly connected from this 
        **topological node**.
        
        """
        return self.iterkeys()

    def deep_nodes(self, allnodes=None):
        """
        A recursive method to return a ``list`` of *all* **node objects** 
        connected from this **toplogical node**.
        
        """
        allnodes = (allnodes or [])
        for (node, node_) in ((node, node_) for (node, node_) in\
            self.iteritems() if not node in allnodes):
            allnodes.append(node)
            node_.deep_nodes(allnodes)
        return allnodes


class DictGraph(dict):
    """
    A dictionary-based graph data structure. This graph implementation is a
    little bit unusual as it does not explicitly hold a list of edges. A 
    ``DictGraph`` instance is a dictionary, where the keys of the dictionary are 
    hashable ``object`` instances (**node objects**), while the values are 
    ``DictNode`` instances (**topological nodes**). A ``DictNode`` instance is 
    also a dictionary, where the keys are **node objects** and the values are 
    ``DictNode`` instances. A ``Node`` instance (value) is basically a 
    dictionary of outgoing edges from the **node object** (key). The edges are
    indexed by the incoming objects. So we end up with a single recursivly 
    nested dictionary which defines the topology of the ``DictGraph`` instance.
    An edge is a tuple of two **node objects**.
    
    Arguments:
        
        - nodes(iterable) [default: ``()``] A sequence of **node objects** 
          to be added to the graph. See: ``Graph.add_nodes``
        - edges(iterable) [default: ``()``] A sequence of edges to be added to 
          the graph. See: ``Graph.add_edges``  
        - xtras(iterable) [default: ``None``] A sequence of property 
          dictionaries for the added **node objects**. The **topological nodes**
          corresponding to the added **node objects** will have their 
          ``Node.xtra`` attributes updated with the contents of this sequence. 
          Either all or no ``"xtra"`` dictionaries must to be given.
          
    """
    def __init__(self, nodes=(), edges=(), xtras=None):
        self.add_nodes(nodes, xtras)
        self.add_edges(edges)
        dict.__init__(self)

    def cmp_branch(self, node1, node2):
        """
        comparison of **node objects** based on the ``"branch"`` attribute of 
        their **topological nodes**.
        
        """
        # note reverse
        return cmp(getattr(self[node2], "branch", None), \
                   getattr(self[node1], "branch", None))

    def dfs(self, node, bucket=None, order="append"):
        """
        Recursive depth first search. By default ("order" = ``"append"``) this 
        returns the **node objects** in the reverse postorder. To change this 
        into the preorder use a ``collections.deque`` as "bucket" and 
        ``"appendleft"`` as "order".
            
        Arguments:

          - bucket(``list`` or ``collections.dequeue``) [default: ``None``] The
            user *must* provide a new ``list`` or ``collections.dequeue`` to 
            store the nodes.
          - order(``str``) [default: ``"append"``] Method of the "bucket" which 
            will be called with the **node object** that has been examined. 
            Other valid options might be ``"appendleft"`` for a 
            ``collections.dequeue``.
        
        """
        if self[node].discovered:
            return bucket
        self[node].discovered = True
        nodes_ = sorted(self[node].iternodes(), cmp=self.cmp_branch)
        for node_ in nodes_:
            self.dfs(node_, bucket, order)
        getattr(bucket, order)(node)
        self[node].examined = True
        return bucket

    def postorder(self):
        """
        Returns a valid postorder of the **node objects** of the ``DictGraph`` 
        *if* the topology is a directed acyclic graph. This postorder is 
        semi-random, because the order of elements in a dictionary is 
        semi-random and so are the starting nodes of the depth-first search 
        traversal, which determines the postorder, consequently some postorders
        will be discovered more frequently.
        
        This postorder enforces some determinism on particular ties:
        
          - toplogically equivalent branches come first are sorted by length
            (shorter branches come first).
          - if the topological Nodes corresponding to the node objects have
            a ``"branch"`` attribute it will be used to sort the graph from 
            left to right.
              
        However the final postorder is still *not* deterministic.
        
        """
        nodes_random = self.nodes()
        # for debugging we could make it more random;)
        # from random import shuffle
        # shuffle(nodes_random)
        # 1. sort branches
        nodes_by_branch = sorted(nodes_random, cmp=self.cmp_branch)
        # 1. topological sort
        nodes_topological = []
        for node in nodes_by_branch:
            self.dfs(node, nodes_topological)
        self.clear_nodes()
        # 2. earthworm sort
        nodes_consecutive = []
        for node in nodes_topological:
            Node = self[node]
            outgoing_nodes = Node.nodes()
            if outgoing_nodes:
                last_index = max([nodes_consecutive.index(on) for on in \
                                   outgoing_nodes])
                nodes_consecutive.insert(last_index + 1, node)
            else:
                nodes_consecutive.append(node)
        return nodes_consecutive

    def node_rank(self):
        """
        Returns the maximum rank for each **topological node** in the 
        ``DictGraph``. The rank of a node is defined as the number of edges 
        between the node and a node which has rank 0. A **topological node** 
        has rank 0 if it has no incoming edges.
        
        """
        nodes = self.postorder()
        node_rank = {}
        for node in nodes:
            max_rank = 0
            for child in self[node].nodes():
                some_rank = node_rank[child] + 1
                max_rank = max(max_rank, some_rank)
            node_rank[node] = max_rank
        return node_rank

    def node_width(self):
        """
        Returns the width of each node in the graph. #TODO
        
        """
        nodes = self.postorder()
        node_width = {}
        for node in nodes:
            sum_width = 0
            for child in self[node].nodes():
                sum_width += node_width[child]
            node_width[node] = (sum_width or 1)
        return node_width

    def rank_width(self):
        """
        Returns the width of each rank in the graph. #TODO
        
        """
        rank_width = defaultdict(int)
        node_rank = self.node_rank()
        for rank in node_rank.values():
            rank_width[rank] += 1
        return dict(rank_width)


    def add_node(self, node, xtra=None, branch=None):
        """
        Adds a **node object** to the ``DictGraph``. Returns ``True`` if a 
        new **node object** has been added. If the **node object** is already in
        the ``DictGraph`` returns ``False``.
        
        Arguments:
        
          - node(``object``) Node to be added. Any hashable Python ``object``.
          - xtra(``dict``) [default: ``None``] The newly created topological 
            ``Node.xtra`` dictionary will be updated with the contents of this 
            dictionary. 
          - branch(``object``) [default: ``None``] an identificator used to 
            sort topologically equivalent branches.
                
        """
        if not node in self:
            node_ = DictNode(node, xtra)
            self.update(node_)
            self[node].branch = (branch or getattr(node, "branch", None))
            return True
        return False

    def del_node(self, node):
        """
        Removes a **node object** from the ``DictGraph``. Returns ``True`` if a 
        **node object** has been removed. If the **node object** is not in the
        ``DictGraph`` raises a ``KeyError``.
        
        Arguments:
            
          - node(``object``) **node object** to be removed. Any hashable Python 
            ``object``.
           
        """
        for node_ in self.values():
            if node in node_:
                node_.pop(node)
        return bool(self.pop(node))

    def add_edge(self, edge, double=False):
        """
        Adds an edge to the ``DictGraph``. An edge is just a pair of **node 
        objects**. If the **node objects** are not in the graph they are 
        created.
        
        Arguments:
        
          - edge(iterable) An ordered pair of **node objects**. The edge is 
            assumed to have a direction from the first to the second **node 
            object**.
          - double(``bool``) [default: ``False```] If ``True`` the the reverse 
            edge is also added.
        
        """
        (left_entity, right_entity) = edge
        self.add_node(left_entity)
        self.add_node(right_entity)
        self[left_entity].update({right_entity:self[right_entity]})
        if double:
            self.add_edge((edge[1], edge[0]))

    def del_edge(self, edge, double=False):
        """
        Removes an edge from the ``DictGraph``. An edge is a pair of **node 
        objects**. The **node objects** are not removed from the ``DictGraph``.
        
        Arguments:
        
          - edge(``tuple``) An ordered pair of **node objects**. The edge is 
            assumed to have a direction from the first to the second **node 
            object**.
          - double(``bool``) [default: ``False```] If ``True`` the the reverse
            edge is also removed.
            
        """
        (left_entity, right_entity) = edge
        self[left_entity].pop(right_entity)
        if double:
            self.del_edge((edge[1], edge[0]))

    def add_nodes(self, nodes, xtras=None):
        """
        Adds **node objects** to the graph.
        
        Arguments:
        
          - nodes(iterable) Sequence of **node objects** to be added to the 
            ``DictGraph``
                
          - xtras(iterable) [default: ``None``] Sequence of ``Node.xtra`` 
            dictionaries corresponding to the **node objects** being added. 
            See: ``Graph.add_node``.
            
        """
        for node, xtra in izip(nodes, (xtras or repeat(None))):
            self.add_node(node, xtra)

    def del_nodes(self, nodes):
        """
        Removes **node objects** from the graph.
        
        Arguments:
        
          - nodes(iterable) Sequence of **node objects** to be removed from the
            ``DictGraph``. See: ``DictGraph.del_node``.
            
        """
        for node in nodes:
            self.del_node(node)

    def add_edges(self, edges, *args, **kwargs):
        """
        Adds edges to the graph. Takes optional arguments for 
        ``DictGraph.add_edge``.
        
        Arguments:
        
          - edges(iterable) Sequence of edges to be added to the 
            ``DictGraph``.
          
        """
        for edge in edges:
            self.add_edge(edge, *args, **kwargs)

    def del_edges(self, edges, *args, **kwargs):
        """
        Removes edges from the graph. Takes optional arguments for 
        ``DictGraph.del_edge``.
        
        Arguments:
        
          - edges(iterable) Sequence of edges to be removed from the 
            ``DictGraph``.
            
        """
        for edge in edges:
            self.del_edge(edge, *args, **kwargs)

    def nodes(self):
        """
        Returns a list of all **node objects** in the ``DictGraph``.
        
        """
        return self.keys()

    def iter_nodes(self):
        """
        Returns an iterator of all **node objects** in the ``DictGraph``.
        
        """
        return self.iterkeys()

    def clear_nodes(self):
        """
        Clears all nodes in the *Graph*. See ``Node.clear``.
        
        """
        for node in self.itervalues():
            node.clear()

    def deep_nodes(self, node):
        """
        Returns all reachable **node objects** from a **node object**. See: 
        ``DictNode.deep_nodes``.
        
        Arguments:
        
          - node(``object``) a **node object** present in the graph.
        
        """
        return self[node].deep_nodes()

    def edges(self, nodes=None):
        """
        Returns a ``tuple`` of all edges in the ``DictGraph`` an edge is a pair
        of **node objects**.
        
        Arguments:
        
          - nodes(iterable) [default: ``None``] iterable of **node objects** if 
            specified the edges will be limited to those outgoing from one of 
            the specified nodes.
            
        """
        # If a Node has been directly updated (__not__ recommended)
        # then the Graph will not know the added nodes and therefore will
        # miss half of their edges.
        edges = set()
        for node in (nodes or self.iterkeys()):
            ends = self[node].nodes()
            edges.update([(node, end) for end in ends])
        return tuple(edges)

    def incoming_edges(self, node):
        """
        Returns a ``tuple`` of incoming edges for a **node object**.
        
        Arguments:
        
          - node(``object``) **node object** present in the graph to be queried 
            for incoming edges.
          
        """
        edges = self.edges()
        in_edges = []
        for out_node, in_node in edges:
            if node is in_node:
                in_edges.append((out_node, in_node))
        return tuple(in_edges)

    def outgoing_edges(self, node):
        """
        Returns a ``tuple`` of outgoing edges for a **node object**.
        
        Arguments:
        
          - node(``object``) **node object** present in the graph to be queried 
            for outgoing edges.
          
        """
        #TODO: pls make outgoig_edges less insane
        edges = self.edges()
        out_edges = []
        for out_node, in_node in edges:
            if node is out_node:
                out_edges.append((out_node, in_node))
        return tuple(out_edges)
