# -*- coding: utf-8 -*-

import unittest
import os
import rdflib
import psycopg2
from psycopg2.extensions import \
    ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED
from rdflib import plugin, URIRef, Literal
from rdflib.store import Store
from rdflib.graph import Graph, ConjunctiveGraph

storename = 'hstore'
storetest = True
connection_uri = os.environ.get(
    'DBURI',
    'postgresql://unittest@localhost/rdflibhstore_test')

alice = URIRef("alice")
bob = URIRef("bob")
michel = URIRef(u'michel')
tarek = URIRef(u'tarek')
bob = URIRef(u'bob')
likes = URIRef(u'likes')
hates = URIRef(u'hates')
pizza = URIRef(u'pizza')
cheese = URIRef(u'cheese')
says = URIRef(u'says')
hello = Literal(u'hello', lang='en')
konichiwa = Literal(u'こんにちは', lang='ja')
something = Literal(u'something')        


class BaseCase(unittest.TestCase):

    def execute(self, command):
        if command in [ 'CREATE', 'DROP' ]:
            server, dbname = connection_uri.rsplit('/',1) 
            c = psycopg2.connect('{}/template1'.format(server))
            try:
                c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                c.cursor().execute('{} DATABASE {}'.format(command, dbname))
                c.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
            finally:
                c.close()
    
    def setUp(self):
        self.execute('CREATE')
        plugin.register(
            'hstore', Store, 'rdflib_hstore.hstorestore', 'HstoreStore')
        self.graphs = []

    def tearDown(self):
        for g in self.graphs:
            g.destroy(connection_uri)
            g.close()
        self.execute('DROP')

class TestHstoreGraph(BaseCase):

    def open_graph(self):
        graph = Graph(store='hstore')
        graph.open(connection_uri, create=True)
        self.graphs.append(graph)
        return graph

    def add_stuff(self, graph):
        graph.add((tarek, likes, pizza))
        graph.add((tarek, likes, cheese))
        graph.add((michel, likes, pizza))
        graph.add((michel, likes, cheese))
        graph.add((bob, likes, cheese))
        graph.add((bob, hates, pizza))
        graph.add((bob, hates, michel))
        graph.add((bob, says, hello))
        graph.add((bob, says, konichiwa))
        graph.add((bob, says, something))

    def remove_stuff(self, graph):
        graph.remove((tarek, likes, pizza))
        graph.remove((tarek, likes, cheese))
        graph.remove((michel, likes, pizza))
        graph.remove((michel, likes, cheese))
        graph.remove((bob, likes, cheese))
        graph.remove((bob, hates, pizza))
        graph.remove((bob, hates, michel))
        graph.remove((bob, says, hello))
        graph.remove((bob, says, konichiwa))
        graph.remove((bob, says, something))        

    def test_namespaces(self):
        graph = self.open_graph()
        graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
        graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
        self.assertTrue(len(list(graph.namespaces())) == 6)
        self.assertTrue(
            ('foaf', rdflib.term.URIRef(u'http://xmlns.com/foaf/0.1/'))
            in list(graph.namespaces()))

    def test_fresh_db(self):
        graph = self.open_graph()
        ntriples = graph.triples((None, None, None))
        self.assertTrue(len(list(ntriples)) == 0)

    def test_add_triples(self):
        graph = self.open_graph()
        self.add_stuff(graph)
        self.assertTrue(len(list(graph.triples((None, None, None)))) == 10)

    def test_remove_triples(self):
        graph = self.open_graph()
        self.add_stuff(graph)
        self.assertTrue(len(list(graph.triples((None, None, None)))) == 10)
        self.remove_stuff(graph)
        self.assertTrue(len(list(graph.triples((None, None, None)))) == 0)

    def test_triples(self):
        graph = self.open_graph()
        self.add_stuff(graph)
        asserte = self.assertEquals

        # unbound subjects
        asserte(len(list(graph.triples((None, likes, pizza)))), 2)
        asserte(len(list(graph.triples((None, hates, pizza)))), 1)
        asserte(len(list(graph.triples((None, likes, cheese)))), 3)
        asserte(len(list(graph.triples((None, hates, cheese)))), 0)

        # unbound objects
        asserte(len(list(graph.triples((michel, likes, None)))), 2)
        asserte(len(list(graph.triples((tarek, likes, None)))), 2)
        asserte(len(list(graph.triples((bob, hates, None)))), 2)
        asserte(len(list(graph.triples((bob, likes, None)))), 1)

        # unbound predicates
        asserte(len(list(graph.triples((michel, None, cheese)))), 1)
        asserte(len(list(graph.triples((tarek, None, cheese)))), 1)
        asserte(len(list(graph.triples((bob, None, pizza)))), 1)
        asserte(len(list(graph.triples((bob, None, michel)))), 1)

        # unbound subject, objects
        asserte(len(list(graph.triples((None, hates, None)))), 2)
        asserte(len(list(graph.triples((None, likes, None)))), 5)

        # unbound predicates, objects
        asserte(len(list(graph.triples((michel, None, None)))), 2)
        asserte(len(list(graph.triples((bob, None, None)))), 6)
        asserte(len(list(graph.triples((tarek, None, None)))), 2)

        # unbound subjects, predicates
        asserte(len(list(graph.triples((None, None, pizza)))), 3)
        asserte(len(list(graph.triples((None, None, cheese)))), 3)
        asserte(len(list(graph.triples((None, None, michel)))), 1)
        asserte(len(list(graph.triples((None, None, konichiwa)))), 1)

        # all unbound
        asserte(len(list(graph.triples((None, None, None)))), 10)
        self.remove_stuff(graph)
        asserte(len(list(graph.triples((None, None, None)))), 0)

    def test_statement_node(self):
        from rdflib import RDF
        from rdflib.term import Statement

        graph = self.open_graph()
        c = URIRef("http://example.org/foo#c")
        r = URIRef("http://example.org/foo#r")
        s = Statement((michel, likes, pizza), c)
        graph.add((s, RDF.value, r))
        self.assertEquals(r, graph.value(s, RDF.value))
        self.assertEquals(s, graph.value(predicate=RDF.value, object=r))

    def test_connected(self):
        graph = self.open_graph()
        self.add_stuff(graph)
        self.assertEquals(True, graph.connected())

        jeroen = URIRef("jeroen")
        unconnected = URIRef("unconnected")
        graph.add((jeroen, likes, unconnected))
        self.assertEquals(False, graph.connected())

    def test_graph_value(self):
        from rdflib import RDF
        from rdflib.graph import GraphValue
        
        graph = self.open_graph()

        g1 = Graph()
        g1.add((alice, likes, pizza))
        g1.add((bob, likes, cheese))
        g1.add((bob, likes, pizza))

        g2 = Graph()
        g2.add((bob, likes, pizza))
        g2.add((bob, likes, cheese))
        g2.add((alice, likes, pizza))

        gv1 = GraphValue(store=graph.store, graph=g1)
        gv2 = GraphValue(store=graph.store, graph=g2)
        graph.add((gv1, RDF.value, gv2))
        self.assertEquals(gv2, graph.value(gv1))

        graph.remove((gv1, RDF.value, gv2))

    def test_reopening_db(self):
        graph = self.open_graph()
        self.add_stuff(graph)
        graph.store.close()
        graph.store.open(connection_uri, create=False)
        self.assertTrue(len(list(graph.triples((None, None, None)))) == 10)

    def test_opening_missing_db(self):
        with self.assertRaises(StandardError):
            graph.open(
                'postgresql://unittest@localhost/nosuchdb',
                create=False)

class TestHstoreConjunctiveGraph(BaseCase):

    def open_graph(self):
        graph = ConjunctiveGraph(store='hstore')
        graph.open(connection_uri, create=True)
        self.graphs.append(graph)
        return graph

    def test_namespaces(self):
        graph = self.open_graph()
        graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
        graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
        self.assertTrue(len(list(graph.namespaces())) == 6)
        self.assertTrue(
            ('foaf', rdflib.term.URIRef(u'http://xmlns.com/foaf/0.1/'))
            in list(graph.namespaces()))

    def test_triples_context_reset(self):
        graph = self.open_graph()
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        graph.add((michel, likes, pizza))
        graph.add((michel, likes, cheese))
        ntriples = graph.triples(
            (None, None, None), context=graph.store)
        self.assertTrue(len(list(ntriples)) == 2)

    def test_remove_context_reset(self):
        graph = self.open_graph()
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        graph.add((michel, likes, pizza))
        graph.add((michel, likes, cheese))
        graph.store.remove((michel, likes, cheese), graph.store)
        self.assertTrue(len(list(graph.triples(
            (None, None, None), context=graph.store))) == 1)

