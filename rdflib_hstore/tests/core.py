# -*- coding: utf-8 -*-

import unittest
import os
import rdflib
import psycopg2
from psycopg2.extensions import \
    ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED
from rdflib import plugin, RDF, RDFS, URIRef, Literal, BNode, Variable
from rdflib.store import Store
from rdflib.graph import Graph, ConjunctiveGraph, QuotedGraph

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
context1 = URIRef(u'context-1')
context2 = URIRef(u'context-2')

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


class TestHstoreGraph(BaseCase):

    def open_graph(self):
        graph = Graph(store='hstore')
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


class ContextTestCase(BaseCase):

    def open_graph(self):
        graph = ConjunctiveGraph(store='hstore')
        graph.open(connection_uri, create=True)
        self.graphs.append(graph)
        return graph

    def get_context(self, store, identifier):
        assert (isinstance(identifier, URIRef) or 
                isinstance(identifier, BNode)), type(identifier)
        return Graph(store=store, identifier=identifier, namespace_manager=self)

    def add_stuff_in_multiple_contexts(self, graph):
        triple = (pizza, hates, tarek)
        # add to default context
        graph.add(triple)
        # add to context 1
        g1 = Graph(graph.store, context1)
        g1.add(triple)
        # add to context 2
        g2 = Graph(graph.store, context2)
        g2.add(triple)

    def test_conjunction(self):
        graph = self.open_graph()
        self.add_stuff_in_multiple_contexts(graph)
        triple = (pizza, likes, pizza)
        # add to context 1
        g1 = Graph(graph.store, context1)
        g1.add(triple)
        self.assertEquals(len(graph), len(g1))

    def test_len_in_one_context(self):
        graph = self.open_graph()
        old_len = len(graph)
        g1 = self.get_context(graph.store, context1)

        for i in range(0, 10):
            g1.add((BNode(), hates, hates))
        self.assertEquals(len(g1), old_len + 10)
        self.assertEquals(len(self.get_context(graph.store, context1)), old_len + 10)
        
        graph.remove_context(self.get_context(graph.store, context1))
        self.assertEquals(len(graph), old_len)
        self.assertEquals(len(g1), 0)

    def test_len_in_multiple_contexts(self):
        graph = self.open_graph()
        old_len = len(graph)
        self.add_stuff_in_multiple_contexts(graph)

        #  add_stuff_in_multiple_contexts is adding the same triple to
        # three different contexts. So it's only + 1
        self.assertEquals(len(graph), old_len + 1)
        self.assertEquals(len(self.get_context(graph.store, context1)), old_len + 1)

    def test_remove_in_multiple_contexts(self):
        graph = self.open_graph()
        self.add_stuff_in_multiple_contexts(graph)

        # triple should be still in store after removing it from context1 + context2
        triple = (pizza, hates, tarek)
        self.assertTrue(triple in graph)

        g1 = self.get_context(graph.store, context1)
        g1.remove(triple)
        self.assertTrue(triple in graph)

        g2 = self.get_context(graph.store, context2)
        g2.remove(triple)
        self.assertTrue(triple in graph)

        graph.remove(triple)
        # now gone!
        self.assertTrue(triple not in graph)

        # add again and see if remove without context removes all triples!
        self.add_stuff_in_multiple_contexts(graph)
        graph.remove(triple)
        self.assertTrue(triple not in graph)

    def test_contexts(self):
        graph = self.open_graph()
        self.add_stuff_in_multiple_contexts(graph)

        self.assertTrue(
            context1 in [g.identifier for g in graph.contexts()])
        self.assertTrue(
            context2 in [g.identifier for g in graph.contexts()])

        triple = (pizza, hates, tarek)
        g2 = self.get_context(graph.store, context2)
        g2.remove(triple)
        self.assertTrue(
            context2 in [g.identifier for g in graph.contexts()])
        self.assertFalse(
            context2 in [g.identifier for g in graph.contexts(triple)])

    def test_remove_context(self):
        graph = self.open_graph()
        self.add_stuff_in_multiple_contexts(graph)

        self.assertTrue(
            context1 in [g.identifier for g in graph.contexts()])
        graph.remove_context(self.get_context(graph.store, context1))
        self.assertFalse(
            context1 in [g.identifier for g in graph.contexts()])

    def test_remove_any(self):
        graph = self.open_graph()
        self.add_stuff_in_multiple_contexts(graph)
        graph.remove((None,None,None))
        self.assertEquals(len(graph), 0)

    def test_triples(self):
        graph = self.open_graph()
        self.add_stuff(self.get_context(graph.store, context1))

        asserte = self.assertEquals
        triples = graph.triples
        context1_triples = self.get_context(graph.store, context1).triples

        # unbound subjects with context
        asserte(len(list(context1_triples((None, likes, pizza)))), 2)
        asserte(len(list(context1_triples((None, hates, pizza)))), 1)
        asserte(len(list(context1_triples((None, likes, cheese)))), 3)
        asserte(len(list(context1_triples((None, hates, cheese)))), 0)

        # unbound subjects without context, same results!
        asserte(len(list(triples((None, likes, pizza)))), 2)
        asserte(len(list(triples((None, hates, pizza)))), 1)
        asserte(len(list(triples((None, likes, cheese)))), 3)
        asserte(len(list(triples((None, hates, cheese)))), 0)

        # unbound objects with context
        asserte(len(list(context1_triples((michel, likes, None)))), 2)
        asserte(len(list(context1_triples((tarek, likes, None)))), 2)
        asserte(len(list(context1_triples((bob, hates, None)))), 2)
        asserte(len(list(context1_triples((bob, likes, None)))), 1)

        # unbound objects without context, same results!
        asserte(len(list(triples((michel, likes, None)))), 2)
        asserte(len(list(triples((tarek, likes, None)))), 2)
        asserte(len(list(triples((bob, hates, None)))), 2)
        asserte(len(list(triples((bob, likes, None)))), 1)

        # unbound predicates with context
        asserte(len(list(context1_triples((michel, None, cheese)))), 1)
        asserte(len(list(context1_triples((tarek, None, cheese)))), 1)
        asserte(len(list(context1_triples((bob, None, pizza)))), 1)
        asserte(len(list(context1_triples((bob, None, michel)))), 1)

        # unbound predicates without context, same results!
        asserte(len(list(triples((michel, None, cheese)))), 1)
        asserte(len(list(triples((tarek, None, cheese)))), 1)
        asserte(len(list(triples((bob, None, pizza)))), 1)
        asserte(len(list(triples((bob, None, michel)))), 1)

        # unbound subject, objects with context
        asserte(len(list(context1_triples((None, hates, None)))), 2)
        asserte(len(list(context1_triples((None, likes, None)))), 5)

        # unbound subject, objects without context, same results!
        asserte(len(list(triples((None, hates, None)))), 2)
        asserte(len(list(triples((None, likes, None)))), 5)

        # unbound predicates, objects with context
        asserte(len(list(context1_triples((michel, None, None)))), 2)
        asserte(len(list(context1_triples((bob, None, None)))), 6)
        asserte(len(list(context1_triples((tarek, None, None)))), 2)

        # unbound predicates, objects without context, same results!
        asserte(len(list(triples((michel, None, None)))), 2)
        asserte(len(list(triples((bob, None, None)))), 6)
        asserte(len(list(triples((tarek, None, None)))), 2)

        # unbound subjects, predicates with context
        asserte(len(list(context1_triples((None, None, pizza)))), 3)
        asserte(len(list(context1_triples((None, None, cheese)))), 3)
        asserte(len(list(context1_triples((None, None, michel)))), 1)

        # unbound subjects, predicates without context, same results!
        asserte(len(list(triples((None, None, pizza)))), 3)
        asserte(len(list(triples((None, None, cheese)))), 3)
        asserte(len(list(triples((None, None, michel)))), 1)

        # all unbound with context
        asserte(len(list(context1_triples((None, None, None)))), 10)
        # all unbound without context, same result!
        asserte(len(list(triples((None, None, None)))), 10)

        for c in [graph, self.get_context(graph.store, context1)]:
            # unbound subjects
            asserte(set(c.subjects(likes, pizza)), set((michel, tarek)))
            asserte(set(c.subjects(hates, pizza)), set((bob,)))
            asserte(set(c.subjects(likes, cheese)), set([tarek, bob, michel]))
            asserte(set(c.subjects(hates, cheese)), set())

            # unbound objects
            asserte(set(c.objects(michel, likes)), set([cheese, pizza]))
            asserte(set(c.objects(tarek, likes)), set([cheese, pizza]))
            asserte(set(c.objects(bob, hates)), set([michel, pizza]))
            asserte(set(c.objects(bob, likes)), set([cheese]))

            # unbound predicates
            asserte(set(c.predicates(michel, cheese)), set([likes]))
            asserte(set(c.predicates(tarek, cheese)), set([likes]))
            asserte(set(c.predicates(bob, pizza)), set([hates]))
            asserte(set(c.predicates(bob, michel)), set([hates]))

            asserte(set(
                c.subject_objects(hates)), set([(bob, pizza), (bob, michel)]))
            asserte(
                set(c.subject_objects(likes)), set(
                    [(tarek, cheese),
                     (michel, cheese),
                     (michel, pizza),
                     (bob, cheese),
                     (tarek, pizza)]))

            asserte(set(c.predicate_objects(
                michel)), set([(likes, cheese), (likes, pizza)]))
            asserte(set(c.predicate_objects(bob)), set([
                        (likes, cheese), 
                        (hates, pizza), 
                        (hates, michel),
                        (says, hello),
                        (says, konichiwa),
                        (says, something),
                        ]))
            asserte(set(c.predicate_objects(
                tarek)), set([(likes, cheese), (likes, pizza)]))

            asserte(set(c.subject_predicates(
                pizza)), set([(bob, hates), (tarek, likes), (michel, likes)]))
            asserte(set(c.subject_predicates(cheese)), set([(
                bob, likes), (tarek, likes), (michel, likes)]))
            asserte(set(c.subject_predicates(michel)), set([(bob, hates)]))

            asserte(set(c), set(
                [(bob, hates, michel),
                 (bob, likes, cheese),
                 (tarek, likes, pizza),
                 (michel, likes, pizza),
                 (michel, likes, cheese),
                 (bob, hates, pizza),
                 (tarek, likes, cheese),
                 (bob, says, hello),
                 (bob, says, konichiwa),
                 (bob, says, something),
                 ]))

        # remove stuff and make sure the graph is empty again
        self.remove_stuff(graph)
        asserte(len(list(context1_triples((None, None, None)))), 0)
        asserte(len(list(triples((None, None, None)))), 0)


class FormulaTestCase(BaseCase):

    def open_graph(self):
        graph = ConjunctiveGraph(store='hstore')
        graph.open(connection_uri, create=True)
        self.graphs.append(graph)
        return graph

    def test_n3_store(self):
        # Thorough test suite for formula-aware store

        implies = URIRef("http://www.w3.org/2000/10/swap/log#implies")
        testN3 = """
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test/> .
{:a :b :c;a :foo} => {:a :d :c,?y}.
_:foo a rdfs:Class.
:a :d :c."""

        g = self.open_graph()
        g.parse(data=testN3, format="n3")

        formulaA = BNode()
        formulaB = BNode()
        for s,o in g.subject_objects(predicate=implies):
            formulaA = s
            formulaB = o
        assert type(formulaA)==QuotedGraph and type(formulaB)==QuotedGraph

        a = URIRef('http://test/a')
        b = URIRef('http://test/b')
        c = URIRef('http://test/c')
        d = URIRef('http://test/d')
        v = Variable('y')
        
        universe = ConjunctiveGraph(g.store)
        
        # test formula as terms
        assert len(list(universe.triples((formulaA, implies, formulaB)))) == 1
        
        # test variable as term and variable roundtrip
        assert len(list(formulaB.triples((None,None,v)))) == 1
        for s,p,o in formulaB.triples((None,d,None)):
            if o != c:
                assert isinstance(o, Variable)
                assert o == v

        s = list(universe.subjects(RDF.type, RDFS.Class))[0]
        assert isinstance(s, BNode)
        assert len(list(universe.triples((None,implies,None)))) == 1
        assert len(list(universe.triples((None,RDF.type,None)))) == 1

        assert len(list(formulaA.triples((None,RDF.type,None)))) == 1
        assert len(list(formulaA.triples((None,None,None)))) == 2

        assert len(list(formulaB.triples((None,None,None)))) == 2
        assert len(list(formulaB.triples((None,d,None)))) == 2

        assert len(list(universe.triples((None,None,None)))) == 3 
        assert len(list(universe.triples((None,d,None)))) == 1
        
        # context tests
        # test contexts with triple argument
        assert len(list(universe.contexts((a,d,c))))==1
        
        # remove test cases
        universe.remove((None,implies,None))
        assert len(list(universe.triples((None,implies,None)))) == 0
        assert len(list(formulaA.triples((None,None,None)))) == 2
        assert len(list(formulaB.triples((None,None,None)))) == 2
        
        formulaA.remove((None,b,None))
        assert len(list(formulaA.triples((None,None,None)))) == 1

        formulaA.remove((None,RDF.type,None))
        assert len(list(formulaA.triples((None,None,None)))) == 0
        
        universe.remove((None,RDF.type,RDFS.Class))
        
        # remove_context tests
        universe.remove_context(formulaB)
        assert len(list(universe.triples((None,RDF.type,None)))) == 0
        assert len(universe) == 1
        assert len(formulaB) == 0
        
        universe.remove((None,None,None))
        assert len(universe) == 0




