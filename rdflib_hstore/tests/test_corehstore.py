import unittest
import os
import rdflib
from rdflib import plugin
from rdflib.store import Store
from rdflib.graph import Graph, ConjunctiveGraph

storename = 'hstore'
storetest = True
connection_uri = os.environ.get(
    'DBURI',
    'postgresql://unittest@localhost/rdflibhstore_test')

class TestHstoreGraphCore(unittest.TestCase):

    def setUp(self):
        plugin.register(
            'hstore', Store, 'rdflib_hstore.hstorestore', 'HstoreStore')
        store = 'hstore'
        self.graph = Graph(store=store)
        self.path = connection_uri
        self.graph.open(self.path, create=True)

    def tearDown(self):
        self.graph.destroy(self.path)
        try:
            self.graph.close()
        except:
            pass

    def test_namespaces(self):
        self.graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
        self.graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
        self.assertTrue(len(list(self.graph.namespaces())) == 6)
        self.assertTrue(
            ('foaf', rdflib.term.URIRef(u'http://xmlns.com/foaf/0.1/'))
            in list(self.graph.namespaces()))

    def test_fresh_db(self):
        ntriples = self.graph.triples((None, None, None))
        self.assertTrue(len(list(ntriples)) == 0)

    def test_add_triples(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        ntriples = self.graph.triples((None, None, None))
        self.assertTrue(len(list(ntriples)) == 2)

    def test_reopening_db(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        self.graph.store.close()
        self.graph.store.open(self.path, create=False)
        ntriples = self.graph.triples((None, None, None))
        self.assertTrue(len(list(ntriples)) == 2)

    def test_opening_missing_db(self):
        with self.assertRaises(StandardError):
            self.graph.open(
                'postgresql://unittest@localhost/nosuchdb',
                create=False)

class TestHstoreConjunctiveGraphCore(unittest.TestCase):
    def setUp(self):
        plugin.register(
            'hstore', Store, 'rdflib_hstore.hstorestore', 'HstoreStore')
        store = 'hstore'
        self.graph = ConjunctiveGraph(store=store)
        self.path = connection_uri
        self.graph.open(self.path, create=True)

    def tearDown(self):
        self.graph.destroy(self.path)
        try:
            self.graph.close()
        except:
            pass

    def test_namespaces(self):
        self.graph.bind("dc", "http://http://purl.org/dc/elements/1.1/")
        self.graph.bind("foaf", "http://xmlns.com/foaf/0.1/")
        self.assertTrue(len(list(self.graph.namespaces())) == 6)
        self.assertTrue(
            ('foaf', rdflib.term.URIRef(u'http://xmlns.com/foaf/0.1/'))
            in list(self.graph.namespaces()))

    def test_triples_context_reset(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        ntriples = self.graph.triples(
            (None, None, None), context=self.graph.store)
        self.assertTrue(len(list(ntriples)) == 2)

    def test_remove_context_reset(self):
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        self.graph.add((michel, likes, pizza))
        self.graph.add((michel, likes, cheese))
        self.graph.commit()
        self.graph.store.remove((michel, likes, cheese), self.graph.store)
        self.graph.commit()
        self.assertTrue(len(list(self.graph.triples(
            (None, None, None), context=self.graph.store))) == 1)

