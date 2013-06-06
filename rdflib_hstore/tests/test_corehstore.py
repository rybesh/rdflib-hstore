import unittest
import os
import rdflib
import psycopg2
from psycopg2.extensions import \
    ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED
from rdflib import plugin
from rdflib.store import Store
from rdflib.graph import Graph, ConjunctiveGraph

storename = 'hstore'
storetest = True
connection_uri = os.environ.get(
    'DBURI',
    'postgresql://unittest@localhost/rdflibhstore_test')

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

class TestHstoreGraphCore(BaseCase):

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
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        graph.add((michel, likes, pizza))
        graph.add((michel, likes, cheese))
        ntriples = graph.triples((None, None, None))
        self.assertTrue(len(list(ntriples)) == 2)

    def test_reopening_db(self):
        graph = self.open_graph()
        michel = rdflib.URIRef(u'michel')
        likes = rdflib.URIRef(u'likes')
        pizza = rdflib.URIRef(u'pizza')
        cheese = rdflib.URIRef(u'cheese')
        graph.add((michel, likes, pizza))
        graph.add((michel, likes, cheese))
        graph.store.close()
        graph.store.open(connection_uri, create=False)
        ntriples = graph.triples((None, None, None))
        self.assertTrue(len(list(ntriples)) == 2)

    def test_opening_missing_db(self):
        with self.assertRaises(StandardError):
            graph.open(
                'postgresql://unittest@localhost/nosuchdb',
                create=False)

class TestHstoreConjunctiveGraphCore(BaseCase):

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

