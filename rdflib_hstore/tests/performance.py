import gc
import os.path
from time import time
from rdflib import Graph
from rdflib import RDF

from .functional import BaseCase

connection_uri = os.environ.get(
    'DBURI',
    'postgresql://unittest@localhost/rdflibhstore_test')

class PerformanceTestCase(BaseCase):

    def setUp(self):
        self.reenable_gc = gc.isenabled()
        gc.collect()
        gc.disable()
        super(PerformanceTestCase, self).setUp()
    
    def tearDown(self):
        super(PerformanceTestCase, self).tearDown()
        if self.reenable_gc: gc.enable()
    
    def open_graph(self):
        graph = Graph(store='hstore')
        graph.open(connection_uri, create=True)
        self.graphs.append(graph)
        return graph

    def test_500triples(self):
        print '500triples', self.parse('500triples')

    def test_1ktriples(self):
        print '1ktriples', self.parse('1ktriples')

    def test_2ktriples(self):
        print '2ktriples', self.parse('2ktriples')

    # def test_3ktriples(self):
    #     print '3ktriples', self.parse('3ktriples')

    # def test_5ktriples(self):
    #     print '5ktriples', self.parse('5ktriples')

    # def test_10ktriples(self):
    #     print '10ktriples', self.parse('10ktriples')

    # def test_25ktriples(self):
    #     print '25ktriples', self.parse('25ktriples')

    # def test_50ktriples(self):
    #     print '50ktriples', self.parse('50ktriples')

    def parse(self, dataset):
        
        graph = self.open_graph()
        
        data = Graph()
        path = os.path.dirname(os.path.realpath(__file__))
        print 'parsing', dataset
        data.parse(location='{}/data/{}.n3'.format(path, dataset), format='n3')
        
        def add_and_iterate(data, graph):
            for triple in data:
                graph.add(triple)
            for s in graph.subjects(RDF.type, None):
                for t in graph.triples((s,None,None)):
                    pass

        iterations = 5
        elapsed = 0.0
        for i in range(iterations):
            print dataset, 'iteration', i+1
            t = time()
            add_and_iterate(data, graph)
            elapsed += (time() - t)
        return "{:.3g}s (averaged over {} iterations)".format(
            elapsed/iterations, iterations)

