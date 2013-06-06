try:
    import psycopg2
except ImportError:
    from nose import SkipTest
    raise SkipTest('psycopg2 not installed, skipping hstore tests')

import context_case
import graph_case
from n3_2_case import testN3Store

storename = "hstore"
connection_uri = os.environ.get(
    'DBURI',
    'postgresql+psycopg2://postgres@localhost/rdflibhstore_test')


class HstoreGraphTestCase(graph_case.GraphTestCase):
    store_name = storename
    path = connection_uri
    storetest = True

class HstoreContextTestCase(context_case.ContextTestCase):
    store_name = storename
    path = connection_uri
    storetest = True

testN3Store(storename, connection_uri)

