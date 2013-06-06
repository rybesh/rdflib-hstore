"""
An adaptation of the BerkeleyDB Store's key-value approach to use hstore
as a back-end.

Based on an original contribution by Drew Perttula: `TokyoCabinet Store
<http://bigasterisk.com/darcs/?r=tokyo;a=tree>`_.

and then a Kyoto Cabinet version by Graham Higgins <gjh@bel-epa.com>,

and a LevelDB one by Gunnar Grimnes.
"""

import hstore
import psycopg2
from rdflib import URIRef
from rdflib.store import Store
from rdflib.store import VALID_STORE
from bisect import bisect_left
from itertools import takewhile

#from lru import lru_cache, lfu_cache

class HstoreStore(Store):
    context_aware = True
    formula_aware = True
    transaction_aware = False

    def __init__(self, configuration=None, identifier=None):
        self._terms = 0
        self.__identifier = identifier
        super(HstoreStore, self).__init__(configuration)
        self.configuration = configuration

    def __get_identifier(self):
        return self.__identifier
    identifier = property(__get_identifier)

    def closed(self):
        return self.__connection.closed

    def open(self, configuration, create=True):

        self.__connection = psycopg2.connect(configuration)

        def dbopen(name):
            if create:
                return hstore.open(self.__connection, name)
            if hstore.exists(self.__connection, name):
                return hstore.open(self.__connection, name)
            raise ValueError('hstore {} does not exist'.format(name))

        # create and open the DBs
        self.__indices_info = [None, ] * 3

        self.__indices_info = tuple(  
            (dbopen(to_key_func(i)(('s','p','o'), 'c')), 
             to_key_func(i), 
             from_key_func(i)) for i in range(3))

        self.__indices = tuple(i[0] for i in self.__indices_info)
        self.__lookup_dict = build_lookup_dict(
            self.__indices, self._from_string)
        self.__contexts = dbopen('contexts')
        self.__namespace = dbopen('namespace')
        self.__prefix = dbopen('prefix')
        self.__k2i = dbopen('k2i')
        self.__i2k = dbopen('i2k')

        try:
            self._terms = int(self.__k2i["__terms__"])
        except KeyError:
            pass  # new store, no problem

        return VALID_STORE

    def close(self, commit_pending_transaction=False):
        self.__connection.close()

    def destroy(self, configuration=''):
        assert not self.closed(), 'The store must be open.'
        self.__indices = [None] * 3
        self.__indices_info = [None] * 3
        self.__contexts = None
        self.__namespace = None
        self.__prefix = None
        self.__i2k = None
        self.__k2i = None

    def add(self, (subject, predicate, object), context, quoted=False):
        assert not self.closed(), 'The store must be open.'
        assert context != self, 'Cannot add triple directly to store'

        # Add the triple to the Store, triggering TripleAdded events
        Store.add(self, (subject, predicate, object), context, quoted)

        s = self._to_string(subject)
        p = self._to_string(predicate)
        o = self._to_string(object)
        c = self._to_string(context)

        cspo, cpos, cosp = self.__indices

        value = cspo.get(u"{}^{}^{}^{}".format(c,s,p,o), None)
        if value is None:
            self.__contexts[c] = u""
            contexts_value = cspo.get(u"^{}^{}^{}^".format(s, p, o), u"")
            contexts = set(contexts_value.split(u"^"))
            contexts.add(c)
            contexts_value = u"^".join(contexts)
            assert contexts_value != None

            cspo[u"{}^{}^{}^{}^".format(c, s, p, o)] = u""
            cpos[u"{}^{}^{}^{}^".format(c, p, o, s)] = u""
            cosp[u"{}^{}^{}^{}^".format(c, o, s, p)] = u""

            if not quoted:
                cspo[u"^{}^{}^{}^".format(s, p, o)] = contexts_value
                cpos[u"^{}^{}^{}^".format(p, o, s)] = contexts_value
                cosp[u"^{}^{}^{}^".format(o, s, p)] = contexts_value

    def __remove(self, (s, p, o), c, quoted=False):
        cspo, cpos, cosp = self.__indices
        contexts_value = cspo.get(u"^{}^{}^{}^".format(s, p, o), u"")
        contexts = set(contexts_value.split(u"^"))
        contexts.discard(c)
        contexts_value = u"^".join(contexts)
        for i, _to_key, _from_key in self.__indices_info:
            del i[_to_key((s, p, o), c)]
        if not quoted:
            if contexts_value:
                for i, _to_key, _from_key in self.__indices_info:
                    i[_to_key((s, p, o), u"")] = contexts_value
            else:
                for i, _to_key, _from_key in self.__indices_info:
                    del i[_to_key((s, p, o), u"")]

    def remove(self, (subject, predicate, object), context):
        assert not self.closed(), "The Store must be open."
        Store.remove(self, (subject, predicate, object), context)

        if context == self:
            context = None
        if (subject is not None and predicate is not None 
            and object is not None and context is not None):
            s = self._to_string(subject)
            p = self._to_string(predicate)
            o = self._to_string(object)
            c = self._to_string(context)
            value = self.__indices[0].get(u"{}^{}^{}^{}^".format(c,s,p,o), None)
            if value is not None:
                self.__remove((s,p,o), c)
        else:
            cspo, cpos, cosp = self.__indices
            index, prefix, from_key, results_from_key = self.__lookup(
                (subject, predicate, object), context)

            for key in takewhile(
                lambda k: k.startswith(prefix), 
                range_iter(index, prefix, include_value=False)):
                c,s,p,o = from_key(key)
                if context is None:
                    contexts_value = index.get(key, u"")
                    # remove triple from all non quoted contexts
                    contexts = set(contexts_value.split(u"^"))
                    contexts.add(u"")  # and from the conjunctive index
                    for c in contexts:
                        for i, _to_key, _ in self.__indices_info:
                            del i[_to_key((s,p,o), c)]
                else:
                    self.__remove((s,p,o), c)

            if context is not None:
                if subject is None and predicate is None and object is None:
                    del self.__contexts[_to_string(context)]

    def triples(self, (subject, predicate, object), context=None):
        """A generator over all the triples matching """
        assert not self.closed(), "The Store must be open."
        if context == self:
            context = None

        index, prefix, from_key, results_from_key = self.__lookup(
            (subject, predicate, object), context)

        for key, value in takewhile(
            lambda pair: pair[0].startswith(prefix),
            range_iter(index, prefix)):
            yield results_from_key(key, subject, predicate, object, value)

    def __len__(self, context=None):
        assert not self.closed(), "The Store must be open."
        if context == self:
            context = None

        if context is None:
            prefix = u"^"
        else:
            prefix = u"{}^".format(self._to_string(context))

        return len(takewhile(
                lambda k: k.startswith(prefix), 
                range_iter(self.__indices[0], prefix, include_value=False)))

    def bind(self, prefix, namespace):
        bound_prefix = self.__prefix.get(namespace, None)
        if bound_prefix is not None:
            del self.__namespace[bound_prefix]
        self.__prefix[namespace] = prefix
        self.__namespace[prefix] = namespace

    def namespace(self, prefix):
        return self.__namespace.get(prefix, None)

    def prefix(self, namespace):
        return self.__prefix.get(namespace, None)

    def namespaces(self):
        return range_iter(self.__namespace)

    def contexts(self, triple=None):
        if triple:
            s, p, o = triple
            s = self._to_string(s)
            p = self._to_string(p)
            o = self._to_string(o)
            contexts = self.__indices[0][u"^{}^{}^{}^".format(s,p,o)]
            if contexts:
                for c in contexts.split(u"^"):
                    if c:
                        yield self._from_string(c)
        else:
            for k in range_iter(self.__contexts, include_value=False):
                yield _from_string(k)

    #@lru_cache(5000)
    #@lfu_cache(5000)
    def _from_string(self, i):
        """rdflib term from index number (as a string)"""
        k = self.__i2k.get(i, None)
        if k is None:
            raise Exception('Key for {} is None'.format(i))
        return self.node_pickler.loads(k)

    #@lru_cache(5000)
    #@lfu_cache(5000)
    def _to_string(self, term):
        """index number (as a string) from rdflib term"""
        k = self.node_pickler.dumps(term)
        i = self.__k2i.get(k, None)
        if i is None:
            i = unicode(self._terms)
            self.__k2i[k] = i
            self.__i2k[i] = k
            self._terms += 1
            self.__k2i["__terms__"] = str(self._terms)
        return i

    def __lookup(self, (subject, predicate, object), context):
        if context is not None:
            context = self._to_string(context)
        i = 0
        if subject is not None:
            i += 1
            subject = self._to_string(subject)
        if predicate is not None:
            i += 2
            predicate = self._to_string(predicate)
        if object is not None:
            i += 4
            object = self._to_string(object)
        index, prefix_func, from_key, results_from_key = self.__lookup_dict[i]
        prefix = u"^".join(prefix_func((subject, predicate, object), context))
        return index, prefix, from_key, results_from_key


def build_lookup_dict(indices, from_string):

    def result(start, i):
        score = 1
        length = 0
        for j in range(start, start + 3):
            if i & (1 << (j % 3)):
                score = score << 1
                length += 1
            else:
                break
        return ((score, 2-start), start, start+length)

    def generate_range(i):
        return tuple(sorted(result(start, i) for start in range(3))[-1][1:])

    def get_prefix_func(start, end):
        def get_prefix(triple, context):
            if context is None:
                yield ""
            else:
                yield context
            i = start
            while i < end:
                yield triple[i % 3]
                i += 1
            yield ""
        return get_prefix

    return { i: (indices[start],
                 get_prefix_func(start,end),
                 from_key_func(start),
                 results_from_key_func(start, from_string)) 
             for i, (start,end) in ((i, generate_range(i)) for i in range(8)) }

def to_key_func(offset):
    def to_key(triple, context):
        'Takes a string; returns key'
        return u"^".join((context, 
                          triple[offset % 3],
                          triple[(offset + 1) % 3], 
                          triple[(offset + 2) % 3], u"")
                         )  # "" to tack on the trailing ^
    return to_key

def from_key_func(i):
    def from_key(key):
        "Takes a key; returns string"
        parts = key.split(u"^")
        return parts[0], parts[(3 - i + 0) % 3 + 1], \
            parts[(3 - i + 1) % 3 + 1], parts[(3 - i + 2) % 3 + 1]
    return from_key

def results_from_key_func(i, from_string):
    def from_key(key, subject, predicate, object, contexts_value):
        "Takes a key and subject, predicate, object; returns tuple for yield"
        parts = key.split(u"^")
        if subject is None:
            s = from_string(parts[(3 - i + 0) % 3 + 1])
        else:
            s = subject
        if predicate is None:
            p = from_string(parts[(3 - i + 1) % 3 + 1])
        else:
            p = predicate
        if object is None:
            o = from_string(parts[(3 - i + 2) % 3 + 1])
        else:
            o = object
        return (s, p, o), (from_string(c)
                           for c in contexts_value.split(u"^") if c)
    return from_key

def range_iter(index, start=None, include_value=True):
    items = sorted(index.items())
    keys = zip(*items)[0] if len(items) > 0 else []
    i = 0 if start is None else bisect_left(keys, start)
    for k,v in items[i:]:
        yield (k,v) if include_value else k

