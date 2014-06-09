'''FoundationDB String Interning Layer.

Provides the StringIntern() class for interning (aka normalizing, aliasing)
commonly-used long strings into shorter representations. The class is
initialized with a subspace used for storage of its state and maintains a local
cache of frequently-used items.

StringIntern() exposes the intern() and lookup() methods for clients.

The intern() method is non-transactional. It calls a transactional function
internally but performs all cache manipulation outside of a transaction. Because
the transaction alters the intern database, the separation of cache manipulation
from the transaction is necessary to maintain cache validity in the event of a
transaction failure.

The lookup() method is transactional and performs its cache manipulation inside
the transaction. This approach is unusual and is possible only because 1) the
transaction only reads the intern database, and 2) the keys in the intern
database are write-once, so the string <-> representation relation grows
monotonically. In particular, no other transaction can alter the intern database
in a way that invalidates previous reads. Hence, any data read by lookup() from
the intern database is correct, even if the transaction fails.

'''

import os
import random

import fdb
import fdb.tuple

fdb.api_version(200)

################
# StringIntern #
################


class StringIntern (object):
    CACHE_LIMIT_BYTES = 10000000

    # Public methods

    def __init__(self, subspace):
        self.subspace = subspace 
        self._string = self.subspace['S'] # nested subspace for strings
        self._uid = self.subspace['U'] # nested subspace for uids
        self._cached_uids = []
        self._uid_string_cache = {}
        self._string_uid_cache = {}
        self._bytes_cached = 0

    def intern(self, db, s):
        """
        Return the normalized representation of the string s.

        Attempt to find string in the cache. If absent, transactionally
        intern representation in the database, then add it to the cache. String
        s must fit within a FoundationDB value.
        """
        if s in self._string_uid_cache:
            return self._string_uid_cache[s]
        u = self._intern_in_db(db, s)
        self._add_to_cache(s, u)
        return u

    @fdb.transactional
    def lookup(self, tr, u):
        """
        Return the reference string for normalized representation u.

        Attempt to find reference string s for u in cache. If absent,
        look up u in database, then add it to the cache.
        """
        if u in self._uid_string_cache:
            return self._uid_string_cache[u]
        s = tr.snapshot[self._uid[u]]
        if s is None:
            raise Exception('String intern identifier not found')
        # WARNING: You should not ordinarily perform operations with
        # side-effects, such as updating a cache, inside a transaction!
        # It is correct here only because we know that the keys in the intern
        # database are write-once, so the string <-> representation relation 
        # grows monotonically. Hence, anything we read in the snapshot is
        # correct, even if the transaction fails.
        self._add_to_cache(s, u)
        return s

    # Private methods

    @fdb.transactional
    def _intern_in_db(self, tr, s):
        """
        Return normalized representation of the string s from the database.

        Attempt to find string s in database. If absent, create and record the
        normalized representation. String s must fit within a FoundationDB
        value.
        """
        u = tr[self._string[s]]
        if u.present():
            return u
        else:
            new_U = self._find_uid(tr)
            tr[self._uid[new_U]] = s
            tr[self._string[s]] = new_U
            return new_U

    def _add_to_cache(self, s, u):
        while self._bytes_cached > StringIntern.CACHE_LIMIT_BYTES:
            self._evict_cache()
        if u not in self._uid_string_cache:
            self._string_uid_cache[s] = u
            self._uid_string_cache[u] = s
            self._cached_uids.append(u)
            self._bytes_cached += (len(s) + len(u)) * 2

    def _evict_cache(self):
        if len(self._cached_uids) == 0:
            raise Exception('Cannot evict from empty cache')
        # Random eviction
        i = random.randint(0, len(self._cached_uids) - 1)

        # remove from _cached_uids
        u = self._cached_uids[i]
        self._cached_uids[i] = self._cached_uids[len(self._cached_uids) - 1]
        self._cached_uids.pop()

        # remove from caches, account for bytes
        s = self._uid_string_cache[u]
        if s is None:
            raise Exception('Error in cache eviction: string not found')

        del self._uid_string_cache[u]
        del self._string_uid_cache[s]

        self._bytes_cached -= (len(s) + len(u)) * 2

    @fdb.transactional
    def _find_uid(self, tr):
        tries = 0
        while True:
            u = os.urandom(4 + tries)
            if u in self._uid_string_cache:
                continue
            if tr[self._uid[u]] == None:
                return u
            tries += 1


###########
# Example #
###########


def stringintern_example():
    db = fdb.open()
    location = fdb.directory.create_or_open(db, ('tests', 'stringintern'))
    strs = StringIntern(location)

    db["0"] = strs.intern(db, "testing 123456789")
    db["1"] = strs.intern(db, "dog")
    db["2"] = strs.intern(db, "testing 123456789")
    db["3"] = strs.intern(db, "cat")
    db["4"] = strs.intern(db, "cat")

    for k, v in db['0':'9']:
        print k, '=', strs.lookup(db, v)

if __name__ == "__main__":
    stringintern_example()
