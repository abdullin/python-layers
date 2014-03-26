"""FoundationDB RankedSet Layer.

Provides a RankedSet class.

Ranked sets support efficient retrieval of elements by their rank as defined by
lexicographic order. Elements are inserted into (or removed from) the set by key.
The rank of any element can then be quickly determined, and an element can be
quickly retrieved by based on its rank.

"""
import struct

import fdb
import fdb.tuple

fdb.api_version(200)

##############
# Ranked Set #
##############

MAX_LEVELS = 6
LEVEL_FAN_POW = 4  # 2^X per level

# Data model
# (level, key) = (count_to_next_key_in_level,)


def encodeCount(c):
    return struct.pack('<q', c)


def decodeCount(v):
    return struct.unpack('<q', str(v))[0]


class RankedSet(object):

    @fdb.transactional
    def __init__(self, tr, subspace):
        self.subspace = subspace
        self._setup_levels(tr)

    @fdb.transactional
    def _slow_count(self, tr, level, begin_key, end_key):
        if level == -1:
            if begin_key == "":
                return 0
            else:
                return 1
        l = tr[self.subspace.pack((level, begin_key)):
               self.subspace.pack((level, end_key))]
        return sum(decodeCount(kv.value) for kv in l)

    @fdb.transactional
    def _setup_levels(self, tr):
        for l in range(MAX_LEVELS):
            k = self.subspace.pack((l, ""))
            if tr[k] == None:
                tr[k] = encodeCount(0)

    # returns key, count
    @fdb.transactional
    def _get_previous_node(self, tr, level, key):

        # _get_previous_node looks for the previous node on a level, but "doesn't care"
        # about the contents of that node. It therefore uses a non-isolated (snaphot)
        # read and explicitly adds a conflict range that is exclusive of the actual,
        # found previous node. This allows an increment of that node not to trigger
        # a transaction conflict. We also add a conflict key on the found previous
        # key in level 0. This allows detection of erasures.

        # print "gpn", level, key
        k = self.subspace.pack((level, key))
        kv = list(tr.snapshot.get_range(fdb.KeySelector.last_less_than(k),
                                        fdb.KeySelector.first_greater_or_equal(k),
                                        limit=1))[0]
        prevKey = self.subspace.unpack(kv.key)[1]
        tr.add_read_conflict_range(kv.key + '\x00', k)
        tr.add_read_conflict_key(self.subspace.pack((0, prevKey)))
        return prevKey

    @fdb.transactional
    def _debug_print(self, tr, func=lambda x: x):
        for l in range(MAX_LEVELS):
            print "\nlevel", l, ":"
            for k, c in tr[self.subspace.range((l,))]:
                t = self.subspace.unpack(k)
                print "\t", func(t[1]), "=", decodeCount(c)

    # public interface 

    @fdb.transactional
    def size(self, tr):
        """ Returns the number of items in the set. """
        return sum(decodeCount(kv.value) for kv in tr[self.subspace[MAX_LEVELS - 1].range()])

    @fdb.transactional
    def insert(self, tr, key):
        """ Inserts an item into the set. No effect if item is already present. """
        if key == "":
            raise Exception("Empty key not allowed in set")
        if self.contains(tr, key):
            return
        level = 0
        keyHash = hash(key)
        for level in range(MAX_LEVELS):
            prevKey = self._get_previous_node(tr, level, key)

            if keyHash & (2 ** (level * LEVEL_FAN_POW) - 1):
                tr.add(self.subspace.pack((level, prevKey)), encodeCount(1))
            else:
                # Insert into this level by looking at the count of the previous
                # key in the level and recounting the next lower level to correct
                # the counts
                prevCount = decodeCount(tr[self.subspace.pack((level, prevKey))])
                newPrevCount = self._slow_count(tr, level - 1, prevKey, key)
                count = prevCount - newPrevCount
                count += 1

                # print "insert", key, "level", level, "count", count,
                # "splits", prevKey, "oldC", prevCount, "newC", newPrevCount
                tr[self.subspace.pack((level, prevKey))] = encodeCount(newPrevCount)
                tr[self.subspace.pack((level, key))] = encodeCount(count)

    @fdb.transactional
    def contains(self, tr, key):
        """ Checks for the presense of an item in the set. """
        if key == "":
            raise Exception("Empty key not allowed in set")
        return tr[self.subspace.pack((0, key))] != None

    @fdb.transactional
    def erase(self, tr, key):
        """ Removes an item from the set. No effect if item is already not present. """
        if not self.contains(tr, key):
            return
        for level in range(MAX_LEVELS):
            # This could be optimized with hash
            k = self.subspace.pack((level, key))
            c = tr[k]
            if c != None:
                del tr[k]
            if level == 0:
                continue

            prevKey = self._get_previous_node(tr, level, key)
            assert prevKey != key
            countChange = -1
            if c != None:
                countChange += decodeCount(c)
            # print "level", level, "setting", prevKey, "from", prevCount,
            # "to", newCount
            tr.add(self.subspace.pack((level, prevKey)),
                   encodeCount(countChange))

    @fdb.transactional
    def rank(self, tr, key):
        """
        Returns the (0-based) index of the lexicographically-ordered items in the set.
        Returns None if the item is not in the set.
        """
        if key == "":
            raise Exception("Empty key not allowed in set")
        if not self.contains(tr, key):
            return None
        r = 0
        rank_key = ""
        for level in range(MAX_LEVELS - 1, -1, -1):
            lss = self.subspace[level]
            lastCount = 0
            for k, c in tr[lss.pack((rank_key,)): fdb.KeySelector.first_greater_than(lss.pack((key,)))]:
                rank_key = lss.unpack(k)[0]
                lastCount = decodeCount(c)
                r += lastCount
            r -= lastCount
            if rank_key == key:
                break
            # print "rank at level", level, " is ", r, "rk=", rank_key
        return r

    @fdb.transactional
    def get_nth(self, tr, rank):
        """
        Returns the Nth lexicographically-ordered item in the set (0-based indexing).
        Returns None if the rank is out of bounds.
        """
        if rank < 0:
            return None
        r = rank
        key = ""
        for level in range(MAX_LEVELS - 1, -1, -1):
            lss = self.subspace[level]
            for k, c in tr[lss[key]: lss.range().stop]:
                key = lss.unpack(k)[0]
                count = decodeCount(c)
                if key != "" and r == 0:
                    return key
                if count > r:
                    break
                # print "level", level, "eating", count, "at", key
                r -= count
            else:
                return None

    @fdb.transactional
    def get_range(self, tr, start_key, end_key):
        """
        Returns the ordered set of keys in the range [start_key, end_key).
        start_key must not be ""
        """
        if start_key == "":
            raise Exception("Empty key not allowed in set")
        return [self.subspace.unpack(k)[-1]
                for k, v in
                tr[self.subspace.pack((0, start_key)):
                    self.subspace.pack((0, end_key))]]

    @fdb.transactional
    def clear_all(self, tr):
        """ Clears the entire set. """
        del tr[self.subspace.range()]
        self._setup_levels(tr)

##################
# Internal tests #
##################

import os
import random
import threading

fdb.impl.TransactionRead.create_transaction = lambda self: self
fdb.impl.TransactionRead.commit = lambda self: self.db.create_transaction().commit()

excl = threading.Lock()


@fdb.transactional
def rankedSetOp(tr, rs):

    op = random.randint(0, 2)
    rk = os.urandom(1)

    if op == 0:
        rs.insert(tr, rk)

    if op == 1:
        if rs.contains(tr, rk):
            rs.erase(tr, rk)

    if op == 2:
        size = rs.size(tr)
        if not size:
            return
        r = random.randint(0, size - 1)
        k = rs.get_nth(tr, r)
        r2 = rs.rank(tr, k)
        r3 = len(rs.get_range(tr, "\x00", k))
        with excl:
            if not (r == r2 == r3):
                print [k], r, r2, r3
                rs._debug_print(tr, lambda x: [x])
        assert r == r2 == r3


def rankedSetTest(db, rs):
    for _ in range(1000):
        rankedSetOp(db, rs)

if __name__ == "__main__":
    db = fdb.open()
    rs_location = fdb.directory.create_or_open(db, ('rstest',))
    del db[rs_location.range()]
    rs = RankedSet(db, rs_location)

    threads = [threading.Thread(target=rankedSetTest, args=(db, rs))
               for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print "test finished"
