"""FoundationDB ScoredSet Layer.

Provides a ScoredSet class.

Scored sets support an extended form of ranked set similar to the Redis "sorted 
set" data type. Scored sets are collections of items (of any type handled by
the tuple layer) associated with an integer score. Items can be present at most
once in the collection, but multiple items can have the same score. Items are
sorted and ranked by their scores.

The RankedSet layer is used to provide fast operations for rankings. In
addition to basic operations for insertion, deletion, and updating of items,
ranges of items can be quickly retrieved by score or by rank.
"""

import os
import struct
import sys

import fdb
import rankedset
fdb.api_version(200)

#############
# ScoredSet #
#############


class ScoredSet:
    # Public methods

    def __init__(self, db, subspace):
        self.subspace = subspace
        self._rs = rankedset.RankedSet(db, self.subspace['R'])
        self._score = self.subspace['S']
        self._items = self.subspace['I']

    @fdb.transactional
    def insert(self, tr, item, score):
        ''' Add item with score, or update its score if item already exists.'''
        old_score = None
        s = tr[self._score[item]]
        if s.present():
            old_score = self._decode_score(s)
            if self._no_other(tr, item, old_score):
                self._rs.erase(tr, old_score)
            del tr[self._items[old_score][item]]
        self._rs.insert(tr, score)
        tr[self._score[item]] = self._encode_score(score)
        tr[self._items[score][item]] = ''
        return old_score

    @fdb.transactional
    def increment(self, tr, item, increment):
        ''' Increase the score of item by increment.'''
        s = tr[self._score[item]]
        if not s.present():
            raise Exception('{} not found'.format(item))
        old_score = self._decode_score(s)
        try:
            score = old_score + increment
        except:
            raise Exception('increment requires integer scores')
        if self._no_other(tr, item, old_score):
            self._rs.erase(tr, old_score)
        del tr[self._items[old_score][item]]
        self._rs.insert(tr, score)
        tr[self._score[item]] = self._encode_score(score)
        tr[self._items[score][item]] = ''
        return old_score

    @fdb.transactional
    def delete(self, tr, item):
        ''' Delete item. '''
        s = tr[self._score[item]]
        if not s.present():
            return None
        score = self._decode_score(s)
        if self._no_other(tr, item, score):
            self._rs.erase(tr, score)
        del tr[self._items[score][item]]
        del tr[self._score[item]]
        return score

    @fdb.transactional
    def delete_by_rank(self, tr, start_rank, stop_rank):
        ''' Delete all items within the range [start_rank, stop_rank). '''
        start_score, stop_score = self._rank_range_to_scores(tr, start_rank,
                                                                 stop_rank)
        return self.delete_by_score(tr, start_score, stop_score)

    @fdb.transactional
    def delete_by_score(self, tr, start_score, stop_score):
        ''' Delete all items in the range [start_score, stop_score). '''
        erased = {}
        for k, _ in tr.get_range(self._items[start_score],
                                 self._items[stop_score]):
            score, item = self._items.unpack(k)
            del tr[self._score[item]]
            if score not in erased:
                self._rs.erase(tr, score)
                erased[score] = ''
        del tr[self._items[start_score]:self._items[stop_score]]
        return list(erased)

    @fdb.transactional
    def get_items(self, tr, score):
        ''' Return list of items with given score. '''
        return [self._items[score].unpack(k)[0]
                for k, _ in tr[self._items[score].range()]]

    @fdb.transactional
    def get_score(self, tr, item):
        ''' Get the score associated with item or None if not present. '''
        s = tr[self._score[item]]
        if not s.present():
            return None
        return self._decode_score(s)

    @fdb.transactional
    def get_items_by_rank(self, tr, rank):
        ''' Return list of items with given rank. '''
        score = self._rs.get_nth(tr, rank)
        return self.get_items(tr, score)

    @fdb.transactional
    def get_range_by_rank(self, tr, start_rank, stop_rank):
        ''' Return list of items in the range [start_rank, stop_rank). '''
        start_score, stop_score = self._rank_range_to_scores(tr, start_rank,
                                                                 stop_rank)
        return self.get_range_by_score(tr, start_score, stop_score)

    @fdb.transactional
    def get_range_by_score(self, tr, start_score, stop_score, reverse=False):
        ''' Return list of items in the range [start_score, stop_score).

        When reverse=True, scores are ordered from high to low.
        '''
        return [self._items.unpack(k)[1]
                for k, _ in tr.get_range(self._items[start_score],
                                         self._items[stop_score],
                                         reverse=reverse)]

    @fdb.transactional
    def get_rank(self, tr, item):
        ''' Return the rank of a item. '''
        score = self.get_score(tr, item)
        return self._rs.rank(tr, score)

    @fdb.transactional
    def get_rank_by_score(self, tr, score):
        ''' Return the rank of a given score. '''
        return self._rs.rank(tr, score)

    @fdb.transactional
    def get_successors(self, tr, item):
        ''' Return list of immediate successors of item by rank. '''
        rank = self.get_rank(tr, item)
        successor_score = self._rs.get_nth(tr, rank + 1)
        if successor_score:
            return self.get_items(tr, successor_score)
        else:
            return []

    @fdb.transactional
    def get_predecessors(self, tr, item):
        ''' Return list of immediate predecessors of item by rank. '''
        rank = self.get_rank(tr, item)
        predecessor_score = self._rs.get_nth(tr, rank - 1)
        if predecessor_score:
            return self.get_items(tr, predecessor_score)
        else:
            return []

    @fdb.transactional
    def get_max_rank(self, tr):
        ''' Return the maximum rank. '''
        size = self._rs.size(tr)
        if not size:
            return None
        return self._rs.size(tr) - 1

    @fdb.transactional
    def get_max_score(self, tr):
        ''' Return the maximum score. '''
        r = self._items.range()
        for k, _ in tr.get_range(r.start, r.stop, limit=1, reverse=True):
            return self._items.unpack(k)[0]

    @fdb.transactional
    def count_by_score(self, tr, start_score, stop_score):
        ''' Return number of items in the range [start_score, stop_score). '''
        count = 0
        for _ in tr.get_range(self._items[start_score],
                              self._items[stop_score]):
            count += 1
        return count

    @fdb.transactional
    def iterate(self, tr):
        ''' Generate items and their scores.'''
        for k, v in tr[self._score.range()]:
            yield self._score.unpack(k)[0], self._decode_score(v)

    # Private methods

    def _encode_score(self, c):
        return struct.pack('<q', c)

    def _decode_score(self, v):
        return struct.unpack('<q', str(v))[0]

    @fdb.transactional
    def _no_other(self, tr, item, score):
        ''' Return True if there is no other element than item with score. '''
        r = self._items[score].range()
        for k, _ in tr.get_range(r.start, r.stop, limit=2):
            s, i = self._items.unpack(k)
            if i != item:
                return False
        return True

    @fdb.transactional
    def _rank_range_to_scores(self, tr, start_rank, stop_rank):
        ''' Return scores corresponding to range [start_rank, stop_rank).
        '''
        if start_rank < 0:
            raise Exception('rank must be nonnegative')
        start_score = self._rs.get_nth(tr, start_rank)
        if stop_rank > self._rs.size(tr) - 1:
            stop_score = sys.maxint
        else:
            stop_score = self._rs.get_nth(tr, stop_rank)
        return start_score, stop_score

###########
# Testing #
###########

import random
import threading

#fdb.impl.TransactionRead.create_transaction = lambda self: self
#fdb.impl.TransactionRead.commit = lambda self: self.db.create_transaction().commit()

excl = threading.Lock()


@fdb.transactional
def scored_set_op(tr, ss):

    op = random.randint(0, 20)
    score = random.randint(0, 100)
    item = os.urandom(1)

    if 0 <= op <= 7:
        ss.insert(tr, item, score)

    if op == 8:
        score = ss.get_score(tr, item)
        if score is not None:
            ss.increment(tr, item, random.randint(1, 10))

    if op == 9:
        ss.delete(tr, item)

    if op == 10:
        max_rank = ss.get_max_rank(tr)
        if max_rank is None:
            return
        ranks = sorted([random.randint(0, max_rank), 
                        random.randint(0, max_rank)])
        ss.delete_by_rank(tr, ranks[0], ranks[1])

    if op == 11:
        scores = sorted([score, random.randint(0, 100)])
        ss.delete_by_score(tr, scores[0], scores[1])

    if op == 12:
        items = ss.get_items(tr, score)
        if not items:
            return
        i = random.choice(items)
        s2 = ss.get_score(tr, i)
        with excl:
            if not (score == s2):
                print [i], score, s2
        assert score == s2

    if op == 13:
        max_rank = ss.get_max_rank(tr)
        if max_rank is None:
            return
        r = random.randint(0, max_rank)
        items = ss.get_items_by_rank(tr, r)
        i = random.choice(items)
        r2 = ss.get_rank(tr, i)
        with excl:
            if not (r == r2):
                print [i], r, r2
        assert r == r2

    if op == 14:
        max_rank = ss.get_max_rank(tr)
        if max_rank is None:
            return
        ranks = sorted([random.randint(0, max_rank), 
                        random.randint(0, max_rank)])
        items = ss.get_range_by_rank(tr, ranks[0], ranks[1])
        if not items:
            return
        i = random.choice(items)
        r = ss.get_rank(tr, i)
        with excl:
            if not (ranks[0] <= r <= ranks[1]):
                print [i], ranks[0], r, ranks[1]
        assert ranks[0] <= r <= ranks[1]

    if op == 15:
        scores = sorted([score, random.randint(0, 100)])
        items = ss.get_range_by_score(tr, scores[0], scores[1])
        if not items:
            return
        i = random.choice(items)
        s = ss.get_score(tr, i)
        with excl:
            if not (scores[0] <= s <= scores[1]):
                print [i], scores[0], s, scores[1]
        assert scores[0] <= s <= scores[1]

    if op == 16:
        r = ss.get_rank(tr, item)
        if r is None:
            return
        items = ss.get_successors(tr, item)
        if not items:
            return
        item2 = random.choice(items)
        r = ss.get_rank(tr, item)
        r2 = ss.get_rank(tr, item2)
        with excl:
            if not (r2 == r + 1):
                print r2, r
        assert r2 == r + 1

    if op == 17:
        r = ss.get_rank(tr, item)
        if r is None:
            return
        items = ss.get_predecessors(tr, item)
        if not items:
            return
        item2 = random.choice(items)
        r = ss.get_rank(tr, item)
        r2 = ss.get_rank(tr, item2)
        with excl:
            if not (r2 == r - 1):
                print r2, r
        assert r2 == r - 1

    if op == 18:
        max_score = ss.get_max_score(tr)
        if max_score is None:
            return
        items = ss.get_items(tr, max_score)
        item = random.choice(items)
        max_rank = ss.get_max_rank(tr)
        r = ss.get_rank(tr, item)
        with excl:
            if not (r == max_rank):
                print max_score, item, r, max_rank
        assert r == max_rank

    if op == 19:
        scores = sorted([score, random.randint(0, 100)])
        count = ss.count_by_score(tr, scores[0], scores[1])
        count2 = len(ss.get_range_by_score(tr, scores[0], scores[1]))
        with excl:
            if not (count == count2):
                print count, count2
        assert count == count2

    if op == 20:
        size1 = len(list(ss.iterate(tr)))
        size2 = ss.count_by_score(tr, 0, sys.maxint)
        with excl:
            if not (size1 == size2):
                print size1, size2
        assert size1 == size2

def random_test(db, ss):
    for _ in range(100):
        scored_set_op(db, ss)


@fdb.transactional
def clear_subspace(tr, subspace):
    del tr[subspace.range()]

if __name__ == "__main__":
    db = fdb.open()
    scored_set = fdb.directory.create_or_open(db, ('S',))
    clear_subspace(db, scored_set)
    ss = ScoredSet(db, scored_set)

    threads = [threading.Thread(target=random_test, args=(db, ss))
               for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print "test finished"
