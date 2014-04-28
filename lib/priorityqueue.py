'''Priority Queue

Provides a double-ended PriorityQueue() class.

Items are pushed wth a specified priority. Items are ordered first by priority,
then by push order, then randomly for simultaneous pushes. Items with either the
minimum or maximum ordering can be popped or peeked.

Two versions of the priority queue can be instantiated. The high-contention 
version is designed to support multiple clients popping the queue concurrently.
Pop operations in this version incur some overhead, but their performance will
scale well with the number of clients.

In the low-contention version, no attempt is made to avoid transaction conflicts
during pop operations. This version performs well with a small number of clients
but will not scale well as the number of clients grows.

The contract for the order of results of pop operations is best effort. Perfect
order is achieved in the low-contention version but not in the high-contention
version.
'''

import os
import time

import fdb
import fdb.tuple

fdb.api_version(200)

#################
# PriorityQueue #
#################


class PriorityQueue(object):
    # Public methods

    def __init__(self, subspace, high=True):
        self.subspace = subspace
        self._high = high
        self._pop_request = self.subspace['P']
        self._requested_item = self.subspace['R']
        self._item = self.subspace['I']
        self._member = self.subspace['M']

    def __contains__(self, item):
        return self._contains(db, item) 

    @fdb.transactional
    def clear(self, tr):
        '''Remove all items from the queue.'''
        del tr[self.subspace.range()]

    @fdb.transactional
    def remove(self, tr, item):
        '''Remove item from arbitrary position in the queue.'''
        for member in tr[self._member[item].range()]:
            priority, index = self._member[item].unpack(member.key)
            for item_key, value in tr[self._item[priority][index].range()]:
                random_id = self._item[priority][index].unpack(item_key)[0]
                i = self._decode(value)
                if i == item:
                    del tr[self._item[priority][index][random_id]]
            del tr[self._member[item][priority][index]]

    @fdb.transactional
    def push(self, tr, item, priority):
        '''Push a single item onto the queue.'''
        index = self._get_next_index(tr.snapshot, self._item[priority])
        self._push_at(tr, self._encode(item), index, priority)

    def pop(self, db, max=False):
        '''Pop the next item from the queue.

        Cannot be composed with other functions in a single transaction.'''
        if self._high:
            result = self._pop_high(db, max)
        else:
            result = self._pop_low(db, max)
        if result is None:
            return None
        return self._decode(result)

    @fdb.transactional
    def empty(self, tr):
        '''Test whether the queue is empty.'''
        return self._get_first_item(tr) is None

    @fdb.transactional
    def peek(self, tr, max=False):
        '''Get the next item in the queue without popping it.'''
        first_item = self._get_first_item(tr, max)
        if first_item is None:
            return None
        else:
            return self._decode(first_item.value)

    # Private methods

    def _random_ID(self):
        # Relies on good random data from the OS to avoid collisions
        return os.urandom(20)

    def _encode(self, value):
        return fdb.tuple.pack((value,))

    def _decode(self, value):
        return fdb.tuple.unpack(value)[0]

    @fdb.transactional
    def _contains(self, tr, item):
        for _ in tr[self._member[item].range()]:
            return True
        return False   

    # Items are pushed on the queue at a key of (priority, index, randomID).
    # Items pushed at the same time with the same priority may have the same
    # index, so their ordering will be random. This makes pushes fast and
    # usually conflict free (unless the queue becomes empty during the push).
    def _push_at(self, tr, item, index, priority):
        key = self._item[priority][index][self._random_ID()]
        # Protect against the unlikely event that someone else got the same
        # random ID while writing with the same priority and index.
        tr.add_read_conflict_key(key)
        tr[key] = item
        tr[self._member[self._decode(item)][priority][index]] = ''

    def _get_next_index(self, tr, subspace):
        last_key = tr.get_key(
            fdb.KeySelector.last_less_than(subspace.range().stop))
        if last_key < subspace.range().start:
            return 0
        return subspace.unpack(last_key)[0] + 1

    def _get_first_item(self, tr, max=False):
        r = self._item.range()
        for kv in tr.get_range(r.start, r.stop, limit=1, reverse=max):
            return kv
        return None

    # This implementation of pop does not attempt to avoid conflicts. If many
    # clients try to pop simultaneously, only one will be able to succeed.
    @fdb.transactional
    def _pop_low(self, tr, max):
        first_item = self._get_first_item(tr, max)
        if first_item is None:
            return None
        key = first_item.key
        item = first_item.value
        del tr[key]
        priority, index, _ = self._item.unpack(key)
        del tr[self._member[self._decode(item)][priority][index]]
        return item

    @fdb.transactional
    def _add_pop_request(self, tr, forced=False):
        index = self._get_next_index(tr.snapshot, self._pop_request)
        if index == 0 and not forced:
            return None
        request_key = self._pop_request.pack((index, self._random_ID()))
        # Protect against the unlikely event that someone else got the same
        # random ID while adding a pop request.
        tr.add_read_conflict_key(request_key)
        tr[request_key] = ''
        return request_key

    def _fulfill_requested_pops(self, db, max):
        ''' Retrieve and process a batch of requests and a batch of items. 

        We initially attempt to retrieve equally sized batches of each. However,
        the number of outstanding requests need not match the number of 
        available items; either could be larger than the other. We therefore 
        only process a number equal to the smaller of the two.
        '''
        batch = 100

        tr = db.create_transaction()
        r = self._pop_request.range()
        requests = list(tr.snapshot.get_range(r.start, r.stop, limit=batch))
        r = self._item.range()
        items = tr.snapshot.get_range(r.start, r.stop, limit=batch, reverse=max)

        i = 0
        for request, (item_key, item_value) in zip(requests, items):
            random_ID = self._pop_request.unpack(request.key)[1]
            tr[self._requested_item[random_ID]] = item_value
            tr.add_read_conflict_key(item_key)
            tr.add_read_conflict_key(request.key)
            del tr[request.key]
            del tr[item_key]
            priority, index, _ = self._item.unpack(item_key)
            del tr[self._member[item_value][priority][index]]
            i += 1

        for request in requests[i:]:
            tr.add_read_conflict_key(request.key)
            del tr[request.key]

        tr.commit().wait()

    # This implementation of pop avoids conflicts by registering a pop request
    # in a semi-ordered set of requests if it doesn't initially succeed. It then
    # enters a retry loop that attempts to fulfill outstanding requests and
    # checks to see if its request has been fulfilled.
    def _pop_high(self, db, max):

        backoff = 0.01

        tr = db.create_transaction()

        try:
            # Check if there are outstanding pop requests. If so, we may not pop
            # before them.
            request_key = self._add_pop_request(tr)
            if request_key is None:
                # No outstanding requests, so just pop.
                item = self._pop_low(tr, max)
                tr.commit().wait()
                return item
            else:
                # Commit the added pop request.
                tr.commit().wait()

        except fdb.FDBError as e:
            # If we didn't succeed, then register our pop request.
            request_key = self._add_pop_request(db, True)

        # When the pop request is eventually fufilled, its result will be stored
        # at a unique key formed from its random ID.
        random_ID = self._pop_request.unpack(request_key)[1]
        result_key = self._requested_item[random_ID]

        tr.reset()

        # Attempt to fulfill outstanding requests, then poll the database to
        # check if our request has been fulfilled.
        while 1:
            try:
                self._fulfill_requested_pops(db, max)
            except fdb.FDBError as e:
                # If the error is 1020 (not_committed), then another client has
                # probably fulfilled a batch of requests. In that case, we check
                # whether our request has been fulfilled. Otherwise, we attempt
                # to continue the retry loop.
                if e.code != 1020:
                    tr.on_error(e.code).wait()
                    continue

            try:
                tr.reset()

                if tr[request_key].present():
                    # Our request has not yet been fulfilled; try again.
                    time.sleep(backoff)
                    backoff = min(1, backoff * 2)
                    continue

                result = tr[result_key]
                if not result.present():
                    return None

                del tr[result_key]
                tr.commit().wait()
                return result

            except fdb.FDBError as e:
                tr.on_error(e.code).wait()

##################
# Internal tests #
##################


def smoke_test(db, max):
    print "\nRunning smoke test:"
    pq = PriorityQueue(fdb.directory.create_or_open(db, ('P',)), False)
    print 'Clear Priority Queue'
    pq.clear(db)
    print 'Empty? %s' % pq.empty(db)
    print 'Push 10, 8, 6'
    pq.push(db, 10, 10)
    pq.push(db, 8, 8)
    pq.push(db, 8, 7)
    pq.push(db, 6, 6)
    #pq.remove(db, 8)
    print 'Empty? %s' % pq.empty(db)
    #print 'Contains {}? {}'.format(8, 8 in pq)
    print 'Pop item: %s' % pq.pop(db, max)
    print 'Next item: %s' % pq.peek(db, max)
    print 'Pop item: %s' % pq.pop(db, max)
    print 'Pop item: %s' % pq.pop(db, max)
    print 'Pop item: %s' % pq.pop(db, max)
    print 'Empty? %s' % pq.empty(db)
    print 'Push 5'
    pq.push(db, 5, 5)
    print 'Clear Priority Queue'
    pq.clear(db)
    print 'Empty? %s' % pq.empty(db)


def single_client(db, ops):
    print "\nRunning single client example:"
    pq = PriorityQueue(fdb.directory.create_or_open(db, ('P',)), False)
    pq.clear(db)
    for i in range(ops):
        pq.push(db, i, i)
    for i in range(ops):
        print pq.pop(db, max=False)


def producer(pq, db, id, total):
    for i in range(total):
        pq.push(db, '%d.%d' % (id, i), id)


def consumer(pq, db, id, total):
    for i in range(total):
        item = pq.pop(db, max=False)
        if item:
            print 'Greenlet {} popped {}'.format(id, item)
        else:
            print 'Greenlet %d popped None' % id
    print 'Finished greenlet %d' % id

import gevent


def multi_client(db, ops, clients, high):
    description = "high-contention" if high else "low-contention"
    print '\nStarting %s test:' % description
    pq = PriorityQueue(fdb.directory.create_or_open(db, ('P',)), high)
    pq.clear(db)
    start = time.time()
    producers = [gevent.spawn(producer, pq, db, i, ops)
                 for i in range(clients)]
    consumers = [gevent.spawn(consumer, pq, db, i, ops)
                 for i in range(clients)]
    gevent.joinall(producers)
    gevent.joinall(consumers)
    end = time.time()
    print 'Finished %s queue in %f seconds' % (description, end - start)

if __name__ == '__main__':
    db = fdb.open(event_model="gevent")
    smoke_test(db, False)
    single_client(db, 10)
    #multi_client(db, 100, 10, False)
    multi_client(db, 100, 10, True)
