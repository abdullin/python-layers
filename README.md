python-layers
=============

What's here
-----------

Example layers written in Python for [FoundationDB](https://foundationdb.com). These layers require the FoundationDB [download](https://foundationdb.com/get) to work.

What are "layers"?
------------------

FoundationDB decouples its data storage technology from its data model. Layers add capabilities to FoundationDB's ordered key-value API. A layer can provide a new data model, compatibility with existing systems, or even serve as an entire framework.

These layers demonstrate the concept of layer development: that even simple layers inherit the transactional, scalable, fault-tolerant properties of FoundationDB. While functional, these layers are designed primarily to be simple, clear examples.

The layers:
-----------

 * **blob.py** - Arbitrary-sized and sparse large binary objects.
 * **bulk.py** - Bulk-loads external datasets to FoundationDB with extensible support for CSV, JSV, and blobs.
 * **counter.py** - High-performance counter that illustrates the use of dynamic sharding for high contention conditions. Note: This layer was implemented prior to the release of our atomic operations. Counters can now be more effectively implemented using an [atomic operation](https://foundationdb.com/documentation/api-python.html#atomic-operations).
 * **priorityqueue.py** - Double-ended priority queues. Items are pushed wth a specified priority, and items with either minimum or maximum priority can be popped or peeked. Supports high-contention popping for multiple clients.
 * **pubsub.py** - Message passing according to the publish-subscribe pattern. Allows management of feeds and inboxes as well as message delivery.
 * **queue.py** - Queues supporting a high contention mode for multiple clients and an optimized mode for single clients.
 * **rankedset.py** - Ranked sets supporting efficient retrieval of elements by their rank within a set as defined by their lexicographic order.
 * **scoredset.py** - Scored sets extending ranked sets to collections of items associated with scores. Items can be present at most once in the collection, but multiple items can have the same score.
 * **simpledoc.py** - A simple, hierarchical data model for storing document-oriented data. Supports a powerful plugin capability with indexes.
 * **spatial.py** - A spatial index for 2D points that allows efficient queries of axis-aligned rectangular regions. Does dimensionality reduction via a Z-order fractal curve (aka geohash).
 * **stringintern.py** - For interning (aka normalizing, aliasing) commonly-used long strings into shorter representations. Maintains the normalization state in the database, as well as a local cache for high performance.
 * **vector.py** - Vectors for storing and manipulating potentially sparse arrays.

Next steps
----------

Discussion of layers and layer development takes place on the [FoundationDB community site](http://community.foundationdb.com). (Sign up [here](https://foundationdb.com).)
