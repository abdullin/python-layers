''' FoundationDB Directory Layer.

Provides a DirectoryLayer class for managing directories in FoundationDB.
Directories are a recommended approach for administering layers and
applications. Directories work in conjunction with subspaces. Each layer or
application should create or open at least one directory with which to manage
its subspace(s).

Directories are identified by paths (specified as tuples) analogous to the paths
in a Unix-like file system. Each directory has an associated subspace that is
used to store content. The layer uses a high-contention allocator to efficiently
map each path to a short prefix for its corresponding subspace.

DirectorLayer exposes methods to create, open, move, remove, or list
directories. Creating or opening a directory returns the corresponding subspace.

The DirectorySubspace class represents subspaces that store the contents of a
directory. An instance of DirectorySubspace can be used for all the usual
subspace operations. It can also be used to operate on the directory with which
it was opened.
'''

import random
import struct

import fdb
import fdb.tuple
from subspace import Subspace

fdb.api_version(100)


class HighContentionAllocator (object):

    def __init__(self, subspace):
        self.counters = subspace[0]
        self.recent = subspace[1]

    @fdb.transactional
    def allocate(self, tr):
        """Returns a byte string that
            1) has never and will never be returned by another call to this
               method on the same subspace
            2) is nearly as short as possible given the above
        """
        [(start, count)] = [(self.counters.unpack(k)[0], struct.unpack("<q", v)[0])
                            for k, v in tr.snapshot.get_range(self.counters.range().start, self.counters.range().stop, limit=1, reverse=True)] or [(0, 0)]

        window = self._window_size(start)
        if (count + 1) * 2 >= window:
            # Advance the window
            del tr[self.counters: self.counters[start].key() + chr(0)]
            start += window
            del tr[self.recent: self.recent[start]]
            window = self._window_size(start)

        # Increment the allocation count for the current window
        tr.add(self.counters[start], struct.pack("<q", 1))

        while True:
            # As of the snapshot being read from, the window is less than half
            # full, so this should be expected to take 2 tries.  Under high
            # contention (and when the window advances), there is an additional
            # subsequent risk of conflict for this transaction.
            candidate = random.randint(start, start + window)
            if tr[self.recent[candidate]] == None:
                tr[self.recent[candidate]] = ""
                return fdb.tuple.pack((candidate,))

    def _window_size(self, start):
        # Larger window sizes are better for high contention, smaller sizes for
        # keeping the keys small.  But if there are many allocations, the keys
        # can't be too small.  So start small and scale up.  We don't want this
        # to ever get *too* big because we have to store about window_size/2 
        # recent items.
        if start < 255: return 64
        if start < 65535: return 1024
        return 8192


# TODO: layer string should be unicode
class DirectoryLayer (object):

    def __init__(self, node_subspace=Subspace(rawPrefix="\xfe"), content_subspace=Subspace()):
        # If specified, new automatically allocated prefixes will all fall within content_subspace
        self.content_subspace = content_subspace
        self.node_subspace = node_subspace
        # The root node is the one whose contents are the node subspace
        self.root_node = self.node_subspace[self.node_subspace.key()]
        self.allocator = HighContentionAllocator(self.root_node['hca'])
        self.path = ()

    @fdb.transactional
    def create_or_open(self, tr, path, layer=None, prefix=None, allow_create=True, allow_open=True):
        """ Opens the directory with the given path.

        If the directory does not exist, it is created (creating parent 
        directories if necessary).

        If prefix is specified, the directory is created with the given physical
        prefix; otherwise a prefix is allocated automatically.

        If layer is specified, it is checked against the layer of an existing
        directory or set as the layer of a new directory.
        """
        self._check_version(tr, write_access=False)

        if not path:
            # Root directory contains node metadata and so may not be opened.
            raise ValueError("The root directory may not be opened.")

        path = self._to_unicode_path(path)

        existing_node = self._find(tr, path).prefetch_metadata(tr)
        if existing_node.exists():
            if existing_node.is_in_partition():
                subpath = existing_node.get_partition_subpath()
                return existing_node.get_contents(self).create_or_open(tr, subpath, layer, prefix, allow_create, allow_open)

            if not allow_open:
                raise ValueError("The directory already exists.")

            if layer and existing_node.layer() != layer:
                raise ValueError("The directory exists but was created with an incompatible layer.")

            return existing_node.get_contents(self)

        if not allow_create:
            raise ValueError("The directory does not exist.")

        self._check_version(tr)

        if prefix == None:
            prefix = self.content_subspace.key() + self.allocator.allocate(tr)

        if not self._is_prefix_free(tr, prefix):
            raise ValueError("The given prefix is already in use.")

        if path[:-1]:
            parent_node = self._node_with_prefix(self.create_or_open(tr, path[:-1], layer=None).key())
        else:
            parent_node = self.root_node
        if not parent_node:
            print repr(path[:-1])
            raise ValueError("The parent directory doesn't exist.")

        node = self._node_with_prefix(prefix)
        tr[parent_node[self.SUBDIRS][path[-1]]] = prefix
        if layer: tr[node['layer']] = layer
        
        return self._contents_of_node(node, path, layer)

    def open(self, db_or_tr, path, layer=None):
        """ Opens the directory with the given path.

        An error is raised if the directory does not exist, or if a layer is
        specified and a different layer was specified when the directory was
        created.
        """
        return self.create_or_open(db_or_tr, path, layer, allow_create=False)

    def create(self, db_or_tr, path, layer=None, prefix=None):
        """Creates a directory with the given path (creating parent directories
           if necessary).

        An error is raised if the given directory already exists.

        If prefix is specified, the directory is created with the given physical
        prefix; otherwise a prefix is allocated automatically.

        If layer is specified, it is recorded with the directory and will be
        checked by future calls to open.
        """
        return self.create_or_open(db_or_tr, path, layer, prefix, allow_open=False)

    @fdb.transactional
    def move(self, tr, old_path, new_path):
        """Moves the directory found at `old_path` to `new_path`.

        There is no effect on the physical prefix of the given directory, or on
        clients that already have the directory open.

        An error is raised if the old directory does not exist, a directory
        already exists at `new_path`, or the parent directory of `new_path` does
        not exist.
        """
        self._check_version(tr)

        old_path = self._to_unicode_path(old_path)
        new_path = self._to_unicode_path(new_path)

        if old_path == new_path[:len(old_path)]:
            raise ValueError("The destination directory cannot be a subdirectory of the source directory.")
        
        old_node = self._find(tr, old_path).prefetch_metadata(tr)
        new_node = self._find(tr, new_path).prefetch_metadata(tr)

        if not old_node.exists():
            raise ValueError("The source directory does not exist.")

        if old_node.is_in_partition() or new_node.is_in_partition():
            if not old_node.is_in_partition() or not new_node.is_in_partition() or old_node.path != new_node.path: 
                raise ValueError("Cannot move between partitions.")

            return new_node.get_contents(self).move(tr, old_node.get_partition_subpath(), new_node.get_partition_subpath())

        if new_node.exists():
            raise ValueError("The destination directory already exists. Remove it first.")

        parent_node = self._find(tr, new_path[:-1])
        if not parent_node.exists():
            raise ValueError("The parent of the destination directory does not exist. Create it first.")
        tr[parent_node.subspace[self.SUBDIRS][new_path[-1]]] = self.node_subspace.unpack(old_node.subspace.key())[0]
        self._remove_from_parent(tr, old_path)
        return self._contents_of_node(old_node.subspace, new_path, old_node.layer())

    @fdb.transactional
    def remove(self, tr, path):
        """Removes the directory, its contents, and all subdirectories.

        Warning: Clients that have already opened the directory might still
        insert data into its contents after it is removed.
        """
        self._check_version(tr)

        path = self._to_unicode_path(path)
        node = self._find(tr, path).prefetch_metadata(tr)
        if not node.exists():
            raise ValueError("The directory doesn't exist.")

        if node.is_in_partition():
            return node.get_contents(self).remove(tr, node.get_partition_subpath())

        self._remove_recursive(tr, node.subspace)
        self._remove_from_parent(tr, path)

    @fdb.transactional
    def list(self, tr, path=()):
        self._check_version(tr, write_access=False)

        path = self._to_unicode_path(path)
        node = self._find(tr, path).prefetch_metadata(tr)
        if not node.exists():
            raise ValueError("The given directory does not exist.")

        if node.is_in_partition(include_empty_subpath=True):
            return node.get_contents(self).list(tr, node.get_partition_subpath())

        return [name for name, cnode in self._subdir_names_and_nodes(tr, node.subspace)]

    ########################################
    ## Private methods for implementation ##
    ########################################

    SUBDIRS=0
    VERSION=(1,0,0)

    def _check_version(self, tr, write_access=True):
        # TODO: Is it ok that we read the key every time? Seems the safest 
        version = tr[self.root_node['version']]

        if not version.present():
            if write_access:
                self._initialize_directory(tr)

            return

        version = struct.unpack('<III', str(version))

        if version[0] > self.VERSION[0]:
            raise Exception("Cannot load directory with version %d.%d.%d using directory layer %d.%d.%d" % (version + self.VERSION)) # TODO: different error type?

        if version[1] > self.VERSION[1] and write_access:
            raise Exception("Directory with version %d.%d.%d is read-only when opened using directory layer %d.%d.%d" % (version + self.VERSION)) # TODO: different error type?

    def _initialize_directory(self, tr):
        tr[self.root_node['version']] = struct.pack('<III', *self.VERSION)

    def _node_containing_key(self, tr, key):
        # Right now this is only used for _is_prefix_free(), but if we add
        # parent pointers to directory nodes, it could also be used to find a
        # path based on a key.
        if key.startswith(self.node_subspace.key()):
            return self.root_node
        for k, v in tr.get_range(self.node_subspace.range(()).start,
                                 self.node_subspace.pack((key,)) + "\x00",
                                 reverse=True,
                                 limit=1):
            prev_prefix = self.node_subspace.unpack(k)[0]
            if key.startswith(prev_prefix):
                return Subspace(rawPrefix=k)  # self.node_subspace[prev_prefix]
        return None

    def _node_with_prefix(self, prefix):
        if prefix == None: return None
        return self.node_subspace[prefix]

    def _contents_of_node(self, node, path, layer=None):
        prefix = self.node_subspace.unpack(node.key())[0]

        if layer == 'partition':
            partition = DirectoryLayer(Subspace(rawPrefix=prefix+"\xfe"), Subspace(rawPrefix=prefix))
            partition.path = self.path + path
            return partition
        
        return DirectorySubspace(path, prefix, self, layer)

    def _find(self, tr, path):
        n = Node(self.root_node, [], path)
        for i, name in enumerate(path):
            n = Node(self._node_with_prefix(tr[n.subspace[self.SUBDIRS][name]]), path[:i+1], path)
            if not n.exists() or n.layer(tr) == 'partition':
                return n
        return n

    def _subdir_names_and_nodes(self, tr, node):
        sd = node[self.SUBDIRS]
        for k, v in tr[sd.range(())]:
            yield sd.unpack(k)[0], self._node_with_prefix(v)

    def _remove_from_parent(self, tr, path):
        parent = self._find(tr, path[:-1])
        del tr[parent.subspace[self.SUBDIRS][path[-1]]]

    def _remove_recursive(self, tr, node):
        for name, sn in self._subdir_names_and_nodes(tr, node):
            self._remove_recursive(tr, sn)
        tr.clear_range_startswith(self.node_subspace.unpack(node.key())[0])
        del tr[node.range(())]

    def _is_prefix_free(self, tr, prefix):
        # Returns true if the given prefix does not "intersect" any currently
        # allocated prefix (including the root node). This means that it neither
        # contains any other prefix nor is contained by any other prefix.
        prefix_range = self.node_subspace.range((prefix,))
        return prefix and not self._node_containing_key(tr, prefix) and not len(list(tr.get_range(prefix_range.start, prefix_range.stop, limit=1))) # TODO: make sure this change is correct

    def _to_unicode_path(self, path):
        if isinstance(path, str):
            path = unicode(path)

        if isinstance(path, unicode):
            return (path,)

        if isinstance(path, tuple):
            path = list(path)
            for i, name in enumerate(path):
                if isinstance(name, str):
                    path[i] = unicode(path[i])
                elif not isinstance(name, unicode):
                    raise ValueError('Invalid path: must be a unicode string or a tuple of unicode strings')

            return tuple(path)

        raise ValueError('Invalid path: must be a unicode string or a tuple of unicode strings')


directory = DirectoryLayer()


class DirectorySubspace (Subspace):
    # A DirectorySubspace represents the *contents* of a directory, but it also
    # remembers the path with which it was opened and offers convenience methods
    # to operate on the directory at that path.

    def __init__(self, path, prefix, directoryLayer=directory, layer=None):
        Subspace.__init__(self, rawPrefix=prefix)
        self.path = path
        self.directoryLayer = directoryLayer
        self.layer = layer

    def __repr__(self):
        return 'DirectorySubspace(' + repr(self.path) + ',' + repr(self.rawPrefix) + ')'

    def check_layer(self, layer):
        if layer and self.layer and layer!=self.layer: # TODO: how to deal with self.layer not being set
            raise ValueError("The directory was created with an incompatible layer.")

    def create_or_open(self, db_or_tr, name_or_path, layer=None, prefix=None):
        if not isinstance(name_or_path, tuple):
            name_or_path = (name_or_path,)
        return self.directoryLayer.create_or_open(db_or_tr, self.path + name_or_path, layer, prefix)

    def open(self, db_or_tr, name_or_path, layer=None):
        if not isinstance(name_or_path, tuple):
            name_or_path = (name_or_path,)
        return self.directoryLayer.open(db_or_tr, self.path + name_or_path, layer)

    def create(self, db_or_tr, name_or_path, layer=None):
        if not isinstance(name_or_path, tuple):
            name_or_path = (name_or_path,)
        return self.directoryLayer.create(db_or_tr, self.path + name_or_path, layer)

    def move(self, db_or_tr, new_path):
        new_path = self.directoryLayer._to_unicode_path(new_path)
        partition_path = new_path[:len(self.directoryLayer.path)]
        if partition_path != self.directoryLayer.path:
            raise ValueError("Cannot move between partitions.")

        return self.directoryLayer.move(db_or_tr, self.path, new_path[len(self.directoryLayer.path):])

    def remove(self, db_or_tr):
        return self.directoryLayer.remove(db_or_tr, self.path)

    def list(self, db_or_tr):
        return self.directoryLayer.list(db_or_tr, self.path)

class Node (object):

    def __init__(self, subspace, path, target_path):
        self.subspace = subspace
        self.path = path
        self.target_path = target_path
        self._layer = None

    def exists(self):
        return self.subspace is not None

    def prefetch_metadata(self, tr):
        if self.exists():
            self.layer(tr)

        return self

    def layer(self, tr=None):
        if tr:
            self.loaded = True
            self._layer = tr[self.subspace['layer']]
        elif self._layer is None:
            raise Exception('Layer has not been read')

        return self._layer

    def is_in_partition(self, tr=None, include_empty_subpath=False):
        return self.exists() and self.layer(tr) == 'partition' and (include_empty_subpath or len(self.target_path) > len(self.path))

    def get_partition_subpath(self):
        return self.target_path[len(self.path):]

    def get_contents(self, directory_layer, tr=None):
        return directory_layer._contents_of_node(self.subspace, self.path, self.layer(tr))


if __name__ == '__main__':
    # If module is run as a script, print the directory tree.  This code will
    # not work well if there are huge numbers of directories!
    @fdb.transactional
    def printdirs(tr, root, indent=""):
        for name in root.list(tr):
            child = root.open(tr, name)
            print indent + name, child.layer or ""
            printdirs(tr, child, indent + "  ")
    db = fdb.open()
    printdirs(db, directory)
