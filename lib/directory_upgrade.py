import struct
import argparse

import fdb
import directory

fdb.api_version(100)

VERSION = (1,0,0)

class NodeInfo:
    def __init__(self, node, parent, path):
        self.node = node
        self.parent = parent
        self.path = path

class UpgradeException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return repr(self.message)

def _compare_paths(path1, path2):
    if len(path1) != len(path2):
        return False
    for n1, n2 in zip(path1, path2):
        if type(n1) != type(n2) or n1 != n2:
            return False

    return True

@fdb.transactional
def _get_version(tr, directory_layer):
    version = tr[directory_layer.root_node['version']]
    if not version.present():
        # Check for the existence of the directory layer
        if len(list(tr.get_range(directory_layer.root_node.range().start, directory_layer.root_node.range().stop, 1))) == 0:
            raise UpgradeException('ERROR: No directory present')

        return (0, 0, 0)
    else:
        return struct.unpack('<III', str(version))

@fdb.transactional
def _set_version(tr, directory_layer):
    tr[directory_layer.root_node['version']] = struct.pack('<III', *VERSION)

def upgrade(db, directory_layer, force_upgrade):
    if not hasattr(directory_layer, 'VERSION'):
        directory_layer.VERSION = (0, 0, 0)

    if directory_layer.VERSION != VERSION:
        print 'ERROR: directory layer must have version %s.%s.%s; instead it had %s.%s.%s' % (VERSION + directory_layer.VERSION)
        return

    try:
        version = _get_version(db, directory_layer)
    except UpgradeException as e:
        print e.message
        return

    upgrade_func = None
    if version == (0, 0, 0) or force_upgrade:
        upgrade_func = _upgrade_v0
    else:
        print 'Directory is already up to date. To force an upgrade, use the -f flag.'
        return

    print '[1/3] Validating that existing directories can be upgraded...'
    if not _run_upgrade(db, directory_layer, upgrade_func, dry_run=True):
        return

    print '[2/3] Upgrading directories...'
    try:
        _run_upgrade(db, directory_layer, upgrade_func, dry_run=False)
    except UpgradeException as e:
        print '  ERROR: A change to the directory layer during the upgrade process has made it so that the upgrade could not complete. Your directory may be in a partially upgraded state. Correct the following issue and rerun the upgrade to complete the process:'
        print '\n  %s' % e.message
        return
    except:
        print '  ERROR: An error occurred during the upgrade process. Your directory may be in a partially upgraded state. Resolve the problem and rerun the upgrade to complete the process.'
        raise

    print '[3/3] Setting directory version to %s.%s.%s...' % VERSION
    _set_version(db, directory_layer)

    print '\nDirectory was upgraded successfully'
    print '\nNOTE: The original directory layer implementation did not support versioning and is able to make incompatible changes to the upgraded directory. It is recommended that all clients accessing this directory use an updated version of the directory layer from this point forward. In the event that incompatible changes are made, you will need to reperform the upgrade with the -f flag.'

def _run_upgrade(db, directory_layer, func, dry_run):
    node_queue = [NodeInfo(directory_layer.root_node, None, ())]
    success = True
    while len(node_queue) > 0:
        node_queue, s = _process_queue(db, directory_layer, func, node_queue, dry_run)
        success = success and s

    return success

@fdb.transactional
def _process_queue(tr, directory_layer, upgrade_func, node_queue, dry_run):
    queue = list(node_queue)
    
    success = True
    index = 0
    while index < len(queue) and index < 100:
        node_info = queue[index]
        try:
            upgrade_func(tr, directory_layer, node_info, dry_run)
            queue.extend([NodeInfo(child_node, node_info.node, node_info.path + (name,)) for name, child_node in directory_layer._subdir_names_and_nodes(tr, node_info.node)])
        except UpgradeException as e:
            success = False
            if dry_run:
                print '  %s' % e.message
            else:
                raise

        index += 1

    return (queue[index:], success)

def _upgrade_v0(tr, directory_layer, node_info, dry_run):
    try:
        unicode_path = directory_layer._to_unicode_path(node_info.path)
    except:
        raise UpgradeException('ERROR: the path %s contains types other than byte and unicode strings' % repr(node_info.path))

    if node_info.parent and not _compare_paths(node_info.path, unicode_path): 
        # Verify that we aren't overwriting an existing directory
        new_location = tr[node_info.parent[directory_layer.SUBDIRS][unicode_path[-1]]]
        if new_location.present():
            raise UpgradeException('ERROR: the path %s can''t be upgraded because its destination %s already exists' % (repr(node_info.path), repr(unicode_path)))

        if not dry_run:
            prefix = tr[node_info.parent[directory_layer.SUBDIRS][node_info.path[-1]]]
            del tr[node_info.parent[directory_layer.SUBDIRS][node_info.path[-1]]]
            tr[node_info.parent[directory_layer.SUBDIRS][unicode_path[-1]]] = prefix

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Upgrades a directory hierarchy created with an older version of the directory layer to version %s.%s.%s' % VERSION)
    parser.add_argument('-C', dest='cluster_file', type=str, help='The cluster file for the database where the directory resides. If none is specified, then the default cluster file is used.', default=None)
    parser.add_argument('-f', dest='force', action='store_true', help='Attempt an upgrade even if the version of the directory is current. This is useful if modifications were made to an upgraded directory from the original directory layer.')
    parser.add_argument('--node-subspace', dest='node_subspace', type=str, help='The node subspace that the directory was created with. If none is specified, then the default node subspace is used.', default=None)
    # parser.add_argument('--upgrade-partitions', dest='upgrade-partitions', action='store_true' help='If set, then partitions found in the directory hierarchy will be opened and upgraded recursively.'

    args = parser.parse_args()

    print ''

    try:
        db = fdb.open(args.cluster_file)
        if args.node_subspace:
            upgrade_dir = directory.DirectoryLayer(node_subspace=args.node_subspace)
        else:
            upgrade_dir = directory.directory

        upgrade(db, upgrade_dir, args.force)
    except fdb.FDBError as e:
        print str(e)

    print ''

