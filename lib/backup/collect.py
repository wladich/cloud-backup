# coding: utf-8
import os
import stat
import hashlib
import logging

log = logging.getLogger('cloud_backup.collect')


def build_tree(directory, exclude):
    assert isinstance(directory, str)
    assert all(isinstance(s, str) for s in exclude)
    if not os.path.isdir(directory):
        raise ValueError('"%s" does not exist or not a directory' % directory)
    directory_size = 0
    children = []
    for name in os.listdir(directory):
        child_path = os.path.join(directory, name)
        if child_path in exclude:
            log.debug('Skipping excluded path "%s"' % child_path)
            continue
        st = os.lstat(child_path)
        # child = {'name': name, 'size': len(name.encode('utf-8')), 'st_size': st.st_size, 'st_ctime': st.st_ctime}
        child = {'name': name, 'size': len(name), 'st_size': st.st_size, 'st_ctime': st.st_ctime}
        if stat.S_ISDIR(st.st_mode):
            subtree = build_tree(child_path, exclude)
            child['size'] += subtree['size']
            child['children'] = subtree['children']
        elif stat.S_ISREG(st.st_mode):
            child['size'] += st.st_size
        elif stat.S_ISLNK(st.st_mode):
            child['size'] += st.st_size
        else:
            continue
        children.append(child)
        directory_size += child['size']
    st = os.stat(directory)
    return {'size': directory_size, 'children': children, 'name': './', 'st_size': st.st_size, 'st_ctime': st.st_ctime}


class Group(object):
    def __init__(self):
        self.paths = []
        self._fingerprint = hashlib.sha1()
        self.size = 0

    def add_item(self, item, dir_path, count_size=True, recurse=True):
        path = os.path.join(dir_path, item['name'])
        self.paths.append(path)
        # self._fingerprint.update('%s|%s|%s' % (path.encode('utf-8'), item['st_size'], item['st_ctime']))
        self._fingerprint.update('%s|%s|%s' % (path, item['st_size'], item['st_ctime']))
        if count_size:
            self.size += item['size']
        if 'children' in item and recurse:
            for child in item['children']:
                self.add_item(child, path, count_size=False)

    def fingerprint(self):
        return self._fingerprint.hexdigest()


def iterate_file_groups(items, size_low_mark, size_high_mark, from_dir):
    assert size_high_mark >= size_low_mark * 2
    items = sorted(items, key=lambda x: x['size'], reverse=True)
    group = Group()
    for it in items:
        if it['size'] > size_low_mark:
            if it['size'] > size_high_mark and 'children' in it:
                for g in iterate_file_groups(it['children'], size_low_mark, size_high_mark,
                                             os.path.join(from_dir, it['name'])):
                    g.add_item(it, from_dir, count_size=False, recurse=False)
                    yield g
            else:
                g = Group()
                g.add_item(it, from_dir)
                yield g
        else:
            if group.size + it['size'] > size_high_mark:
                yield group
                group = Group()
            group.add_item(it, from_dir)
    if group.paths:
        yield group


def get_file_groups(tree, size_low_mark, size_high_mark):
    assert size_high_mark >= size_low_mark * 2
    if tree['size'] <= size_high_mark:
        g = Group()
        g.add_item(tree, '')
        return [g]
    else:
        return list(iterate_file_groups(tree['children'], size_low_mark, size_high_mark, './'))


if __name__ == '__main__':
    import pprint
    import time
    import progressbar

    # basedir = u'/home/w/trips'
    basedir = u'//mnt/tank/home/w/.zfs/snapshot/2016-04-17_01.07.01--1d/'
    t = time.time()
    tree = (build_tree(basedir))
    groups = get_file_groups(tree, 10e6, 20e6)
    for g in groups:
        print g.fingerprint()
    print time.time() - t
