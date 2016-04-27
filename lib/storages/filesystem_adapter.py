# coding: utf-8
import re


def join_paths(*args):
    if not args or not args[0] or args[0][0] != '/':
        raise ValueError('Invalid path components: %s' % (args,))
    return '/' + '/'.join(p.strip('/') for p in args)


def split_iterator(iterator, max_size):
    done = [False]
    while not done[0]:
        def subiterator():
            size = 0
            for s in iterator:
                yield s
                size += len(s)
                if max_size is not None and size >= max_size:
                    break
            else:
                done[0] = True

        yield subiterator()


class FilesystemAdapter(object):
    def __init__(self, root, max_file_size=None):
        self.root = root
        self.max_file_size = max_file_size

    def listdir(self, path):
        """
            returns list of tuples [(name1, "file"), (name2, "folder"), ...]
        """
        raise NotImplementedError

    def exists(self, path):
        """
            returns "file", "folder" or False
        """
        raise NotImplemented

    def delete_file(self, path):
        raise NotImplementedError

    def get_file_read_iterator(self, path):
        raise NotImplementedError

    # def get_file_writer(self, path):
    #     raise NotImplementedError

    def write_file(self, path, writer):
        raise NotImplementedError

    def delete_directory(self, path):
        raise NotImplementedError

    def put(self, volume, key, iterator):
        for part, it in enumerate(split_iterator(iterator, self.max_file_size), 1):
            path = self.make_key_path(volume, key, part)
            self.write_file(path, it)

    def get(self, volume, key, parts):
        for part in sorted(parts):
            path = self.make_key_path(volume, key, part)
            for s in self.get_file_read_iterator(path):
                yield s

    def get_key_bucket(self, key):
        assert re.match(r'^[0-9a-f]{40}$', key)
        return key[:2]

    def make_key_path(self, volume, key, part_n):
        return join_paths(self.root, volume, self.get_key_bucket(key), '%s-%d' % (key, part_n))

    def remove(self, volume, key, parts):
        for n in parts:
            path = self.make_key_path(volume, key, n)
            self.delete_file(path)

    def list_keys(self, volume):
        volume_path = join_paths(self.root, volume)
        exists = self.exists(volume_path)
        if not exists:
            return {}
        if exists == 'folder':
            keys_parts = {}
            for bucket, kind in self.listdir(volume_path):
                if kind == 'folder' and re.match('^[0-9a-f]{2}$', bucket):
                    for name, kind in self.listdir(join_paths(volume_path, bucket)):
                        if kind == 'file':
                            m = re.match(r'^([0-9a-f]{40})-([0-9]+)$', name)
                            if m:
                                key = m.group(1)
                                part_n = int(m.group(2))
                                keys_parts[key] = keys_parts.get(key, []) + [part_n]
            return keys_parts
        else:
            raise IOError('"%s" is not a directory')

    # def list_volumes(self):
    #     exists = self.exists(self.root)
    #     if not exists:
    #         return []
    #     elif exists == 'folder':
    #         return [f for f, e in self.listdir(self.root) if e == 'folder']
    #     else:
    #         raise IOError('"%s" is not a directory')

    def cleanup(self, volume):
        volume_path = join_paths(self.root, volume)
        if self.exists(volume_path) == 'folder':
            for bucket, kind in self.listdir(volume_path):
                if kind == 'folder' and re.match('^[0-9a-f]{2}$', bucket):
                    bucket_path = join_paths(volume_path, bucket)
                    if not self.listdir(bucket_path):
                        self.delete_directory(bucket_path)

