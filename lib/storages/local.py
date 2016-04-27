# coding: utf-8
import logging
import os

import filesystem_adapter

log = logging.getLogger('cloud_backup.local_storage')


class Local(filesystem_adapter.FilesystemAdapter):
    def listdir(self, path):
        result = []
        for name in os.listdir(path):
            item_path = os.path.join(path, name)
            if os.path.isdir(item_path):
                result.append((name, 'folder'))
            elif os.path.isfile(item_path):
                result.append((name, 'file'))
        return result

    def exists(self, path):
        path = os.path.join(self.root, path)
        if os.path.isdir(path):
            return 'folder'
        elif os.path.isfile(path):
            return 'file'
        else:
            return False

    def delete_file(self, path):
        log.debug('Deleting file "%s"', path)
        os.remove(path)

    def delete_directory(self, path):
        os.rmdir(path)

    def write_file(self, path, iterator):
        log.debug('Writing file "%s"', path)
        if not os.path.exists(self.root):
            os.makedirs(self.root)
        temp_file = os.path.join(self.root, '__upload_temp')
        with open(temp_file, 'w') as f:
            for s in iterator:
                f.write(s)
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        os.rename(temp_file, path)

    def get_file_read_iterator(self, path):
        with open(path) as f:
            while True:
                s = f.read(4096)
                if not s:
                    break
                yield s
