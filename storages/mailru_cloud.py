# coding: utf-8
import logging

import filesystem_adapter
from lib.mailru_cloud import cloudapi

log = logging.getLogger('cloud_backup_sync.mru_cloud_storage')


class MailruCloud(filesystem_adapter.FilesystemAdapter):
    def __init__(self, login, password, root, max_file_size=None):
        super(MailruCloud, self).__init__(root, max_file_size)
        self.client = cloudapi.Cloud(login, password)

    def listdir(self, path):
        resp = self.client.api_folder(path)
        result = []
        for item in resp['list']:
            result.append((item['name'], item['kind']))
        return result

    def exists(self, path):
        return self.client.file_exists(path)

    def delete_file(self, path):
        log.debug('Deleting file "%s"', path)
        self.client.api_file_remove(path)

    def delete_directory(self, path):
        log.debug('Deleting directory "%s"', path)
        self.client.api_file_remove(path)

    def write_file(self, path, iterator):
        log.debug('Writing file "%s"', path)
        self.client.upload_file(path, ''.join(s for s in iterator))

    def get_file_read_iterator(self, path):
        f = self.client.get_file_reader(path)
        try:
            while True:
                s = f.read()
                if not s:
                    break
                yield s
        finally:
            f.close()
