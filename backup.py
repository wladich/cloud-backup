#!/usr/bin/env python
# coding: utf-8
import argparse
import logging
import os
import sys
import time

import storages
from lib.backup import utils, collect, encode

log = logging.getLogger('cloud_backup.sync')
logging.basicConfig(level=logging.INFO)
logging.getLogger('requests.packages.urllib3').setLevel(logging.WARN)


class Progress(object):
    def __init__(self, total):
        self.total = total
        self.pos = 0
        self.start = time.time()

    def update_progress(self, increment):
        self.pos += increment

    def format(self):
        done = float(self.pos) / self.total
        running = time.time() - self.start
        eta = running / done - running
        if eta < 60:
            eta = '%d seconds' % eta
        elif eta < 3600:
            eta = '%.2f minutes' % (eta / 60.)
        else:
            eta = '%.2f hours' % (eta / 3600.)
        return '%.1f%% ETA: %s' % (100. * done, eta)


def sync_volume(volume_name, volumes_conf, storages_conf, passphrase):
    log.info('Starting sync volume "%s"' % volume_name)
    volume = volumes_conf[volume_name]
    volume_path = volume['path']#.decode('utf-8')
    exclude_paths = [os.path.join(volume_path, os.path.normpath(p)) for p in volume.get('exclude', [])]
    tree = collect.build_tree(volume_path, exclude=exclude_paths)
    log.debug('Built tree for volume "%s", total size %d bytes' % (volume_name, tree['size']))
    for storage_name in volume['storages']:
        log.info('Starting sync volume "%s" to storage "%s"' % (volume_name, storage_name))
        storage_conf = storages_conf[storage_name]
        groups = collect.get_file_groups(tree, storage_conf['min_file_size'], storage_conf['min_file_size'] * 2)
        log.info('%d file groups in volume "%s"' % (len(groups), volume_name))
        storage = storages.get_storage(storage_conf['class'], storage_conf['params'])
        remote_key_parts = storage.list_keys(volume_name)
        log.info('Found %d keys in storage', len(remote_key_parts))
        groups_by_keys = dict((g.fingerprint(), g.paths) for g in groups)
        remote_keys_set = set(remote_key_parts.keys())
        local_keys_set = set(groups_by_keys.keys())
        keys_to_delete = remote_keys_set - local_keys_set
        if keys_to_delete == remote_keys_set and len(remote_keys_set) > 3:
            raise Exception('Refusing to delete all %d remote keys' % len(remote_keys_set))
        log.info('%d keys in storage will be deleted', len(keys_to_delete))
        for key in keys_to_delete:
            log.debug('Removing key "%s"', key)
            storage.remove(volume_name, key, remote_key_parts[key])
        keys_to_add = local_keys_set - remote_keys_set
        log.info('%d keys will be uploaded', len(keys_to_add))
        progress = Progress(len(keys_to_add))
        for key in keys_to_add:
            log.debug('Uploading key "%s"', key)
            paths = groups_by_keys[key]
            # encoded = encode.encode_files(paths, volume['path'].decode('utf-8'), passphrase)
            encoded = encode.encode_files(paths, volume['path'], passphrase)
            storage.put(volume_name, key, encoded)
            progress.update_progress(1)
            print '\r', progress.format(),
            sys.stdout.flush()
        print '\n'
        storage.cleanup(volume_name)


if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument('-vc', help='volumes config file', metavar='FILE.yaml', required=True, type=utils.yaml_type)
        parser.add_argument('-sc', help='storages config file', metavar='FILE.yaml', required=True, type=utils.yaml_type)
        parser.add_argument('-p', help='file with passphrase', metavar='FILE', required=True)
        conf = parser.parse_args()


        for volume_name in conf.vc.keys():
            sync_volume(volume_name, conf.vc, conf.sc, conf.p)
