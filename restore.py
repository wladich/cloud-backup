# coding: utf-8
import os
import argparse
import utils
import storages
import encode
import  logging

log = logging.getLogger('cloud_backup_restore')
logging.basicConfig(level=logging.DEBUG)

def restore_volume_from_dir(src_dir, dest_dir, passphrase):
    pass


def restore_volume(volume_name, storage_name, dest_dir, storages_conf, passphrase):
    dest_dir = os.path.join(dest_dir, volume_name)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    storage_conf = storages_conf[storage_name]
    storage = storages.get_storage(storage_conf['class'], storage_conf['params'])
    remote_keys_parts = storage.list_keys(volume_name)
    log.debug('%d keys found in storage', len(remote_keys_parts))
    for key, parts in remote_keys_parts.items():
        log.debug('Restoring key "%s"', key)
        reader = storage.get(volume_name, key, parts)
        encode.decode_files(reader, dest_dir, passphrase)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', help='volume name', required=True)
    parser.add_argument('-s', help='storage name', required=True)
    parser.add_argument('-d', help='dest dir', required=True)

    parser.add_argument('-sc', help='storages config file', metavar='FILE.yaml', required=True, type=utils.yaml_type)
    parser.add_argument('-p', help='file with passphrase', metavar='FILE', required=True)
    conf = parser.parse_args()

    if not os.path.isdir(conf.d):
        print 'Destination directory "%s" does not exist' % conf.d
        exit(1)
    restore_volume(volume_name=conf.v, storage_name=conf.s, dest_dir=conf.d, storages_conf=conf.sc, passphrase=conf.p)
