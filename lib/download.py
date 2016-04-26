# coding: utf-8

from cloudapi import Cloud
import sys
import os

# def upload(client, local, dest_dir, delete_extraneous):
def upload(client, local, dest_dir):
    dest_path = os.path.join(dest_dir, os.path.basename(local))
    if os.path.isdir(local):
        for name in os.listdir(local):
            # upload(client, os.path.join(local, name), dest_path, delete_extraneous)
            upload(client, os.path.join(local, name), dest_path)
    else:
        client.upload_file(dest_path, open(local))


if __name__ == '__main__':
    import argparse
    import getpass

    parser = argparse.ArgumentParser()
    parser.add_argument('src', help='Remote file or directory to downlod')
    parser.add_argument('dest_dir', help='Directory to place downloaded files in')
    # parser.add_argument('--delete', help='Delete extraneous remote files')
    conf = parser.parse_args()

    print 'Login:'
    # login = sys.stdin.readline().rstrip('\n')
    # password = getpass.getpass()
    login = 'wladimirych'
    password = 'totymote'


    client = Cloud(login, password)
    # upload(client, conf.src, conf.dest_dir, conf.delete)
    upload(client, conf.src, conf.dest_dir)