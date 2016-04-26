# coding: utf-8

from cloudapi import Cloud, NotFoundError
import sys
import os
import progressbar


def get_recursive_remote_filelist(client, remote_dir):
    remote_type = client.file_exists(remote_dir)
    if remote_type:
        if remote_type != 'folder':
            raise Exception('Remote %s exists but is not a folder' % remote_dir)
    else:
        return []
    files = []

    def scan_dir(folder):
        print 'SCAN_DIR', folder
        for item in client.api_folder(folder)['list']:
            if item['kind'] == 'folder':
                scan_dir(item['home'])
            else:
                files.append((item['home'], item['size']))

    scan_dir(remote_dir)
    return files


def get_recursive_local_filellist(local_dir):
    files = []

    def scan_dir(directory):
        for name in sorted(os.listdir(directory)):
            path = os.path.join(directory, name)
            if os.path.isdir(path):
                scan_dir(path)
            elif os.path.isfile(path):
                files.append((path, os.path.getsize(path)))

    scan_dir(local_dir)
    return files


def upload_dir(client, local, dest_dir):
    job = []
    changed = 0
    job_size = 0

    dest_path = os.path.join(dest_dir, os.path.basename(local))
    remote_files = dict(get_recursive_remote_filelist(client, dest_path))
    local_files = get_recursive_local_filellist(local)

    for path, size in local_files:
        remote_path = os.path.join(dest_path, os.path.relpath(path, local))
        if remote_path in remote_files:
            if remote_files[remote_path] == size:
                continue
            else:
                changed += 1
                # print remote_path, remote_files[remote_path], size
                client.api_file_remove(remote_path)
        else:
            pass
        job.append((path, remote_path, size))
        job_size += size

    gb = 1024 * 1024 * 1024
    if job_size > gb:
        job_size_str = '%.2f Gb' % (float(job_size) / gb)
    else:
        job_size_str = '%.2f Mb' % (float(job_size) / 1024 / 1024)
    print 'Total files: %d' % len(local_files)
    print '%s in %d files to upload (%d changed, %d new)' % (job_size_str, len(job), changed, len(job) - changed)
    if job:
        widgets = [
            progressbar.Percentage(),
            ' ', progressbar.Bar(),
            ' ', progressbar.ETA(),
            ' ', progressbar.FileTransferSpeed(),
        ]
        pbar = progressbar.ProgressBar(widgets=widgets, maxval=job_size)
        pbar.start()
        for path, remote_path, size in job:
            client.upload_file(remote_path, open(path))
            pbar.update(pbar.currval + size)
        pbar.finish()


def upload_file(client, local, dest_dir):
    dest_path = os.path.join(dest_dir, os.path.basename(local))
    try:
        file_info = client.api_file(dest_path)
        remote_exists = True
    except NotFoundError:
        remote_exists = False
    if remote_exists:
        if file_info['kind'] != 'file':
            raise Exception('Remote "%s" exists and not a file' % dest_path)
        local_size = os.path.getsize(local)
        if file_info['size'] != local_size:
            print "%s: file changed, updating" % dest_path
            client.api_file_remove(dest_path)
        else:
            print "%s: file not changed, skipping" % dest_path
            return
    else:
        print '%s: uploading new file' % dest_path
    client.upload_file(dest_path, open(local))


if __name__ == '__main__':
    import argparse
    import getpass

    parser = argparse.ArgumentParser()
    parser.add_argument('src', help='File or directory to upload')
    parser.add_argument('dest_dir', help='Directory to place uploaded files in')
    parser.add_argument('-c', '--credentials', help='File with credentials in form login:passwod')

    # parser.add_argument('--delete', help='Delete extraneous remote files')
    conf = parser.parse_args()
    if conf.credentials:
        with open(conf.credentials) as f:
            s = f.readline().rstrip('\r\n')
            login, password = s.split(':', )
    else:
        print 'Login:'
        login = sys.stdin.readline().rstrip('\n')
        password = getpass.getpass()

    client = Cloud(login, password)

    src = os.path.normpath(conf.src)
    try:
        if os.path.isdir(src):
            upload_dir(client, src, conf.dest_dir)
        elif os.path.isfile(src):
            upload_file(client, src, conf.dest_dir)
    except KeyboardInterrupt:
        print 'Stopped'
        exit(1)
