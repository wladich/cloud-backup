# coding: utf-8
import subprocess
import time
import tempfile


def encode_files(paths, root, passphrase):
    with tempfile.NamedTemporaryFile() as paths_list:
        for s in paths:
            paths_list.write(s.replace('\\', '\\\\') + '\n')
        paths_list.flush()
        tar = subprocess.Popen(['/bin/tar', 'c', '--no-recursion', '-T', paths_list.name], cwd=root,
                               stdout=subprocess.PIPE, stdin=subprocess.PIPE)

        gpg = subprocess.Popen(
            ['/usr/bin/gpg', '--symmetric', '-o', '-', '--passphrase-file', passphrase, '--compress-algo=zlib',
             '--compress-level=1', '-q', '--no-tty'],
            stdin=tar.stdout, stdout=subprocess.PIPE)

        while True:
            s = gpg.stdout.read(4096)
            if not s:
                break
            yield s

    t = time.time()
    timeout = 1
    while time.time() < t + timeout:
        if tar.poll() is not None and gpg.poll() is not None:
            break
        time.sleep(.001)
    if tar.returncode != 0 or gpg.returncode != 0:
        raise IOError('Encoding failed: tar returned %s, gpg returned %s' % (tar.returncode, gpg.returncode))


def decode_files(reader, dest_dir, passphrase):
    gpg = subprocess.Popen(['/usr/bin/gpg', '-d', '-o', '-', '--passphrase-file', passphrase, '-q'],
                           stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    tar = subprocess.Popen(
        ['tar', 'x', '--preserve-permissions', '--same-owner', '--delay-directory-restore', '--atime-preserve'],
        cwd=dest_dir, stdin=gpg.stdout)
    for s in reader:
        if tar.poll() is not None or gpg.poll() is not None:
            raise IOError('Decoding failed: tar returned %s, gpg returned %s' % (tar.returncode, gpg.returncode))
        gpg.stdin.write(s)
    gpg.stdin.close()
    t = time.time()
    timeout = 1
    while time.time() < t + timeout:
        if tar.poll() is not None and gpg.poll() is not None:
            break
        time.sleep(.001)
    if tar.poll() != 0 or gpg.poll() != 0:
        raise IOError('Decoding failed: tar returned %s, gpg returned %s' % (tar.returncode, gpg.returncode))


if __name__ == '__main__':
    import time

    size = 0
    t = time.time()
    for s in encode_files(['bin', 'orient.db', 'opt'], '/home/w', 'test_config/passphrase'):
        size += len(s)
        print '\r', size / 1e6,
    print time.time() - t
    print 'Done'
