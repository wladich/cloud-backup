# coding: utf-8
import yaml
import argparse


def yaml_type(filename):
    try:
        with open(filename) as f:
            return yaml.load(f)
    except Exception as e:
        raise argparse.ArgumentTypeError(str(e))


def load_credentials(filename):
    with open(filename) as f:
        s = f.readline().rstrip('\r\n')
        login, password = s.split(':', )
        return login, password


def human_format_size(n):
    if n < 1024:
        return '%d bytes' % n
    if n < 1024 * 1024:
        return '%.1f Kb' % (n / 1024.)
    if n < 1024 * 1024 * 1024:
        return '%.1f Mb' % (n / 1024. / 1024)
    return '%.1f Gb' % (n / 1024. / 1024 / 1024)
