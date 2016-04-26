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
