# -*- coding: utf-8 -*-

import sys
import argparse
import requests

def auth():
    pass

def ls():
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    auth_parser = subparsers.add_parser('auth')
    auth_parser.set_defaults(func=auth)
    ls_parser = subparsers.add_parser('ls')
    ls_parser.set_defaults(func=ls)
    parser.parse_args(sys.argv[1:])


