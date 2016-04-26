# coding: utf-8
import yaml
import argparse

def yaml_type(filename):
    try:
        with open(filename) as f:
            return yaml.load(f)
    except Exception as e:
        raise argparse.ArgumentTypeError(str(e))

