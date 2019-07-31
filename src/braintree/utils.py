import os
import json
import logging

LOG = logging.getLogger(__name__)

def load_config(secret_file_path,config_file_path):
    """ Load config object from config file """

    LOG.info("Loading config file from %s" % (config_file_path))
    LOG.info("Loading secret file from %s" % (secret_file_path))
    if not os.path.isfile(config_file_path):
        raise Exception('No config file under path [%s]' % config_file_path)
    if not os.path.isfile(secret_file_path):
        raise Exception('No secrets file under path [%s]' % secret_file_path)

    with open(config_file_path, "r") as config_input:
        config = json.load(config_input)
        with open(secret_file_path, "r") as pass_input:
            secrets = json.load(pass_input)
            config = merge_dicts(config, secrets)
    return config


def merge_dicts(a, b, path=[]):
    "merges a and b, b overrides a. returns new dict"

    if not a:
        return b
    elif not b:
        return a

    c = {}
    for key in a:
        c[key] = a[key]

    for key in b:
        if key in c:
            if isinstance(c[key], dict) and isinstance(b[key], dict):
                c[key] = merge_dicts(c[key], b[key], path.append(str(key)))
            elif not isinstance(c[key], dict) and not isinstance(b[key], dict):
                c[key] = b[key]
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            c[key] = b[key]
    return c
