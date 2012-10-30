# TODO
"""pyvsb configuration file parser."""

import copy
import imp
import os
import re

from .core import Error


def get_config(path):
    """Parses the configuration file."""

    if not os.path.exists(path):
        raise Error("It doesn't exist.")

    config_obj = imp.load_source("config_obj", path)

    config = {}

    _get_param(config_obj, config, "backup_root", str, validate = _validate_path)
    _get_param(config_obj, config, "backup_items", dict,
        validate = _validate_backup_items)

    _get_param(config_obj, config, "max_backups", int)
    _get_param(config_obj, config, "max_backup_groups", int)

    _get_param(config_obj, config, "trust_modify_time", bool, default = True) # TODO

    return config


def _get_param(config_obj, config, name, value_type, default = None, validate = lambda value: value):
    """Gets the specified parameter from config."""

    config_name = name.upper()

    if hasattr(config_obj, config_name):
        value = getattr(config_obj, config_name)
    else:
        if default is None:
            raise Error("Missing required configuration parameter '{}'.", config_name)

        value = default

    if type(value) != value_type:
        raise Error("Invalid value type for configuration parameter '{}'.", config_name)

    config[name] = validate(value)


def _validate_backup_items(backup_items):
    """Validates backup items."""

    items = {}

    for path, params in backup_items.items():
        if type(path) != str:
            raise Error("Invalid value type backup item path.")

        path = _validate_path(path)
        params = copy.deepcopy(params)

        if type(params) != dict:
            raise Error("Invalid value type backup item parameters.")

        for param, value in params.items():
            if type(param) != str:
                raise Error("Backup item parameter name must be a string.")

            if param in ( "before", "after" ):
                if type(value) != str:
                    raise Error("Backup item's '{}' parameter must be a string.", param)
            elif param == "filter":
                if type(value) != list or any(type(regex) != str for regex in value):
                    raise Error("Backup item's '{}' parameter must be a list of strings.", param)

                regexes = []

                for regex in value:
                    policy = regex[0:1]
                    if policy not in ( "-", "+" ):
                        raise Error("Invalid backup item's filter '{}': "
                            "it must be prepended with filtering policy ( '-' or '+' ).", regex)

                    regex = regex[1:]

                    try:
                        regex = re.compile(regex)
                    except Exception as e:
                        raise Error("Invalid backup item's filter '{}': {}.", regex, e)

                    regexes.append(( policy == "+", regex ))

                params[param] = regexes
            else:
                raise Error("Invalid backup item parameter: '{}'.", param)

        items[path] = params

    return items


def _validate_path(path):
    """Checks a path specified in the configuration file."""

    if not os.path.isabs(path):
        raise Error("Invalid path '{}': it must be an absolute path.", path)

    return os.path.normpath(path)
