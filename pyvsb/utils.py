"""Various utils."""

import grp
import pwd

_DB_ENTRIES_CACHE = {}
"""A DB entries cache."""


def getgrgid(gid):
    """Cached grp.getgrgid()."""

    return _get_gr_entries()[2][gid]


def getgrnam(name):
    """Cached grp.getgrnam()."""

    return _get_gr_entries()[0][name]


def getpwuid(uid):
    """Cached pwd.getpwuid()."""

    return _get_pwd_entries()[2][uid]


def getpwnam(name):
    """Cached pwd.getpwnam()."""

    return _get_pwd_entries()[0][name]


def _get_db_entries(name, func):
    """Returns cached DB entries.

    grp and pwd modules reread /etc/group and /etc/passwd files on each method
    call.
    """

    cache = _DB_ENTRIES_CACHE.get(name)

    if cache is None:
        id_cache = {}
        name_cache = {}

        cache = {
            2: id_cache,
            0: name_cache,
        }

        for entry in func():
            id_cache[entry[2]] = name_cache[entry[0]] = entry

        _DB_ENTRIES_CACHE[name] = cache

    return cache


def _get_gr_entries():
    """Returns cached grp database entries."""

    return _get_db_entries("grp", grp.getgrall)


def _get_pwd_entries():
    """Returns cached pwd database entries."""

    return _get_db_entries("pwd", pwd.getpwall)
