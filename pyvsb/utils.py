"""Various utils."""

import grp
import pwd

_GRP_CACHE = None
"""grp module cache."""

_PWD_CACHE = None
"""pwd module cache."""


def getgrgid(gid):
    """Cached grp.getgrgid()."""

    global _GRP_CACHE

    if _GRP_CACHE is None:
        _GRP_CACHE = { group[2]: group for group in grp.getgrall() }

    return _GRP_CACHE[gid]


def getpwuid(uid):
    """Cached pwd.getpwuid()."""

    global _PWD_CACHE

    if _PWD_CACHE is None:
        _PWD_CACHE = { user[2]: user for user in pwd.getpwall() }

    return _PWD_CACHE[uid]
