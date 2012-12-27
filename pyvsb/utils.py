"""Various utils."""

import bz2
import errno
import grp
import gzip
import pwd
import tarfile

from hashlib import sha1

_DB_ENTRIES_CACHE = {}
"""A DB entries cache."""


class CompressedTarFile:
    """A wrapper for a compressed tar file."""

    __formats = {
        "bz2": {
            "extension": ".bz2",
            "mode":      ":bz2",
            "class":     bz2.BZ2File,
        },
        "gz": {
            "extension": ".gz",
            "mode":      ":gz",
            "class":     gzip.GzipFile,
        },
        "none": {
            "extension": "",
            "mode":      "",
        },
    }
    """Available file formats."""


    def __init__(self, path, write = None, decompress = True):
        if write is None:
            for file_format in self.__formats.values():
                try:
#                    if decompress and "class" in file_format:
#                        with file_format["class"](path + file_format["extension"]):
#                            # TODO FIXME
#                            self.__file = tarfile.open()
#                    else:
                    self.__file = tarfile.open(
                        path + file_format["extension"], "r" + file_format["mode"])
                except EnvironmentError as error:
                    if error.errno != errno.ENOENT:
                        raise
                else:
                    break
            else:
                raise error
        else:
            file_format = self.__formats[write]

            self.__file = tarfile.open(
                path + file_format["extension"], "w" + file_format["mode"])


    def __getattr__(self, attr):
        return getattr(self.__file, attr)


    def __iter__(self):
        return iter(self.__file)


    def close(self):
        """Closes the file."""

        self.__file.close()



class HashableFile():
    """A wrapper for a file object that hashes all read data."""

    def __init__(self, file):
        self.__file = file
        self.__hash = sha1()


    def hexdigest(self):
        """Returns read data hash."""

        return self.__hash.hexdigest()


    def read(self, *args, **kwargs):
        """Reads data from the file and hashes the returning value."""

        data = self.__file.read(*args, **kwargs)
        self.__hash.update(data)
        return data


    def reset(self):
        """Resets the file position."""

        self.__file.seek(0)
        self.__hash = sha1()



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
