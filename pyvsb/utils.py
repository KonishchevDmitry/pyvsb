"""Various utils."""

import bz2
import errno
import grp
import gzip
import logging
import pwd
import shutil
import tarfile
import tempfile

from hashlib import sha256

import psys

LOG = logging.getLogger(__name__)


_DB_ENTRIES_CACHE = {}
"""A DB entries cache."""


class CompressedTarFile:
    """A wrapper for a compressed tar file."""

    __formats = {
        "bz2": {
            "extension":    ".bz2",
            "mode":         ":bz2",
            "decompressor": bz2.BZ2File,
        },
        "gz": {
            "extension":    ".gz",
            "mode":         ":gz",
            "decompressor": gzip.GzipFile,
        },
        "none": {
            "extension": "",
            "mode":      "",
        },
    }
    """Available file formats."""


    __file = None
    """Opened tar file."""

    __temp_file = None
    """A temporary file."""


    def __init__(self, path, write = None, decompress = True):
        try:
            if write is None:
                for file_format in self.__formats.values():
                    cur_path = path + file_format["extension"]

                    try:
                        if decompress and "decompressor" in file_format:
                            with file_format["decompressor"](cur_path) as compressed_file:
                                self.__decompress(cur_path, compressed_file)

                        if self.__file is None:
                            self.__file = tarfile.open(cur_path, "r" + file_format["mode"])
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
                    path + file_format["extension"], "w" + file_format["mode"],
                    format = tarfile.PAX_FORMAT)
        except:
            self.close()
            raise


    def __getattr__(self, attr):
        return getattr(self.__file, attr)


    def __iter__(self):
        return iter(self.__file)


    def close(self):
        """Closes the file."""

        try:
            if self.__file is not None:
                self.__file.close()
        finally:
            if self.__temp_file is not None:
                self.__temp_file.close()


    def __decompress(self, path, compressed_file):
        """Decompresses a compressed tar archive."""

        LOG.debug("Decompressing '%s'...", path)

        try:
            self.__temp_file = tempfile.NamedTemporaryFile(dir = "/var/tmp")
            shutil.copyfileobj(compressed_file, self.__temp_file)
            self.__temp_file.flush()
        except BaseException as e:
            if self.__temp_file is not None:
                try:
                    self.__temp_file.close()
                except Exception as e:
                    LOG.error("Failed to delete a temporary file '%s': %s.",
                        self.__temp_file.name, psys.e(e))
                finally:
                    self.__temp_file = None

            if not isinstance(e, Exception):
                raise

            LOG.error("Failed to decompress '%s': %s.", path, psys.e(e))
        else:
            LOG.debug("Decompressing finished.")
            self.__file = tarfile.open(self.__temp_file.name)



class HashableFile():
    """A wrapper for a file object that hashes all read data."""

    def __init__(self, file):
        self.__file = file
        self.__hash = sha256()


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
        self.__hash = sha256()



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
