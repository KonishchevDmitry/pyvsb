import errno
import logging
import os
import time

import psys

from .backup import Backup
from .core import Error

LOG = logging.getLogger(__name__)


# TODO
class Storage:
    """Represents a backup storage."""

    MODE_BACKUP = "backup"
    """Backup mode."""

    MODE_RESTORE = "restore"
    """Restore mode."""


    def __init__(self, config, mode):
        self.__config = config

        try:
            domains = sorted(
                domain for domain in os.listdir(self.__config["backup_root"])
                    if not domain.startswith("."))
        except EnvironmentError as e:
            raise Error("Error while reading backup directory '{}': {}.",
                self.__config["backup_root"], psys.e(e))

        if domains:
            domain = domains[-1]
        else:
            domain = self.__create_domain()

        domain_path = self.__get_domain_path(domain)
        self.__backup = Backup(domain_path,
            time.strftime("%Y.%m.%d-%H:%M:%S", time.localtime()),
            Backup.MODE_WRITE, config)


    def add_file(self, path, stat_info, link_target = "", file_obj = None):
        """Adds a file to the storage."""

        # TODO: exceptions
        LOG.debug("Storing file '%s'...", path)
        self.__backup.add_file(path, stat_info, link_target, file_obj)


    # TODO:
    def close(self):
        self.__backup.close()


    def commit(self):
        self.__backup.commit()


    def __create_domain(self):
        """Creates a new backup domain."""

        domain = time.strftime("%Y.%m.%d", time.localtime())
        domain_path = self.__get_domain_path(domain)

        try:
            os.mkdir(domain_path)
        except EnvironmentError as e:
            if e.errno != errno.EEXIST:
                raise Error("Unable to create a new backup domain directory '{}': {}.",
                    domain_path, psys.e(e))

        return domain


    def __get_domain_path(self, domain):
        """Returns path to the specified domain."""

        return os.path.join(self.__config["backup_root"], domain)
