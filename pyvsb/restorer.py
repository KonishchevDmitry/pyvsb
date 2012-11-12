# TODO
"""Controls restore process."""

import errno
import logging
import os
import stat

import psys
from psys import eintr_retry

import psh
system = psh.Program("sh", "-c", _defer = False)

from .core import LogicalError
from .storage import Storage

LOG = logging.getLogger(__name__)


class Restorer:
    """Controls restore process."""

    def __init__(self, backup_path):
        # TODO: links here and everywhere (rmtree)

        backup_path = os.path.abspath(backup_path)
        backup_group_path = os.path.dirname(backup_path)

        config = {
            "backup":       os.path.basename(backup_path),
            "backup_group": os.path.basename(backup_group_path),
            "backup_root":  os.path.dirname(backup_group_path),
            "restore_path": "restore",
        }

        # Backup storage abstraction
        self.__storage = Storage(config, Storage.MODE_RESTORE)


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


    # TODO
    def restore(self):
        self.__storage.restore()


    def close(self):
        """Closes the object."""

        self.__storage.close()
