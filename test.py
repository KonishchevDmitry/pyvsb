try:
    import pycl.log
except ImportError:
    pass
else:
    pycl.log.setup(debug_mode = True)

import os
import pprint
import shutil
import signal
import socket
import stat
import time

import pytest

import psh
import psys

from psh import sh

from pyvsb.backup import Restore
from pyvsb.backuper import Backuper

BACKUP_ROOT = os.path.join(os.getcwd(), "test_backup_root")
"""Test backup root path."""

_FAKE_ROOT_TEMPLATE = os.path.join(os.getcwd(), "root")

FAKE_ROOT = os.path.join(os.getcwd(), "test_fake_root")

RESTORE_PATH = "test_restore"


def get_config():
    return {
        "backup_root": BACKUP_ROOT,
        "max_backups": 1,
        "max_backup_groups": 1,
        "preserve_hard_links": True,
        "trust_modify_time": False,
    }



def pytest_funcarg__test(request):
    if os.path.exists(FAKE_ROOT):
        shutil.rmtree(FAKE_ROOT)

    shutil.copytree(_FAKE_ROOT_TEMPLATE, FAKE_ROOT, symlinks = True)
    #os.mkdir(FAKE_ROOT)


    if os.path.exists(BACKUP_ROOT):
        shutil.rmtree(BACKUP_ROOT)

    os.mkdir(BACKUP_ROOT)

    def finalize():
        try:
            shutil.rmtree(BACKUP_ROOT)
        finally:
            try:
                shutil.rmtree(FAKE_ROOT)
            finally:
                if os.path.exists(RESTORE_PATH):
                    shutil.rmtree(RESTORE_PATH)

    request.addfinalizer(finalize)


def test_simple(test):
    config = get_config()
    config["backup_items"] = { FAKE_ROOT: {} }

    with Backuper(config) as backuper:
        assert backuper.backup()

    with Restore(_get_backup_path(), RESTORE_PATH) as restorer:
        assert restorer.restore()

    assert _get_tree(FAKE_ROOT) == _get_tree(RESTORE_PATH + FAKE_ROOT)


#def test_on_execute_with_exeption(unittest):
#    config = get_config()
#    config["backup_items"] = {
#        FAKE_ROOT: {}
#    }
#
#    os.mkfifo(os.path.join(FAKE_ROOT, "fifo"))
#
#    with open(os.path.join(FAKE_ROOT, "symlink-target"), "w"):
#        os.symlink("symlink-target", os.path.join(FAKE_ROOT, "symlink"))
#
#    target_directory = os.path.join(FAKE_ROOT, "symlink-target-directory")
#    os.mkdir(target_directory)
#    with open(os.path.join(target_directory, "file-to-omit"), "w"):
#        pass
#    os.symlink(target_directory, os.path.join(FAKE_ROOT, "directory-symlink"))
#
#    os.symlink("broken-symlink-target", os.path.join(FAKE_ROOT, "broken-symlink"))
#
#    with open(os.path.join(FAKE_ROOT, "hardlink-target"), "w"):
#        os.link(os.path.join(FAKE_ROOT, "hardlink-target"),
#            os.path.join(FAKE_ROOT, "hardlink"))
#
#    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
#    unix_socket.bind(os.path.join(FAKE_ROOT, "socket"))
#
#
#    with Backuper(config) as backuper:
#        assert backuper.backup()
#
#    with Restore(_get_backup_path(), RESTORE_PATH) as restorer:
#        assert restorer.restore()
#
#    pprint.pprint(_get_tree(FAKE_ROOT, ignore = [ "socket" ]))
##    time.sleep(60)
##    fs
#    assert _get_tree(FAKE_ROOT, ignore = [ "socket" ]) == _get_tree(RESTORE_PATH + FAKE_ROOT)
#
#
#    shutil.rmtree(RESTORE_PATH)
#
#    time.sleep(1)
#
#    with Backuper(config) as backuper:
#        assert backuper.backup()
#
#    with Restore(_get_backup_path(), RESTORE_PATH) as restorer:
#        assert restorer.restore()


def _get_tree(path, root = True, ignore = []):
    stat_info = os.lstat(path)

    tree = {
        "name":  os.path.basename(path),
        "mode":  stat_info.st_mode,
        "uid": stat_info.st_uid,
        "gid": stat_info.st_gid,
        "links": stat_info.st_nlink,
    }

    if stat.S_ISLNK(stat_info.st_mode):
        tree["target"] = os.readlink(path)
    elif stat.S_ISREG(stat_info.st_mode):
        tree["size"] = stat_info.st_size
        tree["mtime"] = int(stat_info.st_mtime)
    elif stat.S_ISDIR(stat_info.st_mode):
        tree["files"] = {
            name: _get_tree(os.path.join(path, name), False)
                for name in os.listdir(path) }

        for path in ignore:
            path_tree = tree
            names = path.split(os.path.sep)

            for id, name in enumerate(names):
                files = path_tree.get("files", {})

                if id == len(names) - 1:
                    if name in files:
                        del files[name]
                else:
                    path_tree = files.get(name)
                    if path_tree is None:
                        break

    return tree["files"] if root else tree



def _get_backup_path():
    backup_path = BACKUP_ROOT
    backup_path = os.path.join(backup_path, sorted(os.listdir(backup_path))[-1])
    return os.path.join(backup_path, sorted(os.listdir(backup_path))[-1])


