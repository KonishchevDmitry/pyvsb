"""Tests pyvsb's backup and restore process."""

# TODO
from pyvsb.main import setup_logging
setup_logging(debug_mode = True)

# TODO
import hashlib
import os
import pprint
import shutil
import signal
import socket
import stat
import tempfile
import time

import pytest

import psh
import psys

from psh import sh

from pyvsb.backup import Restore
from pyvsb.backuper import Backuper


def pytest_funcarg__env(request):
    env = {
        # TODO
        "data_template_path": os.path.join(os.getcwd(), "root")
    }

    def finalize():
        if "test_path" in env:
            shutil.rmtree(env["test_path"])
    request.addfinalizer(finalize)

    env["test_path"] = tempfile.mkdtemp()
    env["data_path"] = os.path.join(env["test_path"], "data")
    env["backup_path"] = os.path.join(env["test_path"], "backup")
    env["restore_path"] = os.path.join(env["test_path"], "restore")

    if True:
        shutil.copytree(env["data_template_path"],
            env["data_path"], symlinks = True)
    else:
        os.mkdir(env["data_path"])

    os.mkdir(env["backup_path"])

    # TODO
    env["config"] = {
        "backup_root":         env["backup_path"],
        "max_backups":         1,
        "max_backup_groups":   1,
        "preserve_hard_links": True,
        "trust_modify_time":   False,
        "backup_items":        { env["data_path"]: {} }
    }

    return env


def test_simple(env):
    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    with Restore(_get_backups(env)[-1], env["restore_path"]) as restorer:
        assert restorer.restore()

    assert _get_tree(env["data_path"]) == _get_tree(env["restore_path"] + env["data_path"])


def test_double(env):
    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    with Restore(_get_backups(env)[-1], env["restore_path"]) as restorer:
        assert restorer.restore()

    assert _get_tree(env["data_path"]) == _get_tree(env["restore_path"] + env["data_path"])

    shutil.rmtree(env["restore_path"])


    time.sleep(1)

    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    with Restore(_get_backups(env)[-1], env["restore_path"]) as restorer:
        assert restorer.restore()

    assert _get_tree(env["data_path"]) == _get_tree(env["restore_path"] + env["data_path"])


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


# TODO
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

        with open(path, "rb") as hashing_file:
            file_hash = hashlib.md5()

            while True:
                data = hashing_file.read(4096)
                if not data:
                    break

                file_hash.update(data)

            tree["md5"] = file_hash.hexdigest()
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



# TODO
def _get_backups(env):
    backup_group_path = os.path.join(
        env["backup_path"], sorted(os.listdir(env["backup_path"]))[-1])

    return [
        os.path.join(backup_group_path, backup_name)
        for backup_name in sorted(os.listdir(backup_group_path)) ]
