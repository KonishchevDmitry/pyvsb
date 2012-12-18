"""Tests backup and restore process."""

#from pyvsb.main import setup_logging
#setup_logging(debug_mode = True)

import hashlib
import os
import re
import shutil
import socket
import stat
import tempfile
import time

import pytest

import pyvsb.storage
from pyvsb.backup import Restore
from pyvsb.backuper import Backuper

# Tweak backup group name to be able to create a few backup groups in one
# minute.
pyvsb.storage._GROUP_NAME_FORMAT = pyvsb.storage._BACKUP_NAME_FORMAT
pyvsb.storage._GROUP_NAME_RE = pyvsb.storage._BACKUP_NAME_RE


def pytest_funcarg__env(request):
    env = {}
    data_template_path = os.path.join(os.getcwd(), "tests/root")

    def finalize():
        if "test_path" in env:
            shutil.rmtree(env["test_path"])
    request.addfinalizer(finalize)

    env["test_path"] = tempfile.mkdtemp()
    env["data_path"] = os.path.join(env["test_path"], "data")
    env["backup_path"] = os.path.join(env["test_path"], "backup")
    env["restore_path"] = os.path.join(env["test_path"], "restore")

    shutil.copytree(data_template_path, env["data_path"], symlinks = True)
    os.mkdir(env["backup_path"])

    env["config"] = {
        "backup_root":         env["backup_path"],
        "max_backups":         1,
        "max_backup_groups":   1000,
        "preserve_hard_links": True,
        "trust_modify_time":   True,
        "backup_items":        { env["data_path"]: {} }
    }

    return env


def test_simple(env):
    source_tree = _hash_tree(env["data_path"])

    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    with Restore(_get_backups(env)[-1], env["restore_path"]) as restorer:
        assert restorer.restore()

    assert _hash_tree(env["data_path"]) == source_tree
    assert _hash_tree(env["restore_path"] + env["data_path"]) == source_tree


@pytest.mark.parametrize("max_backups", ( 1, 10 ))
def test_double(env, max_backups):
    source_tree = _hash_tree(env["data_path"])


    env["config"]["max_backups"] = max_backups

    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    with Restore(_get_backups(env)[-1], env["restore_path"]) as restorer:
        assert restorer.restore()

    assert _hash_tree(env["data_path"]) == source_tree
    assert _hash_tree(env["restore_path"] + env["data_path"]) == source_tree

    shutil.rmtree(env["restore_path"])


    time.sleep(1)

    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    with Restore(_get_backups(env)[-1], env["restore_path"]) as restorer:
        assert restorer.restore()

    assert _hash_tree(env["data_path"]) == source_tree
    assert _hash_tree(env["restore_path"] + env["data_path"]) == source_tree


@pytest.mark.parametrize(( "max_groups", "max_backups" ), (
    ( 1, 1 ), ( 2, 1 ), ( 2, 3 ),
))
def test_groups(env, max_groups, max_backups):
    source_tree = _hash_tree(env["data_path"])

    env["config"]["max_backup_groups"] = max_groups
    env["config"]["max_backups"] = max_backups

    for num in range(1, 8):
        if num != 1:
            time.sleep(1)

        with Backuper(env["config"]) as backuper:
            assert backuper.backup()

        groups = _get_groups(env)
        assert len(groups) == min(max_groups, ( num if max_backups == 1 else num // max_backups + min(1, num % max_backups) ) )
        for id, group in enumerate(sorted(groups, reverse = True)):
            assert len(_get_backups(env, group)) == ( max_backups if id else ( 1 if max_backups == 1 else num % max_backups or max_backups ) )

    assert _hash_tree(env["data_path"]) == source_tree


@pytest.mark.parametrize("config", [
    {
        "max_backups":         max_backups,
        "preserve_hard_links": preserve_hard_links,
        "trust_modify_time":   trust_modify_time,
    }
    for max_backups in ( 1, 2, 3, 10 )
        for trust_modify_time in ( True, False )
            for preserve_hard_links in ( True, False )
])
def test_complex(env, config):
    source_trees = []
    env["config"].update(config)


    env["config"]["backup_items"] = {
        env["data_path"] + "/etc": {},
        env["data_path"] + "/home": {
            "before": "echo SCRIPT_TEST > " + env["data_path"] + "/home/script_test",
            "after": "rm " + env["data_path"] + "/home/script_test",
        },
    }

    with open(env["data_path"] + "/etc/changing_file", "w") as changing_file:
        changing_file.write("first revision")

    source_tree = _hash_tree(env["data_path"])
    source_tree["files"] = { name: tree
        for name, tree in source_tree["files"].items()
            if name in ( "etc", "home" )
    }
    del source_tree["files"]["home"]["mtime"]

    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    source_trees.append(( source_tree, _get_backups(env)[-1] ))


    time.sleep(1)

    env["config"]["backup_items"] = {
        env["data_path"] + "/etc": {},
        env["data_path"] + "/non-existing": {},
    }

    with open(env["data_path"] + "/etc/changing_file", "w") as changing_file:
        changing_file.write("second revision")

    source_tree = _hash_tree(env["data_path"])
    source_tree["files"] = { name: tree
        for name, tree in source_tree["files"].items()
            if name == "etc"
    }

    with Backuper(env["config"]) as backuper:
        assert not backuper.backup()

    source_trees.append(( source_tree, _get_backups(env)[-1] ))


    time.sleep(1)

    env["config"]["backup_items"] = {
        env["data_path"] + "/etc": {
            "filter": [ ( False, re.compile("^bash_completion.d") ) ],
        },
        env["data_path"] + "/tmp": {},
    }

    with open(env["data_path"] + "/etc/changing_file", "w") as changing_file:
        changing_file.write("third revision")

    source_tree = _hash_tree(env["data_path"])
    source_tree["files"] = { name: tree
        for name, tree in source_tree["files"].items()
            if name in ( "etc", "tmp" )
    }
    del source_tree["files"]["etc"]["files"]["bash_completion.d"]

    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    source_trees.append(( source_tree, _get_backups(env)[-1] ))


    time.sleep(1)

    tmp_path = os.path.join(env["data_path"], "tmp")

    os.mkfifo(os.path.join(tmp_path, "fifo"))

    with open(os.path.join(tmp_path, "symlink-target"), "w"):
        os.symlink("symlink-target", os.path.join(tmp_path, "symlink"))

    target_directory = os.path.join(tmp_path, "directory-symlink-target")
    os.mkdir(target_directory)
    with open(os.path.join(target_directory, "file"), "w"):
        pass
    os.symlink(target_directory, os.path.join(tmp_path, "directory-symlink"))

    os.symlink("broken-symlink-target", os.path.join(tmp_path, "broken-symlink"))

    with open(os.path.join(tmp_path, "hardlink-target"), "w"):
        os.link(os.path.join(tmp_path, "hardlink-target"),
            os.path.join(tmp_path, "hardlink"))

    unix_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unix_socket.bind(os.path.join(tmp_path, "socket"))

    source_tree = _hash_tree(env["data_path"])
    source_tree["files"] = { name: tree
        for name, tree in source_tree["files"].items()
            if name in ( "etc", "home", "tmp" )
    }
    del source_tree["files"]["tmp"]["files"]["socket"]
    if not env["config"]["preserve_hard_links"]:
        source_tree["files"]["tmp"]["files"]["hardlink"]["links"] = 1
        source_tree["files"]["tmp"]["files"]["hardlink-target"]["links"] = 1

    env["config"]["backup_items"] = {
        env["data_path"] + "/etc": {},
        env["data_path"] + "/home": {},
        env["data_path"] + "/tmp": {},
    }
    with Backuper(env["config"]) as backuper:
        assert backuper.backup()

    source_trees.append(( source_tree, _get_backups(env)[-1] ))


    for backup_id, backup in enumerate(source_trees):
        source_tree, backup_path = backup

        with Restore(backup_path, env["restore_path"]) as restorer:
            assert restorer.restore()

        restore_tree = _hash_tree(env["restore_path"] + env["data_path"])

        if backup_id == 0:
            del restore_tree["files"]["home"]["mtime"]
            del restore_tree["files"]["home"]["files"]["script_test"]

            with open(env["restore_path"] + env["data_path"] + "/home/script_test") as script_test:
                assert script_test.read() == "SCRIPT_TEST\n"

        assert source_tree == restore_tree
        shutil.rmtree(env["restore_path"])


def _get_backups(env, group = None):
    """Returns backups in the specified backup group (last by default)."""

    backup_group_path = os.path.join(env["backup_path"],
        _get_groups(env)[-1] if group is None else group)

    return [
        os.path.join(backup_group_path, backup_name)
        for backup_name in sorted(os.listdir(backup_group_path)) ]


def _get_groups(env):
    """Returns all backup group names."""

    return sorted(os.listdir(env["backup_path"]))


def _hash_tree(path, root = True):
    """Hashes a directory tree to compare directories for equality."""

    stat_info = os.lstat(path)

    tree = {
        "name": os.path.basename(path),
        "uid":  stat_info.st_uid,
        "gid":  stat_info.st_gid,
        "mode": stat_info.st_mode,
    }

    if not stat.S_ISLNK(stat_info.st_mode):
        tree["mtime"] = int(stat_info.st_mtime)

    if stat.S_ISREG(stat_info.st_mode):
        tree["size"] = stat_info.st_size
        tree["links"] = stat_info.st_nlink

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
            name: _hash_tree(os.path.join(path, name), root = False)
                for name in os.listdir(path) }
    elif stat.S_ISLNK(stat_info.st_mode):
        tree["target"] = os.readlink(path)

    return { "files": tree["files"] } if root else tree