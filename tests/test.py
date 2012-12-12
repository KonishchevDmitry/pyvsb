# -*- coding: utf-8 -*-

"""Common test utils."""

from __future__ import unicode_literals

import os
import platform
import re
import shutil
import subprocess
import threading

import psys

BACKUP_ROOT = os.path.join(os.getcwd(), "test_backup_root")
"""Test backup root path."""

_FAKE_ROOT_TEMPLATE = os.path.join(os.getcwd(), "root")

FAKE_ROOT = os.path.join(os.getcwd(), "test_fake_root")

RESTORE_PATH = "test_restore"


def check_leaks(request):
    """Test wrapper that checks the module for leaks."""

    if os.path.exists(FAKE_ROOT):
        shutil.rmtree(FAKE_ROOT)

    shutil.copytree(_FAKE_ROOT_TEMPLATE, FAKE_ROOT, symlinks = True)
    #os.mkdir(FAKE_ROOT)


    if os.path.exists(BACKUP_ROOT):
        shutil.rmtree(BACKUP_ROOT)

    os.mkdir(BACKUP_ROOT)

    #def opened_fds():
    #    if platform.system() == "Darwin":
    #        fd_path = "/dev/fd"
    #    else:
    #        fd_path = "/proc/self/fd"

    #    return set( int(fd) for fd in os.listdir(fd_path) )

    #def running_threads():
    #    return set( thread.ident for thread in threading.enumerate() )

    #def process_childs():
    #    process = subprocess.Popen(
    #        [ "ps", "-A", "-o", "ppid=,pid=,command=" ],
    #        stdout = subprocess.PIPE)

    #    stdout = psys.u(process.communicate()[0])
    #    assert not process.returncode
    #    assert stdout

    #    childs = ""

    #    for line in stdout.split("\n"):
    #        match = re.search(r"^\s*{0}\s+(\d+)".format(os.getpid()), line)

    #        if match is not None and int(match.group(1)) != process.pid:
    #            if childs:
    #                childs += "\n"

    #            childs += line

    #    return childs

    #fds = opened_fds()
    #threads = running_threads()
    #childs = process_childs()

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


def get_config():
    return {
        "backup_root": BACKUP_ROOT,
        "max_backups": 1,
        "max_backup_groups": 1,
        "preserve_hard_links": True,
        "trust_modify_time": False,
    }


def init(globals):
    """Initializes the test."""

    globals["pytest_funcarg__unittest"] = check_leaks

    try:
        import pycl.log
    except ImportError:
        pass
    else:
        pycl.log.setup(debug_mode = True)
