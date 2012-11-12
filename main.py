# TODO
from __future__ import print_function

import argparse
import os
import sys
import logging

from pyvsb.backuper import Backuper
from pyvsb.config import get_config
from pyvsb.core import Error
from pyvsb.restorer import Restorer

class OutputHandler(logging.Handler):
    """
    Log handler that logs debug and info messages to stdout and all other
    messages to stderr.
    """

    def __init__(self, *args, **kwargs):
        logging.Handler.__init__(self, *args, **kwargs)


    def emit(self, record):
        self.acquire()

        try:
            stream = sys.stdout if record.levelno <= logging.INFO else sys.stderr
            print(self.format(record), file = stream)
        except:
            self.handleError(record)
        finally:
            self.release()


def main():
    """The script's main function."""

    parser = argparse.ArgumentParser(
        description = "pyvsb - A very simple in configuring but powerful backup tool")

    parser.add_argument("-c", "--config", metavar = "CONFIG_PATH", type = str,
        default = os.path.expanduser("~/.pyvsb.conf"),
        help = "configuration file path (default is ~/.pyvsb.conf)")

    parser.add_argument("-r", "--restore", metavar = "BACKUP_PATH",
        default = None, help = "restore the specified backup")

    args = parser.parse_args()

    try:
        # TODO
        setup(True)
        # TODO
        success = False

        if args.restore:
            try:
                with Restorer(args.restore) as restorer:
                    success = restorer.restore()
            except Exception as e:
                raise Error("Backup failed: {}", e)
        else:
            try:
                config = get_config(args.config)
            except Exception as e:
                raise Error("Error while reading configuration file {}: {}",
                    args.config, e)

            try:
                with Backuper(config) as backuper:
                    backuper.backup()
            except Exception as e:
                raise Error("Backup failed: {}", e)
    except Exception as e:
        # TODO
        fsdfs
        sys.exit(str(e))
    else:
        sys.exit(int(not success))


def setup(debug_mode = False, filter = None, max_log_name_length = 16, level = None):
    """Sets up the logging."""

    logging.addLevelName(logging.DEBUG,   "D")
    logging.addLevelName(logging.INFO,    "I")
    logging.addLevelName(logging.WARNING, "W")
    logging.addLevelName(logging.ERROR,   "E")

    log = logging.getLogger()

    log.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    if level is not None:
        log.setLevel(level)

    format = ""
    if debug_mode:
        format += "%(asctime)s.%(msecs)03d (%(filename)12.12s:%(lineno)04d) [%(name){0}.{0}s]: ".format(max_log_name_length)
    format += "%(levelname)s: %(message)s"

    handler = OutputHandler()
    handler.setFormatter(logging.Formatter(format, "%Y.%m.%d %H:%M:%S"))
    if filter is not None:
        handler.addFilter(filter)

    log.addHandler(handler)

if __name__ == "__main__":
    main()
