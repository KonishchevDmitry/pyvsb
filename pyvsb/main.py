"""pyvsb main script."""

from __future__ import print_function # To suppress code checker errors

import argparse
import os
import sys
import logging

from pyvsb.backup import Restore
from pyvsb.backuper import Backuper
from pyvsb.config import get_config
from pyvsb.core import Error

LOG = logging.getLogger(__name__)


class OutputHandler(logging.Handler):
    """
    A log handler that logs debug and info messages to stdout and all other
    messages to stderr.
    """

    def __init__(self, *args, **kwargs):
        logging.Handler.__init__(self, *args, **kwargs)


    def emit(self, record):
        self.acquire()

        try:
            print(self.format(record),
                file = sys.stderr if record.levelno > logging.INFO else sys.stdout)
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

    parser.add_argument("-d", "--debug", action = "store_true",
        help = "turn on debug messages")

    parser.add_argument("--cron", action = "store_true",
        help = "show only warning and error messages (intended to be used from cron)")

    args = parser.parse_args()


    log_level = logging.WARNING if args.cron else logging.INFO
    setup_logging(args.debug, log_level)

    try:
        if args.restore is None:
            try:
                try:
                    config = get_config(args.config)
                except Exception as e:
                    raise Error("Error while reading configuration file {}: {}",
                        args.config, e)

                with Backuper(config) as backuper:
                    success = backuper.backup()
            except Exception as e:
                raise Error("Backup failed: {}", e)
        else:
            try:
                with Restore(os.path.abspath(args.restore)) as restorer:
                    success = restorer.restore()
            except Exception as e:
                raise Error("Restore failed: {}", e)
    except Exception as e:
        (LOG.exception if args.debug else LOG.error)(e)
        success = False

    sys.exit(int(not success))


def setup_logging(debug_mode = False, level = None, max_log_name_length = 14):
    """Sets up logging."""

    logging.addLevelName(logging.DEBUG,   "D")
    logging.addLevelName(logging.INFO,    "I")
    logging.addLevelName(logging.WARNING, "W")
    logging.addLevelName(logging.ERROR,   "E")

    log = logging.getLogger("pyvsb")

    if debug_mode:
        level = logging.DEBUG
    elif level is None:
        level = logging.INFO

    log.setLevel(level)

    format = "%(levelname)s: %(message)s"
    if debug_mode:
        format = "%(asctime)s.%(msecs)03d (%(filename)11.11s:%(lineno)04d) [%(name){0}.{0}s]: {1}".format(max_log_name_length, format)

    handler = OutputHandler()
    handler.setFormatter(logging.Formatter(format, "%Y.%m.%d %H:%M:%S"))

    log.addHandler(handler)


if __name__ == "__main__":
    main()
