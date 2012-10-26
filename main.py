import sys
import logging

from pyvsb.backup import Backuper

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
            print >> stream, self.format(record).encode("utf-8")
        except:
            self.handleError(record)
        finally:
            self.release()


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
    setup(True)
    Backuper().backup()
