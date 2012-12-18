"""Core tools."""

class Error(Exception):
    """A base class for all exceptions the program raises."""

    def __init__(self, error, *args, **kwargs):
        super(Error, self).__init__(
            error.format(*args, **kwargs) if args or kwargs else error)


class LogicalError(Error):
    """Exception for a logical error."""

    def __init__(self):
        super().__init__("Logical error")
