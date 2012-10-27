# TODO

class Error(Exception):
    """A base class for all exceptions the module throws."""

    def __init__(self, error, *args, **kwargs):
        super(Error, self).__init__(
            error.format(*args, **kwargs) if args or kwargs else error)

class LogicalError(Exception):
    def __init__(self):
        super().__init__("Logical error")
