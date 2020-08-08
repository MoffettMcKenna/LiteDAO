

class ImaginaryColumnException(BaseException):
    """
    Exception for when the user asks for a column which doesn't exist.
    """

    def __init__(self, table, col):
        self.Table = table
        self.Column = col

    def __str__(self):
        return f'Tried to read non-existent column {self.Column} from {self.Table}.'
