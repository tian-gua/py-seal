class UnsupportedException(Exception):

    def __init__(self, code=1, message='unsupported operation'):
        self.code = code
        self.message = message

    def __str__(self):
        return f"{self.code} - {self.message}"
