class BusinessException(Exception):

    def __init__(self, code=1, message='业务异常'):
        self.code = code
        self.message = message

    def __str__(self):
        return f"{self.code} - {self.message}"
