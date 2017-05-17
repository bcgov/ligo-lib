class LinkError(Exception):
    """
        Base class for exceptions in linking module.
    """
    NO_PROJECT = 'NO_PROJECT'
    INVALID_TYPE = 'INVALID_TYPE'
    TYPE_MISSING = 'TYPE_MISSING'

    ERROR_MESSAGES = {
        NO_PROJECT: 'No project is provided. Project cannot be empty.',
        INVALID_TYPE: 'Invalid project type. Project type should be either LINK or DEDUP.',
        TYPE_MISSING: 'Project type is missing.'
    }

    @classmethod
    def get_error(cls, code):
        return LinkError.ERROR_MESSAGES.get(code, 'Unknown Error')


class ValidationError(LinkError):
    """
    Exception raised because of validation error in linking or de-duplication project.

    Attributes:
        message -- Details of the validation error.

    """

    def __init__(self, codes):
        self.message = 'Validation Errors:\n' if len(codes) > 1 else 'Validation Error:\n'
        self.codes = codes
        for code in codes:
            self.message += "\t{0}: {1}\n".format(code, LinkError.get_error(code))

    def __str__(self):
        return self.message


