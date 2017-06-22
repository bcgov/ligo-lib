class LinkError(Exception):
    """
        Base class for exceptions in linking module.
    """
    NO_PROJECT = 'NO_PROJECT'
    INVALID_TYPE = 'INVALID_TYPE'
    TYPE_MISSING = 'TYPE_MISSING'
    OUT_PATH_MISSING = 'OUT_PATH_MISSING'
    NO_STEPS = 'NO_STEPS'
    NAME_MISSING = 'NAME_MISSING'
    DATASET_MISSING = 'DATASET_MISSING'
    INDEX_MISSING = 'INDEX_MISSING'
    FILEPATH_MISSING = 'FILEPATH_MISSING'
    NO_BLOCKING = 'NO_BLOCKING'
    NO_LINKING = 'NO_LINKING'
    ENTITY_ID_MISSING = 'ENTITY_ID_MISSING'
    INVALID_PATH = 'INVALID_PATH'
    INVALID_INDEX = 'INVALID_INDEX'
    INVALID_ENTITY_FIELD = 'INVALID_ENTITY_FIELD'
    DATASET_MISSING = 'DATASET_MISSING'

    ERROR_MESSAGES = {
        NO_PROJECT: 'No project is provided. Project cannot be empty.',
        INVALID_TYPE: 'Invalid project type. Project type should be either LINK or DEDUP.',
        TYPE_MISSING: 'Project type is missing.',
        OUT_PATH_MISSING: 'Path to project result storage is missing.',
        NO_STEPS: 'Project step(s) missing. At least one project step must be provided.',
        NAME_MISSING: 'Project name is required',
        DATASET_MISSING: 'Input dataset(s) missing.',
        INDEX_MISSING: 'Dataset index field is missing.',
        FILEPATH_MISSING: 'Path to dataset file is required.',
        NO_BLOCKING: 'Step blocking variable(s) not provided.',
        NO_LINKING: 'Step linking variable(s) not provided',
        ENTITY_ID_MISSING: 'Dataset entity id field is missing.',
        INVALID_PATH: 'Invalid file path. Dataset file does not exist.',
        INVALID_INDEX: 'Invalid Index Field. Index field does not exist in dataset.',
        INVALID_ENTITY_FIELD: 'Invalid Entity ID Field. Entity ID does not exist in dataset.',
        DATASET_MISSING: 'Project dataset is missing.'
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
