import pandas as pd
import logging

from cdilinker.config.config import config
from cdilinker.linker.linker_factory import LinkerFactory
from cdilinker.linker.validation import LinkError, ValidationError

logger = logging.getLogger(__name__)


def get_fields(file_path):
    df = pd.read_csv(file_path, nrows=1)

    return list(df)


def validate_dataset(dataset, errors, type='DEDUP'):
    # Validate path to dataset file
    fields = []
    if 'url' not in dataset:
        errors.append(LinkError.FILEPATH_MISSING)

    try:
        fields = get_fields(dataset['url'])
    except IOError as err:
        errors.append(LinkError.INVALID_PATH)
        raise ValidationError(errors)

    if 'index_field' not in dataset:
        errors.append(LinkError.INDEX_MISSING)
    else:
        index_field = dataset['index_field']
        if index_field not in fields:
            errors.append(LinkError.INVALID_INDEX)

    if type == 'LINK' and 'entity_field' not in dataset:
        errors.append(LinkError.ENTITY_ID_MISSING)
    else:
        entity_field = dataset['entity_field']
        if entity_field not in fields:
            errors.append(LinkError.INVALID_ENTITY_FIELD)
    return errors


def validate_step(step, errors):
    if 'blocking_schema' not in step:
        errors.append(LinkError.NO_BLOCKING)

    if 'linking_schema' not in step:
        errors.append(LinkError.NO_LINKING)

    if 'linking_method' not in step:
        errors.append(LinkError.NO_LINKING_METHOD)
    return errors


def validate(project):
    logger.info('Validating project')
    errors = []
    if project is None:
        errors.append(LinkError.NO_PROJECT)
        raise ValidationError(errors)

    if 'name' not in project:
        errors.append(LinkError.NAME_MISSING)

    # Validate project type.
    if 'type' not in project:
        errors.append(LinkError.TYPE_MISSING)
    elif project['type'] not in ['DEDUP', 'LINK']:
        errors.append(LinkError.INVALID_TYPE)

    # Validate project output directory
    if 'output_root' not in project:
        errors.append(LinkError.OUT_PATH_MISSING)

    if 'steps' not in project or type(project['steps']) is not list:
        errors.append(LinkError.NO_STEPS)

    # Check project datasets
    if 'datasets' not in project or type(project['datasets']) is not list:
        errors.append(LinkError.NO_DATASETS)
    elif len(project['datasets']) == 0:
        errors.append(LinkError.DATASET_MISSING)
    else:
        validate_dataset(project['datasets'][0], errors)
        if project['type'] == 'LINK':
            if len(project['datasets']) != 2:
                errors.append(LinkError.DATASET_MISSING)
            else:
                validate_dataset(project['datasets'][1], errors)

    for step in project['steps']:
        validate_step(step, errors)

    # Check errors and raise Validation error if the errors list is not empty.
    if len(errors) > 0:
        logger.error('Project validation failed.')
        raise ValidationError(errors)


def execute_project(project):
    """
    Executes a linking/De-duplication project and generates the summary report.
    :param project: The project json object
    :return: Path to the summary report.
    """

    """
    Create a unique project identifier before running the project.
    This unique identifier will be used to uniquely identify the project specific recources
    such as linking input, output and temporary files.
    """

    import uuid
    import os

    validate(project)

    logger.info('Assigning uuid to project.')
    if 'task_uuid' not in project:
        project['task_uuid'] = uuid.uuid4().hex
    logger.info('Project uuid %s', project['task_uuid'])

    project['output_root'] += project['task_uuid'] + '/'
    os.makedirs(project['output_root'])

    project['temp_path'] += project['task_uuid'] + '/'
    os.makedirs(project['temp_path'])

    task = LinkerFactory.create_linker(project)

    task.load_data()
    task.run()
    return task.save()
