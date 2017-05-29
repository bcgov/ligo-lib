from __future__ import print_function

import pandas as pd
from cdilinker.linker.base import CHUNK_SIZE
from cdilinker.linker.validation import LinkError, ValidationError

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_dataset_size(file_path):
    return sum(1 for row in open(file_path))


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
    return errors


def validate(project):
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
        raise ValidationError(errors)


def execute_project(project):
    validate(project)
    left_size = get_dataset_size(project['datasets'][0]['url'])
    if project['type'] == 'DEDUP':
        right_size = 0
    else:
        right_size = get_dataset_size(project['datasets'][1]['url'])

    if left_size > CHUNK_SIZE or right_size > CHUNK_SIZE:
        import cdilinker.linker.chunked_dedup as dedup
        import cdilinker.linker.chunked_link as link
    else:
        import cdilinker.linker.dedup as dedup
        import cdilinker.linker.link as link

    if project['type'] == 'DEDUP':
        task = dedup.DeDeupProject(project)
    else:
        task = link.Linker(project)

    task.load_data()
    task.run()
    return task.save()
