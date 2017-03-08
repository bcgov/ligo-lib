import json
from django.forms.models import model_to_dict
from django.conf import settings
from .models import LinkingProject, LinkingDataset

def project_to_json(name):

    project = LinkingProject.objects.get(name=name)
    project_json = model_to_dict(project)

    project_json['steps'] = [model_to_dict(step) for step in project.steps.all()]
    project_json['datasets'] = [model_to_dict(dataset) for dataset in project.datasets.all()]
    datasets = project_json['datasets']
    left_link = LinkingDataset.objects.get(link_project=project, link_seq=1)
    try:
        left_columns = json.loads(left_link.columns) or []
    except:
        left_columns = []

    if len(datasets) > 0:
        datasets[0]['columns'] = left_columns
        data_types = datasets[0]['data_types']
        if data_types:
            selected_types = {}
            for (key, value) in data_types.iteritems():
                if key in left_columns:
                    selected_types[key] = value
            datasets[0]['data_types'] = selected_types

    if len(datasets) > 1 and project.type == 'LINK':
        right_link = LinkingDataset.objects.get(link_project=project, link_seq=2)
        try:
            right_columns = json.loads(right_link.columns) or []
        except:
            right_columns = []

        datasets[1]['columns'] = right_columns
        data_types = datasets[1]['data_types']
        if data_types:
            selected_types = {}
            for (key, value) in data_types.iteritems():
                if key in right_columns:
                    selected_types[key] = value
            datasets[1]['data_types'] = selected_types

    for dataset in project_json.get('datasets', []):
        dataset['url'] = settings.DATASTORE_URL + dataset['url']
        del dataset['id']

    for step in project_json['steps']:
        del step['id']
        del step['linking_project']

    project_json['output_root'] = settings.OUTPUT_URL

    del project_json['id']
    if project.type == 'DEDUP':
        del project_json['relationship_type']

    return project_json

