from django.forms.models import model_to_dict
from django.conf import settings
from .models import LinkingProject

def project_to_json(name):

    project = LinkingProject.objects.get(name=name)
    project_json = model_to_dict(project)

    project_json['steps'] = [model_to_dict(step) for step in project.steps.all()]
    project_json['datasets'] = [model_to_dict(dataset) for dataset in project.datasets.all()]
    for dataset in project_json.get('datasets', []):
        dataset['url'] = settings.DATASTORE_URL + dataset['url']
    project_json['output_root'] = settings.OUTPUT_URL

    if project.type == 'DEDUP':
        del project_json['relationship_type']

    return project_json

