from linkage.taskapp.celery import app
from models import LinkingProject
from utils import project_to_json

from cdilinker.linker.linker import run_project

@app.task(name="run_project")
def run_task(name, project_json):
    try:
        LinkingProject.objects.filter(name=name).update(status='RUNNING')

        project = LinkingProject.objects.get(name=name)

        report_file = run_project(project_json)
        if report_file:
            project.results_file = report_file
            project.status = 'COMPLETED'
            project.comments = ''
            project.save()
    except Exception as e:
        project.status = 'FAILED'
        msg = e.message
        if len(msg) > 100:
            msg = msg[:100] + ' ...'
        project.comments = msg

        project.save()
