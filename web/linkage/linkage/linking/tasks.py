from linkage.taskapp.celery import app
from linkage.linking.models import LinkingProject

from linkage.linking.utils import project_to_json
from  cdilinker.linker.commands import execute_project
import logging


@app.task(name="run_project")
def run_task(name, project_json):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    try:
        project = None
        project = LinkingProject.objects.get(name=name)

        logger.debug("name : {0}".format(name))
        logger.debug("project_json : {0}".format(project_json))
        LinkingProject.objects.filter(name=name).update(status='RUNNING')

        logger.debug("project : {0}".format(project))

        report_file = execute_project(project_json)
        logger.debug("report_file : {0}".format(report_file))

        if report_file:
            project.results_file = report_file
            project.status = 'COMPLETED'
            project.comments = ''
            project.save()
    except Exception as e:
        try:
            msg = e.message
        except AttributeError as ae:
            msg = 'An error occured during project execution. Please check the logs for details'

        logger.debug(e)
        if len(msg) > 100:
            msg = msg[:100] + ' ...'
        if project is not None:
            project.status = 'FAILED'
            project.comments = msg

            project.save()
