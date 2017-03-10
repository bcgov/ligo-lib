from jinja2 import Environment, PackageLoader
from cdilinker.linker.base import LINKING_RELATIONSHIPS, LINKING_METHODS

from xhtml2pdf import pisa


def generate_linking_summary(data, dest_dir='.'):
    print "Genrating summary report..."
    project = data.project
    jenv = Environment(loader=PackageLoader('cdilinker.reports', 'templates'))

    template = jenv.get_template("linking_summary.html")

    datasets = [dataset['name'] for dataset in project['datasets']]
    relationship = None
    if project['type'] == 'LINK':
        for rel in LINKING_RELATIONSHIPS:
            if rel[0] == project['relationship_type']:
                relationship = rel[1]

    steps = []
    for step in data.project['steps']:
        step = {
            "seq": step['seq'],
            "linking_method": LINKING_METHODS.get(step['linking_method'], "Deterministic"),
            "blocking_schema": step['blocking_schema'],
            "linking_schema": step['linking_schema'],
            "total_records_linked": data.steps[step['seq']].get('total_records_linked', None),
            "total_entities": data.steps[step['seq']].get('total_entities', None),
            "total_matched_not_linked": data.steps[step['seq']].get('total_matched_not_linked', None)
        }

        steps.append(step)

    project_types = {
        'LINK': 'Linking',
        'DEDUP': 'De-Duplication'
    }

    template_vars = {
        "name": project['name'],
        "type": project_types[project['type']],
        "datasets": ", ".join(datasets),
        "relationship_type": relationship,
        "total_records_linked": data.total_records_linked,
        "total_entities": data.total_entities,
        "steps": steps
    }

    html_out = template.render(template_vars)

    report_file_name = dest_dir + "/" + project['name'] + "_summary.pdf"

    report_file = open(report_file_name, "w+b")
    pisa.CreatePDF(html_out.encode('utf-8'), dest=report_file, encoding='utf-8')

    return project['name'] + "_summary.pdf"
