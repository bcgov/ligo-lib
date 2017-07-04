import yaml
import argparse, os
from jinja2 import Environment, FileSystemLoader, Template
import os.path

#Call syntax
#C:\Users\Suraiya\khalegh_linking_latest_2\data-linking>python transform.py --projectroot C:\Users\Suraiya\khalegh_linking_latest_2\data-linking --input "C:\Users\Suraiya\khalegh_linking_latest_2\data-linking\web\linkage\config\openshift\sampleinput\web\deployment.sample.input"   --output django_deployment.yml --template deployment.yaml.tpl --projectroot "C:\Users\Suraiya\khalegh_linking_latest_2\data-linking\web\linkage\config\openshift\template\web"

INPUT_FILE_EXTENSION  = '.input'
OUTPUT_FILE_EXTENSION  = ['.yml', '.yaml']
TEMPLATE_FILE_EXTENSION  = '.tpl'

#as a future improvement step we could remove the project root and pass
# that as part of input
parser = argparse.ArgumentParser(description='Stashes input values to configuration template and produces actual configuration file')
parser.add_argument("-i",'--input', help = 'File with input configuration values', required = True )
parser.add_argument("-t",'--template', help = 'Configuration template', required = True)
parser.add_argument("-o",'--output', help = 'Output configuration', required = True)
parser.add_argument("-r",'--projectroot', help = 'absolute dir part to template file', required = True)
args = vars(parser.parse_args())

def extension(filename):
    file_extension = os.path.splitext(filename)[-1]
    print(file_extension)
    return file_extension

project_root = args['projectroot']
input_file = args['input']
template_file = args['template']
output_file = args['output']

if ((extension(input_file) != INPUT_FILE_EXTENSION) or
    (extension(output_file) not in OUTPUT_FILE_EXTENSION) or
    (extension(template_file) != TEMPLATE_FILE_EXTENSION)
    ):
    print("Either input, output or template files do not have appropriate extension; exiting")
    exit(0)

#absolute dir part to template file
print(project_root)
print(input_file)
print(template_file)
print(output_file)

ENV = Environment(loader=FileSystemLoader(project_root))
print ENV
customconfig = None

#enforce rule so that we take only file with extension .input
#with open("customconfig.input") as file1:

with open(input_file) as file1:
    #print(file1.read())
    try:
        customconfig =  yaml.load(file1)
        print customconfig
    except yaml.YAMLError as exc:
        print(exc)

    print(customconfig)
    print(type(customconfig))

#with open('C:\\Users\\Suraiya\\khalegh_linking_latest_2\\data-linking\\customconfig.j2') as tmpl:
#    print(tmpl.read())

#template = ENV.get_template('customconfig.tpl')
template = ENV.get_template(template_file)

#print(template)
#print (template.render())
print (template.render(customconfig=customconfig))
#generate file with extension yml
#with open("outputconfig.yml", "w") as file2:
with open(output_file, "w") as file2:
    file2.write(template.render(customconfig=customconfig))


#do not commit .input; .env, .yml, .yaml
