import yaml
import argparse, os
from jinja2 import Environment, FileSystemLoader, Template
import os.path

#Call syntax
#C:\Users\Suraiya\khalegh_linking_latest_2\data-linking>python transform.py
# --input "C:\Users\Suraiya\khalegh_linking_latest_2\data-linking\web\linkage\config\openshift\sampleinput\web\deployment.sample.input"
# --output djangodeploymentconfig.yml --template deployment.yaml.tpl 
#--projectroot "C:\Users\Suraiya\khalegh_linking_latest_2\data-linking\web\linkage\config\openshift\template\web"


#To make it difficult for the developer to accidentally commit output configuration
#files or input; in addition to gitignore we enforced a file naming convention. To be
#more specific the transform.py script would not run to completion if the input
#file does not have an extension of .input, and output file is not called .env or
#does not end in .yaml or .yml and the template file needs to have an extension of tpl.
#The user can commit the template file. The restriction on the template file extension
#is there so that someone does not assign .yml extension to a template file (which
#would require deliberately overriding .yml ignore in the .gitignore file) but this
#restriction could be removed.

INPUT_FILE_EXTENSION  = '.input'
OUTPUT_FILE_EXTENSION  = ['.yml', '.yaml']
#this restriction on template file extension could be removed
TEMPLATE_FILE_EXTENSION  = '.tpl'
ENV_FILE = '.env'

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
    print("extension: {0}".format(file_extension))
    return file_extension

project_root = args['projectroot']
input_file = args['input']
template_file = args['template']
output_file = args['output']

if extension(output_file) == '':
    print("Output file extension is blank")

#the output needs to have a name that has .yaml or .yml as an extension
#or it needs to be called .env
if ((extension(input_file) != INPUT_FILE_EXTENSION) or
    not (((extension(output_file) == '') and (os.path.split(output_file)[1] == ENV_FILE))
     or (extension(output_file) in OUTPUT_FILE_EXTENSION))
    or
    (extension(template_file) != TEMPLATE_FILE_EXTENSION)
    ):
    print("Either input, output or template files do not have appropriate extension; exiting")
    exit(0)

#absolute dir part to template file
print(project_root)
print(input_file)
print(template_file)
print(output_file)
print(extension(output_file))

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

template = ENV.get_template(template_file)

#print(template)
#print (template.render())
print (template.render(customconfig=customconfig))

#generating actual configuration files

with open(output_file, "w") as file2:
    try:
        file2.write(template.render(customconfig=customconfig))
    except:
        print( "Problem while creating/generating actual (environment/deployment/service/routing) configuration files")

#in .gitignore we would not let the user commit files with extension or name
#.input; .env, .yml, .yaml unless explicitly negated (would be needed for
# docker-compose.yml as we would want to be able to commit it)
