
import subprocess

version=subprocess.check_output(['git', 'describe','--always']).decode('utf-8').strip()

#we are adding always flag to get git hash  when tag is not present..
#we want the deployment to PyPI (as well as docker) to fail if there is no tagged commit
#build would still happen but due to "on tag" set in deployment section of travis config the push to PyPI 
#would be prevented if there is no tagged commit 
#the decode and utf-8 options are there to convert byte to string
#if we want the build to fail (the call to get version would fail) if there is not tagged commit then 
#we can remove the  '--always' option in the subprocess.check_output command used above
