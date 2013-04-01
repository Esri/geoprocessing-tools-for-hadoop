# read and log the version of package
import os
from webhdfs import WebHDFS, WebHDFSError

version = "latest"

try:
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
    version = open(fn).read().strip()
except IOError:
    pass
