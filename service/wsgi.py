# Copyright (C) 2014 International Institute of Social History.
# @author Vyacheslav Tykhonov <vty@iisg.nl>
# @author Fons Laan

import os, sys

# setup the virtual environment for the web server
activate_this = "/home/dpe/python2714/bin/activate_this.py"
execfile( activate_this, dict( __file__ = activate_this ) ) # Py2
#exec( open( activate_this ).read() )                       # Py3

reload( os )
reload( sys )
import logging

#os.environ[ "FLASK_DEBUG" ] = "1"

RUSREP_HOME = "/home/dpe/rusrep"
RUSSIANREPO_CONFIG_PATH = RUSREP_HOME + "/config/russianrepo.config"
os.environ[ "RUSSIANREPO_CONFIG_PATH" ] = RUSSIANREPO_CONFIG_PATH

sys.path.insert( 0, RUSREP_HOME + "/service" )

#logging.basicConfig( level = logging.DEBUG )   # apache error.log can become very big (> 500 MB)
logging.basicConfig( level = logging.INFO )
logging.debug( __file__ )

logging.debug( "Python version: %s" % sys.version )

os_path = os.environ.get( "PATH", '' )
logging.debug( "PATH: %s" % os_path )
os_pythonpath = os.environ.get( "PYTHONPATH", '' )
logging.debug( "PYTHONPATH: %s" % os_pythonpath )

from services import app as application
