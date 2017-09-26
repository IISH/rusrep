# Copyright (C) 2014 International Institute of Social History.
# @author Vyacheslav Tykhonov <vty@iisg.nl>
#
# This program is free software: you can redistribute it and/or  modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the GNU Affero General Public License in all respects
# for all of the code used other than as permitted herein. If you modify
# file(s) with this exception, you may extend this exception to your
# version of the file(s), but you are not obligated to do so. If you do not
# wish to do so, delete this exception statement from your version. If you
# delete this exception statement from all source files in the program,
# then also delete it in the license file.


import os, sys


# setup the virtual environment for the web server
# see also services.py/documentation()
#SSLError: hostname 'datasets.socialhistory.org' doesn't match 'data.socialhistory.org'
activate_this = "/home/dpe/python2714/bin/activate_this.py"
execfile( activate_this, dict( __file__ = activate_this ) ) # Py2
#exec( open( activate_this ).read() )                       # Py3

reload( os )
reload( sys )
import logging


os.environ[ "FLASK_DEBUG" ] = "1"

RUSREP_HOME = "/home/dpe/rusrep"
RUSSIANREPO_CONFIG_PATH = RUSREP_HOME + "/config/russianrepo.config"
os.environ[ "RUSSIANREPO_CONFIG_PATH" ] = RUSSIANREPO_CONFIG_PATH

sys.path.insert( 0, RUSREP_HOME + "/service" )

logging.basicConfig( level = logging.DEBUG )   # apache error.log can become very big (> 500 MB)
#logging.basicConfig( level = logging.INFO )
logging.debug( __file__ )

logging.debug( "Python version: %s" % sys.version )

os_path = os.environ.get( "PATH", '' )
logging.debug( "PATH: %s" % os_path )
os_pythonpath = os.environ.get( "PYTHONPATH", '' )
logging.debug( "PYTHONPATH: %s" % os_pythonpath )

from services import app as application
