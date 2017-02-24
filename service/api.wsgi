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

import logging
from os import environ
from sys import path, version

"""
# setup the virtual environment for the web server
activate_this = "/data/opt/python2713/bin/activate_this.py"
execfile( activate_this, dict( __file__ = activate_this ) ) # Py2
#exec( open( activate_this ).read() )                       # Py3
"""

RUSREP_HOME = "/home/dpe/rusrep"
RUSREP_CONFIG_PATH = RUSREP_HOME + "/config/russianrep.config"
environ[ "RUSREP_CONFIG_PATH" ] = RUSREP_CONFIG_PATH

path.insert( 0, RUSREP_HOME + "/service" )

logging.basicConfig( level = logging.DEBUG )
logging.debug( __file__ )
logging.debug( "Python version: %s" % version )

from services import app as application
