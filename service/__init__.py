# VT-13-Jul-2016
# FL-08-Feb-2017

from __future__ import absolute_import

import logging
import os
import sys

database = "datasets"

config_path = os.environ[ "CLIOINFRA_CONFIG_PATH" ]

if os.path.isfile( config_path ):
    logging.info( __file__ )
    logging.info( "using configpath: %s" % config_path )
else:
    logging.error( "in %s" % __file__ )
    logging.error( "configpath %s FILE DOES NOT EXIST" % config_path )
    logging.error( "EXIT" )
    sys.exit( 1 )

FORBIDDENURI = "(curl|bash|mail|ping|sleep|passwd|cat\s+|cp\s+|mv\s+|certificate|wget|usr|bin|lhost|lport)"
FORBIDDENPIPES = '[\|\;><`()$]'
ERROR1 = "Something went wrong with special characters in url..."
ERROR2 = "Something definitely went wrong with specific terms in url..."
