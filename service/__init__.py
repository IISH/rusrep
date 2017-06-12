# VT-13-Jul-2016
# FL-12-Jun-2017

from __future__ import absolute_import

import os
import sys

database = "datasets"

config_path = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]

if os.path.isfile( config_path ):
    #print( __file__ )
    #print( "using configpath: %s" % config_path )
    pass
else:
    print( "in %s" % __file__ )
    print( "configpath %s FILE DOES NOT EXIST" % config_path )
    print( "EXIT" )
    sys.exit( 1 )

FORBIDDENURI = "(curl|bash|mail|ping|sleep|passwd|cat\s+|cp\s+|mv\s+|certificate|wget|usr|bin|lhost|lport)"
FORBIDDENPIPES = '[\|\;><`()$]'
ERROR1 = "Something went wrong with special characters in url..."
ERROR2 = "Something definitely went wrong with specific terms in url..."
