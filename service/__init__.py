# VT-13-Jul-2016
# FL-16-Jan-2017

from __future__ import absolute_import

import os
import sys

database = 'datasets'

#configpath = "/etc/apache2/russianrep.conf"	# inluded in clioinfra.conf
configpath = "/etc/apache2/clioinfra.conf"
if not os.path.isfile(configpath):
    print("in %s" % __file__)
    print("configpath %s DOES NOT EXIST" % configpath )
    print("EXIT" )
    sys.exit(1)

FORBIDDENURI = "(curl|bash|mail|ping|sleep|passwd|cat\s+|cp\s+|mv\s+|certificate|wget|usr|bin|lhost|lport)"
FORBIDDENPIPES = '[\|\;><`()$]'
ERROR1 = "Something went wrong with special characters in url..."
ERROR2 = "Something definitely went wrong with specific terms in url..."
