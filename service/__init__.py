# VT-13-Jul-2016
# FL-14-Dec-2016

from __future__ import absolute_import

database = 'datasets'

configpath = "/etc/apache2/clioinfra.conf"
#configpath = "/etc/apache2/russianrep.conf"	# inluded in clioinfra.conf

FORBIDDENURI = "(curl|bash|mail|ping|sleep|passwd|cat\s+|cp\s+|mv\s+|certificate|wget|usr|bin|lhost|lport)"
FORBIDDENPIPES = '[\|\;><`()$]'
ERROR1 = "Something went wrong with special characters in url..."
ERROR2 = "Something definitely went wrong with specific terms in url..."
