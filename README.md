rusrep
======

Russian Repository
Development version 0.1

The Electronic Repository of Russian Historical Statistics will be the first research tool of its kind and scope to be created in the Russian Federation. Project is based of ETL (Extract, Load, Transform) processes in order to store qualitative datasets in the structure of relational database. Users should be able to query datasets and download combined data as datasets.

The aim of this research project is to create an on-line electronic repository of Russian historical statistics. The repository operates on the principle of a historical data-hub, bringing together data extracted from various published and unpublished sources in one place. Its principal focus is Russian economic and social history of the last three centuries (18th-21st). All datasets will be visualized in the different way to extract insights and get meaning out of the data stored in the repository.

Participating institutions are:
- Interdisciplinary Centre for Studies in History, Economy and Society (ICSHES)
- International Institute of Social History (Royal Netherlands Academy of Arts and Sciences), Amsterdam, Netherlands
- New Economic School, Moscow, Russia (NES)

Dependencies:
PostgreSQL 8.4-9.3
Apache2

Perl modules: DBI Getopt::Long

Python modules (installation with pip manager):
	flask web framework
	http
	json
	urllib2
	glob
	csv
	xlwt
	os
	sys
	psycopg2
	psycopg2.extras
	pprint
	collections
	getopt

Apache configuration:
	Extend /etc/apache2/apache.conf with lines

	WSGIScriptAlias /service /home/clio-infra/public_html/service/api.wsgi
	WSGIDaemonProcess service user=clio-infra group=clio-infra processes=1 threads=5

	Add this instructions to node-xxx file:

    	<Directory /home/clio-infra/public_html/service>
        WSGIProcessGroup service
        WSGIApplicationGroup service
        Order deny,allow
        Allow from all
    	</Directory>
