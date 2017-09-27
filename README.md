RusRep
======

FL-27-Sep-2017

@author Vyacheslav Tykhonov, IISH
@author Fons Laan, IISH

GNU GENERAL PUBLIC LICENSE Version 3 applies, see LICENSE file. 

Russian Repository version 1.0
Participating institutions are:
- Interdisciplinary Centre for Studies in History, Economy and Society (ICSHES)
- International Institute of Social History (Royal Netherlands Academy of Arts and Sciences), Amsterdam, Netherlands
- New Economic School, Moscow, Russia (NES)

For general documentation on The Electronic Repository of Russian Historical Statistics, 
see https://datasets.socialhistory.org/dataverse/RISTAT

Tested with Ubuntu 12.04
Needed software:
- Python
    To create a virtual Python, see install/virtual-python-2.7.14.txt
    For needed addional Python modules, see install/requirements_py2.7.txt

- Apache, plus WSGI module
- add to the apache config file:
    WSGIDaemonProcess <RUSREP_URL> processes=2 threads=15 display-name=%{GROUP}
    WSGIProcessGroup <RUSREP_URL>
    WSGIScriptAlias /service <RUSREP_HOME>/service/wsgi.py
- create a wsgi.py from wsgi.py.default

- PostgreSQL, use the schema install/ristat.russianrepository-schema.sql

- MongoDB

- For daily automatic updates from the RiStat dataverse, one can add a line to crontab:
  0 0 * * * /<RUSREP_HOME>/etl/autoupdate.sh

[eof]
