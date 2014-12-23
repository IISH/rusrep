#!/usr/bin/python

import glob
import csv
import xlwt
import os
import sys
import psycopg2
import psycopg2.extras
import pprint
import collections
import getopt
import xlsxwriter
import ConfigParser

# Find configuration
def walkpath (name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

def findconfig(configfile):
    #configfile = 'russianrep.config'
    basedir = '/etc/apache2'

    if os.path.isfile(basedir + '/' + configfile):
         filename = basedir + '/' + configfile
    else:
         filename = walkpath(configfile, '../../../')
    return filename

# Reading parameters
def read_params():
	year = 0
	datatype = 0
	filename = 'output.xls'
	region = 0
	fields = ''
	global debug 
	debug = 0

	try:
    	    myopts, args = getopt.getopt(sys.argv[1:],"y:d:h:r:f:Dp:F:c:")
        except getopt.GetoptError as e:
    		print (str(e))
    		print("Usage: %s -y year -d datatype -r region -f filename -DDEBUG -o output" % sys.argv[0])
    		sys.exit(2)
 
	for o, a in myopts:
    	    if o == '-y':
                year=a
    	    elif o == '-d':
        	datatype=a
	    elif o == '-r':
		region=a
	    elif o == '-f':
		filename=a
	    elif o == '-p':
		path=a
            elif o == '-F':
                fields=a
            elif o == '-c':
                copyrights=a
	    elif o == '-D':
		debug=1

	if debug:
	    print filename + "\n"

        return (year, datatype, region, filename, path, fields, copyrights, debug)

def load_data(year, datatype, region, copyrights, debug):
        #Define connection to database from default configuration file 
        cparser = ConfigParser.RawConfigParser()
        cpath = findconfig('russianrep.config');
        cparser.read(cpath)

        conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (cparser.get('config', 'dbhost'), cparser.get('config', 'dbname'), cparser.get('config', 'dblogin'), cparser.get('config', 'dbpassword'))
        # products will be loaded in tulips
        data = {}

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        cursor = conn.cursor()

        # execute our Query
        # Example SQL: cursor.execute("select * from russianrepository where year='1897' and datatype='3.01' limit 1000")
	query = "select * from russianrepository WHERE 1 = 1 ";
        if year > 0:
	    query += " AND base_year = '%s'" % year
        if datatype > 0:
            query += " AND datatype = '%s'" % datatype
	if region:
	    query += " AND territory = '%s'" % region
        if debug:
	    print query + " TEST <br>\n"
	# In Excel 2003, the maximum worksheet size is 65536 rows by 256 columns
	# this should be improved in further version
	query += ' order by ter_code asc limit 65535'

	# execute
        cursor.execute(query)

        # retrieve the records from the database
        records = cursor.fetchall()

        row_count = 0
	i = 0
        for row in records:
		i = i + 1
		data[i] = row
#		print row[0]

        return (i, data)

def main():
    # Initialization
    row_count = 0
    year = 0
    datatype = 0
    dataset = {}
    value = 0
    filename = ''
    datadir = "/home/clio-infra/public_html/tmp/"

    (year, datatype, region, filename, datadir, fieldline, copyrights, debug) = read_params()
    (row_count, dataset) = load_data(year, datatype, region, copyrights, debug)

    wb = xlwt.Workbook(encoding='utf')

    f_short_name = "Data"
    ws = wb.add_sheet(str(f_short_name))
    fieldline = "ID,TERRITORY,TER_CODE,TOWN,DISTRICT,YEAR,MONTH,VALUE,VALUE_UNIT,VALUE_LABEL,DATATYPE,HISTCLASS1,HISTCLASS2,HISTCLASS3,HISTCLASS4,HISTCLASS5,HISTCLASS6,HISTCLASS7,HISTCLASS8,HISTCLASS9,HISTCLASS10,CLASS1,CLASS2,CLASS3,CLASS4,CLASS5,CLASS6,CLASS7,CLASS8,CLASS9,CLASS10,COMMENT_SOURCE,SOURCE,VOLUME,PAGE,NABORSHIK_ID,COMMENT_NABORSHIK"
    fieldnames = fieldline.split(',')
    i = 0
    for row in fieldnames:
	ws.write(0, i, row)
        i = i+1

    for i in range(1,row_count+1):
	ulen = len(dataset[i]) - 1
	for j in range(1,ulen):
	     value = dataset[i][j]
             if not (value > 0):
		if (j == 8):
                	value = "."
		else:
			value = '.'
             ws.write(i, j-1, value)

    f_copyrights_name = "Copyrights"
    wscop = wb.add_sheet(str(f_copyrights_name))
    wscop.write(0,0,copyrights)

    wb.save(datadir + "/" + filename)
    print datadir + "/" + filename

    if debug:
        print datadir + filename

main()
