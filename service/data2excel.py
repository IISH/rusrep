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

# Reading parameters
def read_params():
	year = 0
	datatype = 0
	filename = 'output.xls'
	region = 0
	global debug 
	debug = 0

	try:
    	    myopts, args = getopt.getopt(sys.argv[1:],"y:d:h:r:f:Dp:")
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
	    elif o == '-D':
		debug=1

	if debug:
	    print filename + "\n"

        return (year, datatype, region, filename, path, debug)

def load_data(year, datatype, region, debug):
        #Define connection to products database
        conn_string = "host='10.24.63.148' dbname='russian_pilot1' user='clioweb' password='clio-dev-911'"
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
	    query += " AND year = '%s'" % year
        if datatype > 0:
            query += " AND datatype = '%s'" % datatype
	if region:
	    query += " AND territory = '%s'" % region
        if debug:
	    print query + " TEST <br>\n"
	query += 'order by territory asc'

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

    (year, datatype, region, filename, datadir, debug) = read_params()
    (row_count, dataset) = load_data(year, datatype, region, debug)

    wb = xlwt.Workbook(encoding='utf')

    f_short_name = "Data"
    ws = wb.add_sheet(str(f_short_name))
    for i in range(1,row_count):
	for j in range(len(dataset[i])):
	     value = dataset[i][j]
             ws.write(i, j, value)

    wb.save(datadir + "/" + filename)
    print datadir + "/" + filename

    if debug:
        print datadir + filename

main()
