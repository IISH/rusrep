from flask import Flask, Response, request
from twisted.web import http
import json
import urllib2
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

def connect():
    	# Define connection to Russian Repository database
    	conn_string = "host='10.24.63.148' dbname='russian_pilot1' user='clioweb' password='clio-dev-911'"

    	# get a connection, if a connect cannot be made an exception will be raised here
    	conn = psycopg2.connect(conn_string)

    	# conn.cursor will return a cursor object, you can use this cursor to perform queries
    	cursor = conn.cursor()

    	#(row_count, dataset) = load_regions(cursor, year, datatype, region, debug)
	return cursor

def load_years(cursor):
        data = {}
        sql = "select * from datasets.years where 1=1";
        # execute
        cursor.execute(sql)

        # retrieve the records from the database
        data = cursor.fetchall()
        json_string = json.dumps(data, encoding="utf-8")

        return json_string

def sqlfilter(sql):
        items = ''
        sqlparams = ''

	for key, value in request.args.items():
            items = request.args.get(key, '')
            itemlist = items.split(",")
            for item in itemlist:
                sqlparams = "\'%s\',%s" % (item, sqlparams)
            sqlparams = sqlparams[:-1]
            sql += " AND %s in (%s)" % (key, sqlparams)
	return sql

def load_topics(cursor):
        data = {}
        sql = "select * from datasets.topics where 1=1";
	sql = sqlfilter(sql) 

        # execute
        cursor.execute(sql)

        # retrieve the records from the database
        data = cursor.fetchall()
        json_string = json.dumps(data, encoding="utf-8")

        return json_string

def load_regions(cursor):
        data = {}
        sql = "select * from datasets.regions where 1=1";
	sql = sqlfilter(sql)
	sql = sql + ';'
        # execute
        cursor.execute(sql)

        # retrieve the records from the database
        data = cursor.fetchall()
        json_string = json.dumps(data, encoding="utf-8")

        return json_string

def load_data(cursor, year, datatype, region, debug):
        data = {}

        # execute our Query
        # Example SQL: cursor.execute("select * from russianrepository where year='1897' and datatype='3.01' limit 1000")
	#    for key, value in request.args.iteritems():
	#        extra = "%s<br>%s=%s<br>" % (extra, key, value)

        query = "select * from russianrepository WHERE 1 = 1 ";
	query = sqlfilter(query)
        if debug:
            print "DEBUG " + query + " <br>\n"
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
#               print row[0]
        json_string = json.dumps(data, encoding="utf-8")

        return json_string;

app = Flask(__name__)

@app.route('/')
def test():
    description = 'Russian Repository API Service v.0.1<br>/service/regions<br>/service/topics<br>/service/data<br>/service/histclasses<br>/service/years<br>/service/maps (reserved)<br>'
    return description

@app.route('/topics')
def topics():
    cursor = connect()
    data = load_topics(cursor)
    return data

@app.route('/years')
def years():
    cursor = connect()
    data = load_years(cursor)
    return data

@app.route('/regions')
def regions():
    cursor = connect()
    data = load_regions(cursor)
    return data

@app.route('/data')
def data():
    cursor = connect()
    year = 0
    datatype = '1.01'
    region = 0
    debug = 0
    data = load_data(cursor, year, datatype, region, debug)
    return data

# http://bl.ocks.org/mbostock/raw/4090846/us.json
@app.route('/maps')
def maps():
    donors_choose_url = "http://bl.ocks.org/mbostock/raw/4090846/us.json"
    response = urllib2.urlopen(donors_choose_url)
    json_response = json.load(response)
    return json.dumps(json_response)

if __name__ == '__main__':
    app.run()
