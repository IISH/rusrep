from flask import Flask, Response, request
from twisted.web import http
import json
import tables
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
import ConfigParser
import re

def connect():
        cparser = ConfigParser.RawConfigParser()
        cpath = "/etc/apache2/rusrep.config"
        cparser.read(cpath)

	conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (cparser.get('config', 'dbhost'), cparser.get('config', 'dbname'), cparser.get('config', 'dblogin'), cparser.get('config', 'dbpassword'))

    	# get a connection, if a connect cannot be made an exception will be raised here
    	conn = psycopg2.connect(conn_string)

    	# conn.cursor will return a cursor object, you can use this cursor to perform queries
    	cursor = conn.cursor()

    	#(row_count, dataset) = load_regions(cursor, year, datatype, region, debug)
	return cursor

def json_generator(c, jsondataname, data):
	sqlnames = [desc[0] for desc in c.description]
        jsonlist = []
        jsonhash = {}
        
        for valuestr in data:    
            datakeys = {}
            for i in range(len(valuestr)):
               name = sqlnames[i]
               value = valuestr[i]
               datakeys[name] = value
               #print "%s %s", (name, value)
            jsonlist.append(datakeys)
        
        jsonhash[jsondataname] = jsonlist;
        json_string = json.dumps(jsonhash, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)
	#encoding="utf8"
	#.decode('unicode-escape')
 	#.encode('utf8')
	#json_string = json.dumps(jsonhash, ensure_ascii=False,indent=4).encode('utf8')

        return json_string

def translatedclasses(cursor, classinfo):
    dictdata = {}
    sql = "select * from datasets.classmaps"; # where class_rus in ";
    sqlclass = ''
    for classname in classinfo:
	if sqlclass:
            sqlclass = "%s, '%s'" % (sqlclass, classinfo[classname])
	else:
	    sqlclass = "'%s'" % classinfo[classname]
    sql = "%s (%s)" % (sql, sqlclass)
 
    sql = "select * from datasets.regions";
    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [desc[0] for desc in cursor.description]
    if data:
        for valuestr in data:
            datakeys = {}
            for i in range(len(valuestr)):
                name = sqlnames[i]
                value = valuestr[i]
		if name == 'region_name':
		    name = 'class_rus'
		if name == 'region_name_eng':
		    name = 'class_eng'
                datakeys[name] = value

            dictdata[datakeys['class_eng']] = datakeys
	    dictdata[datakeys['class_rus']] = datakeys
        
    sql = "select * from datasets.valueunits";
    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [desc[0] for desc in cursor.description]
    if data:
        for valuestr in data:
	    datakeys = {}
            for i in range(len(valuestr)):
                name = sqlnames[i]
                value = valuestr[i]
                datakeys[name] = value
            dictdata[datakeys['class_rus']] = datakeys
            dictdata[datakeys['class_eng']] = datakeys

    # FIX
    sql = "select * from datasets.classmaps";
    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [desc[0] for desc in cursor.description]
    if data:
        for valuestr in data:
            datakeys = {}
            for i in range(len(valuestr)):
               name = sqlnames[i]
               value = valuestr[i]
               datakeys[name] = value
            dictdata[datakeys['class_rus']] = datakeys
	    dictdata[datakeys['class_eng']] = datakeys

    return dictdata

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
            if key == 'basisyear':
                sql += " AND %s LIKE '%s" % ('region_code', itemlist[0])
		sql += "%'"
            else:
                for item in itemlist:
                    sqlparams = "\'%s\',%s" % (item, sqlparams)
                sqlparams = sqlparams[:-1]
                sql += " AND %s in (%s)" % (key, sqlparams)
	return sql

def sqlconstructor(sql):
        items = ''
        sqlparams = ''

        for key, value in request.args.items():
            items = request.args.get(key, '')
            itemlist = items.split(",")
	    if key == 'language':
		skip = 1
            elif key == 'classification':
                skip = 1
	    elif key == 'basisyear':
		sql += " AND %s like '%s'" % ('region_code', sqlparams)
	    else:
                for item in itemlist:
                    sqlparams = "\'%s\'" % item
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
        jsondata = json_generator(cursor, 'data', data)
        
        return jsondata

def datasetfilter(data, sqlnames, classification):
    if data:
        # retrieve the records from the database
        datafilter = []
        for dataline in data:
            datarow = {}
            active = ''
            for i in range(len(sqlnames)):
                name = sqlnames[i]
                if classification == 'historical':
                    if name.find("class", 0):
                        try:
                            nextvalue = dataline[i+1]
                        except:
                            nextvalue = '.'

                        if (dataline[i] == "." and nextvalue == "."):
                            skip = 'yes'
                        else:
                            toplevel = re.search("(\d+)", name)
                            if name.find("histclass10", 0):
                                datarow[name] = dataline[i]
                                if toplevel:
                                    datarow["levels"] = toplevel.group(0)
                if classification == 'modern':
                    if name.find("histclass", 0):
                        try:
                            nextvalue = dataline[i+1]
                        except:
                            nextvalue = '.'

                        if (dataline[i] == "." and nextvalue == "."):
                            skip = 'yes'
                        else:
                            toplevel = re.search("(\d+)", name)
                            if name.find("class10", 0):
                                datarow[name] = dataline[i]
                                if toplevel:
                                    if toplevel.group(0) != '10':
                                        datarow["levels"] = toplevel.group(0)
            try:
                if datarow["levels"] > 0:
                    datafilter.append(datarow)
            except:
                skip = 'yes'

        if classification:
	    #return datafilter
            return json.dumps(datafilter, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)

def load_classes(cursor):
        data = {}
	engdata = {}
	total = 0
	classification = 'historical'
	if request.args.get('classification'):
	    classification = request.args.get('classification')
	if request.args.get('language') == 'en':
	    engdata = translatedclasses(cursor, request.args)
	if request.args.get('overview'):
	    sql = "select distinct %s, year, datatype from datasets.classification where 1=1" % request.args.get('overview') 
	    sql = sql + " AND %s <> '.'" % request.args.get('overview')
	    if request.args.get('year'):
		sql = sql + " AND %s = '%s' " % ('year', request.args.get('year'))
            if request.args.get('datatype'):
                sql = sql + " AND %s = '%s' " % ('datatype', request.args.get('datatype'))
	
	else:
	    sql = "select * from datasets.classification where 1=1";
            sql = sqlconstructor(sql)

        # execute
        cursor.execute(sql)
	sqlnames = [desc[0] for desc in cursor.description]

        # retrieve the records from the database
	datafilter = []
        data = cursor.fetchall()
	for dataline in data:
	    datarow = {}
	    active = ''
	    for i in range(len(sqlnames)):
		name = sqlnames[i]
                if classification == 'historical':
                    if name.find("class", 0):
			try:
			    nextvalue = dataline[i+1]
			except:
			    nextvalue = '.'

			if (dataline[i] == "." and nextvalue == "."):
			    skip = 'yes'
			else:
			    toplevel = re.search("(\d+)", name)
			    if name.find("histclass10", 0):
				value = dataline[i]
				if value in engdata:
				    value = engdata[value]['class_eng']
			        datarow[name] = str(value) 
			        if toplevel:
				    datarow["levels"] = toplevel.group(0)

	        if classification == 'modern':
		    if name.find("histclass", 0):
                        try:
                            nextvalue = dataline[i+1]
                        except:
                            nextvalue = '.'

		 	if (dataline[i] == "." and nextvalue == "."):
                            skip = 'yes'
                        else:
			    toplevel = re.search("(\d+)", name)
			    if name.find("class10", 0):
                                datarow[name] = dataline[i]
                                if toplevel:
				    if toplevel.group(0) != '10':
                                        datarow["levels"] = toplevel.group(0)
	    try:
	        if datarow["levels"] > 0:	
	    	    datafilter.append(datarow)
	    except:
		skip = 'yes'

	if classification:
	    return json.dumps(datafilter, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)

        jsonlist = []
        jsonhash = {}

        for valuestr in data:
            datakeys = {}
	    sortedkeys = []
            for i in range(len(valuestr)):
                name = sqlnames[i]
                value = valuestr[i]
		if classification == 'historical':
	            if not name.find("class", 1):
			datakeys[name] = value 
                else:
		    datakeys[name] = value
	    for i in range(10, 1, -1):
	        histclass = "histclass%s" % i
	        mclass = "class%s" % i
            jsonlist.append(datakeys)
#	return str(jsonlist)

        jsondata = json_generator(cursor, 'data', data)

        return jsondata

def load_histclasses(cursor):
        data = {}
        sql = "select * from datasets.histclasses where 1=1";
        sql = sqlfilter(sql)

        # execute
        cursor.execute(sql)

        # retrieve the records from the database
        data = cursor.fetchall()
        jsondata = json_generator(cursor, 'data', data)

        return jsondata

def load_regions(cursor):
        data = {}
        sql = "select * from datasets.regions where 1=1";
	sql = sqlfilter(sql)
	sql = sql + ';'
        # execute
	#return sql
        cursor.execute(sql)

        # retrieve the records from the database
        data = cursor.fetchall()
	jsondata = json_generator(cursor, 'regions', data)
	return jsondata

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
        query += ' order by territory asc'

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
	jsondata = json_generator(cursor, 'data', records)

        return jsondata;

app = Flask(__name__)

@app.route('/')
def test():
    description = 'Russian Repository API Service v.0.1<br>/service/regions<br>/service/topics<br>/service/data<br>/service/histclasses<br>/service/years<br>/service/maps (reserved)<br>'
    return description

@app.route('/topics')
def topics():
    cursor = connect()
    data = load_topics(cursor)
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/histclasses')
def histclasses():
    cursor = connect()
    data = load_histclasses(cursor)
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/aggregate1')
def aggr():
    cursor = connect()
    data = aggregate(cursor)
    return Response(data,  mimetype='application/json; charset=utf-8')

class Histclass(tables.IsDescription):
    histclass1 = tables.StringCol(256,pos=0)
    histclass2 = tables.StringCol(256,pos=0)
    histclass3 = tables.StringCol(256,pos=0)

@app.route('/aggregate', methods=['POST', 'GET'])
def aggr():
    data = {}
    sqlfields = ''
    sqlkeys = ''
    engdata = {}
    cursor = connect()
    total = 0
    try:
        qinput = json.loads(request.data)
    except:
        return '{}'

    forbidden = ["classification", "action", "language", "path"]
    if cursor:
        #     extra = "%s<br>%s=%s<br>" % (extra, key, value)
	if 'language' in qinput:
            if qinput['language']== 'en':
                engdata = translatedclasses(cursor, request.args)

	for key in qinput:
	    if key not in forbidden:
	        value = qinput[key]
                if sqlfields:
                    sqlfields = "%s, %s" % (sqlfields, key)
                    sqlkeys = "%s, %s" % (sqlkeys, key)
                else:
                    sqlfields = key
                    sqlkeys = key

        sql = "select cast(value as double precision) as value, value_unit, territory, year, histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10, %s from russianrepository where 1=1" % sqlfields
        for name in qinput:
            if not name in forbidden:
	        value = str(qinput[name])
                if value in engdata:
                    value = engdata[value]['class_rus']
	        if value[0] != "[":
                    sql+= " AND %s = '%s'" % (name, qinput[name])
		else:
		    orvalue = ''
		    for val in qinput[name]:
			if val in engdata:
			    val = engdata[val]['class_rus']
			orvalue+=" '%s'," % (val)
		    orvalue = orvalue[:-1]
		    sql+= " AND %s IN (%s)" % (name, orvalue)
            elif name == 'path':
                fullpath = qinput[name]
		topsql = 'AND ('
		for path in fullpath:	
		    tmpsql = ' ('
                    for xkey in path:
		        value = path[xkey]
		        if value in engdata:
			    value = str(engdata[value]['class_rus'])
			try:
		            tmpsql+= " %s = '%s' AND " % (xkey, value.decode('utf-8'))
			except:
			    tmpsql+= " %s = '%s' AND " % (xkey, value)
		    tmpsql+='1=1 ) '
		    topsql+=tmpsql + " OR "
		topsql = topsql[:-3]
		topsql+=')'
		sql+=topsql

        #sql = sqlconstructor(sql)
        wheresql = "group by histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10, territory, year, %s, value_unit, value" % sqlkeys
        sql = "%s %s" % (sql, wheresql)

        # execute
        cursor.execute(sql)
	sqlnames = [desc[0] for desc in cursor.description]

        # retrieve the records from the database
        data = cursor.fetchall()
	result = []
	chain = {}
        class inchain(object):
    	    def __init__(self,name):
                self.name = name

	hclasses = {}
	for row in data:
	    lineitem = {}
	    for i in range(0, len(sqlnames)):
		if row[i]:
		    value = row[i]
                    if value in engdata:
                        value = engdata[value]['class_eng']
	            lineitem[sqlnames[i]] = value 
            try:
                total+=float(lineitem['value'])
            except:
                total = 'NA'

	    sorteditems = {}
	    order = []
	    for item in sorted(lineitem):
	 	order.append(item)
	    for item in order:	
		sorteditems[item] = lineitem[item]	
	    x = collections.OrderedDict(sorted(sorteditems.items()))
	    #return json.dumps(x)

	    vocab = {}
	    for i in range(1,10):
	        histkey = "histclass%s" % str(i)
		if histkey in x:
		    vocab[histkey] = x[histkey]
		    del x[histkey]
	
	    x['histclases'] = vocab
	    result.append(x)
	
        #jsondata = json_generator(cursor, 'data', result)
	#result = hclasses
	final = {}
	final['url'] = 'http://ristat.sandbox.socialhistoryservices.org/datasets/indicators?download=20160329.115021.zip'
	final['total'] = total
	final['data'] = result

        return Response(json.dumps(final),  mimetype='application/json; charset=utf-8')

@app.route('/classes')
def classes():
    cursor = connect()
    data = load_classes(cursor)
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/years')
def years():
    cursor = connect()
    data = load_years(cursor)
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/regions')
def regions():
    cursor = connect()
    data = load_regions(cursor)
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/data')
def data():
    cursor = connect()
    year = 0
    datatype = '1.01'
    region = 0
    debug = 0
    data = load_data(cursor, year, datatype, region, debug)
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/translate')
def translate():
    cursor = connect()
    if cursor:
        data = {}
        sql = "select * from datasets.classmaps where 1=1";
        sql = sqlfilter(sql)

        # execute
        cursor.execute(sql)

        # retrieve the records from the database
        data = cursor.fetchall()
        jsondata = json_generator(cursor, 'data', data)
	return Response(jsondata,  mimetype='application/json; charset=utf-8')

@app.route('/filter', methods=['POST', 'GET'])
def login(settings=''):
    cursor = connect()
    filter = {}
    try:
        qinput = request.json
    except:
        return '{}' 
    try:
        if qinput['action'] == 'aggregate':
	    sql = "select histclass1, datatype, value_unit, value, ter_code from russianrepository where 1=1 "
    except:
	sql = "select * from datasets.classification where 1=1";
        #datatype='7.01' and base_year='1897' group by histclass1, datatype, value_unit, ter_code, value;
    try:
	classification = qinput['classification']	
    except:
	classification = 'historical'
    forbidden = ["classification", "action", "language"]
    for name in qinput:
	if not name in forbidden:
	    sql+= " AND %s='%s'" % (name, qinput[name])

    #return sql
    if sql:
        # execute
        cursor.execute(sql)
	sqlnames = [desc[0] for desc in cursor.description]

        data = cursor.fetchall()
	jsondata = datasetfilter(data, sqlnames, classification)
	return Response(jsondata,  mimetype='application/json; charset=utf-8')
    else:
	return ''

# http://bl.ocks.org/mbostock/raw/4090846/us.json
@app.route('/maps')
def maps():
    donors_choose_url = "http://bl.ocks.org/mbostock/raw/4090846/us.json"
    response = urllib2.urlopen(donors_choose_url)
    json_response = json.load(response)
    return Response(json_response,  mimetype='application/json; charset=utf-8')

if __name__ == '__main__':
    app.run()
