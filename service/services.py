from __future__ import absolute_import
from flask import Flask, Response, request
import requests
from twisted.web import http
from pymongo import MongoClient
import json
import simplejson
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
import urllib
import random
import ConfigParser
import re
import os
import sys
import unittest
import pandas as pd
from StringIO import StringIO
import numpy as np
from rdflib import Graph, plugin, Namespace, Literal, term, BNode
from rdflib.serializer import Serializer
from rdflib.namespace import DC, FOAF
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname("__file__"), './')))
from cliocore.configutils import Configuration, Utils, DataFilter
from dataverse import Connection
from excelmaster import aggregate_dataset, preprocessor

forbidden = ["classification", "action", "language", "path"]

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

def classcollector(keywords):
    classdict = {}
    normaldict = {}
    for item in keywords:
        classmatch = re.search(r'class', item)
        if classmatch:
            classdict[item] = keywords[item]
        else:
            normaldict[item] = keywords[item]
    return (classdict, normaldict)

def json_generator(c, jsondataname, data):
        cparser = ConfigParser.RawConfigParser()
        cpath = "/etc/apache2/rusrep.config"
        cparser.read(cpath)

	sqlnames = [desc[0] for desc in c.description]
        jsonlist = []
        jsonhash = {}
        
	forbidden = {'dataactive', 0, 'datarecords', 1}
        for valuestr in data:    
            datakeys = {}
	    extravalues = {}
            for i in range(len(valuestr)):
                name = sqlnames[i]
                value = valuestr[i]
	        if name not in forbidden:
                    datakeys[name] = value
		else:
		    extravalues[name] = value

	    # If aggregation check data output for 'NA' values
	    if 'total' in datakeys:
		if extravalues['dataactive']:
		    datakeys['total'] = 'NA'

	    (path, output) = classcollector(datakeys)
	    output['path'] = path
            jsonlist.append(output)
        
	# Cache
	clientcache = MongoClient()
        dbcache = clientcache.get_database('datacache')

        jsonhash[jsondataname] = jsonlist
	newkey = str("%05.8f" % random.random())
	jsonhash['url'] = "%s/service/download?key=%s" % (cparser.get('config', 'root'), newkey)
	json_string = json.dumps(jsonhash, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)
	try:
	    thisdata = jsonhash
	    del thisdata['url']
	    thisdata['key'] = newkey
	    result = dbcache.data.insert(thisdata)
	except:
	    skip = 'something went wrong...'

        return json_string

def translatedvocabulary(newfilter):
    client = MongoClient()
    dbname = 'vocabulary'
    db = client.get_database(dbname)
    if newfilter:
	#vocab = db.data.find({"YEAR": thisyear})	
	vocab = db.data.find(newfilter)
    else:
        vocab = db.data.find()
    data = {}
    histdata = {}
    for item in vocab:
	if 'RUS' in item:
	    try:
	        item['RUS'] = item['RUS'].encode('UTF-8')
	        item['EN'] = item['EN'].encode('UTF-8')
                if item['RUS'].startswith('"') and item['RUS'].endswith('"'):
                    item['RUS'] = string[1:-1]
                if item['EN'].startswith('"') and item['EN'].endswith('"'):
                    item['EN'] = string[1:-1]
	        item['RUS'] = re.sub(r'"', '', item['RUS'])
	        item['EN'] = re.sub(r'"', '', item['EN'])
	        data[item['RUS']] = item['EN']
	        data[item['EN']] = item['RUS'] 
	    except:
	        skip = 1
    return data

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

def load_years(cursor, datatype):
	clioinfra = Configuration()
	years = clioinfra.config['years'].split(',')
        data = {}
 #       sql = "select * from datasets.years where 1=1";
	sql = "select base_year, count(*) as c from russianrepository where 1=1"
	if datatype:
	    sql=sql + " and datatype='%s'" % datatype 
	sql= sql + " group by base_year";
        cursor.execute(sql)

        # retrieve the records from the database
        data = cursor.fetchall()
	result = {}
	for val in data:
	    if val[0]:
                result[val[0]] = val[1]
	for year in years:
	     if int(year) not in result:
	         result[int(year)] = 0
	
#	jsondata = json_generator(cursor, 'years', result)
        json_string = json.dumps(result, encoding="utf-8")

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

def translateitem(item, engdata):
    # Translate first
    newitem = {}
    if engdata:
        for name in item:
            value = item[name].encode('UTF-8')
            if value in engdata:
                value = engdata[value]
            newitem[name] = value
        item = newitem
    return item

def load_vocabulary(vocname):
    client = MongoClient()
    dbname = 'vocabulary'
    db = client.get_database(dbname)
    newfilter = {}
    engdata = {}
    if request.args.get('classification'):
	vocname = request.args.get('classification')
	if vocname == 'historical':
	    newfilter['vocabulary'] = 'ERRHS_Vocabulary_histclasses'
	else:
	    newfilter['vocabulary'] = 'ERRHS_Vocabulary_modclasses'

    if request.args.get('language') == 'en':
	thisyear = ''
        if request.args.get('base_year'):
	    if vocname == 'historical':
                thisyear = request.args.get('base_year')
	        newfilter['YEAR'] = thisyear 
        engdata = translatedvocabulary(newfilter)
	units = translatedvocabulary({"vocabulary": "ERRHS_Vocabulary_units"})
	for item in units:
	    engdata[item] = units[item]

    params = {"vocabulary": vocname}
    for name in request.args:
	if name not in forbidden:
	    params[name] = request.args.get(name)

    if vocname:
        vocab = db.data.find(params)
    else:
        vocab = db.data.find()

    data = []
    uid = 0
    for item in vocab:
	del item['_id']
	del item['vocabulary']
	regions = {}
	if vocname == "ERRHS_Vocabulary_regions":
	    uid+=1
	    regions['region_name'] = item['RUS']
	    regions['region_name_eng'] = item['EN']
	    regions['region_code'] = item['ID']
	    regions['region_id'] = uid
	    regions['region_ord'] = 189702
	    regions['description'] = regions['region_name']
	    regions['active'] = 1
	    item = regions
	    data.append(item)
	elif vocname == 'modern':
	    if engdata:
                item = translateitem(item, engdata)
	    data.append(item)
        elif vocname == 'historical':
	    if engdata:
	        item = translateitem(item, engdata)
            data.append(item)
	else:
	    # Translate first
	    newitem = {}
	    if engdata:
		for name in item:
		    value = item[name]
		    if value in engdata: 
			value = engdata[value]
		    newitem[name] = value
		item = newitem

            (path, output) = classcollector(item)
	    if path:
                output['path'] = path
	        data.append(output)
	    else:
	        data.append(item)

    jsonhash = {}
    if vocname == "ERRHS_Vocabulary_regions":
	jsonhash['regions'] = data
    elif vocname == 'modern':
	jsonhash = data
    elif vocname == 'historical':
        jsonhash = data
    else:
        jsonhash['data'] = data
    jsondata = json.dumps(jsonhash, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)
    return jsondata

    return jsonhash

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

@app.route('/export')
def export():
    settings = Configuration()
    keys = ["intro", "intro_rus", "datatype_intro", "datatype_intro_rus", "note", "note_rus", "downloadpage1", "downloadpage1_rus" "downloadclick", "downloadclick_rus", "warningblank", "warningblank_rus", "mapintro", "mapintro_rus"]
    exportkeys = {}
    for ikey in keys:
	if ikey in settings.config:
	    exportkeys[ikey] = settings.config[ikey]
    result = json.dumps(exportkeys, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)
    return Response(result,  mimetype='application/json; charset=utf-8')

@app.route('/topics')
def topics():
    cursor = connect()
    data = load_topics(cursor)
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/histclasses')
def histclasses():
    cursor = connect()
    #data = load_histclasses(cursor)
    data = load_vocabulary('historical')
    return Response(data,  mimetype='application/json; charset=utf-8')

def rdfconvertor(url):
    f = urllib.urlopen(url)
    data = f.read()
    csvio = StringIO(str(data))
    dataframe = pd.read_csv(csvio, sep='\t', dtype='unicode')
    finalsubset = dataframe
    columns = finalsubset.columns
    rdf = "@prefix ristat: <http://ristat.org/api/vocabulary#> .\n"
    vocaburi = "http://ristat.org/service/vocab#"
    vocaburi = "http://data.sandbox.socialhistoryservices.org/service/vocab#"
    g=Graph()

    for ids in finalsubset.index:
        item = finalsubset.ix[ids]    
	uri = term.URIRef("%s%s" % (vocaburi, str(item['ID'])))        
	if uri:
            for col in columns:
                if col is not 'ID':
                    if item[col]:
			c = term.URIRef(col)
			g.add((uri, c, Literal(str(item[col]))))
                        rdf+="ristat:%s " % item['ID']
                        rdf+="ristat:%s ristat:%s." % (col, item[col])
                    rdf+="\n"
    return g

@app.route('/vocab')
def vocab():
    url = "https://datasets.socialhistory.org/api/access/datafile/586?&key=6f07ea5d-be76-444a-8a20-0ee2f02fda21&show_entity_ids=true&q=authorName:*"
    g = rdfconvertor(url)
    showformat = 'json'
    if request.args.get('format'):
	showformat = request.args.get('format')
    if showformat == 'turtle':
	jsondump = g.serialize(format='n3')
	return Response(jsondump,  mimetype='application/x-turtle; charset=utf-8')
    else:
        jsondump = g.serialize(format='json-ld', indent=4)
        return Response(jsondump,  mimetype='application/json; charset=utf-8')

@app.route('/vocabulary')
def getvocabulary():
    data = translatedvocabulary()
    json_string = json.dumps(data, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)
    return Response(json_string,  mimetype='application/json; charset=utf-8')

class Histclass(tables.IsDescription):
    histclass1 = tables.StringCol(256,pos=0)
    histclass2 = tables.StringCol(256,pos=0)
    histclass3 = tables.StringCol(256,pos=0)

def get_sql_query(name, value):
    sqlquery = ''    
    result = re.match("\[(.+)\]", value)   
    if result:
        query = result.group(1)
        ids = query.split(',')
        for param in ids:
	    param=re.sub("u'", "'", str(param))
            sqlquery+="%s," % param        
        if sqlquery:
            sqlquery = sqlquery[:-1]
            sqlquery = "%s in (%s)" % (name, sqlquery)
    else:
        sqlquery = "%s = '%s'" % (name, value)
    return sqlquery

@app.route('/aggregation', methods=['POST', 'GET'])
def aggregation():
    thisyear = ''
    try:
        qinput = json.loads(request.data)
    except:
        return '{}'
    engdata = {}
    cursor = connect()
    forbidden = ["classification", "action", "language", "path"]
    if cursor:
        #     extra = "%s<br>%s=%s<br>" % (extra, key, value)
        if 'language' in qinput:
            if qinput['language']== 'en':
                #engdata = translatedclasses(cursor, request.args)
		newfilter = {}
		if 'year' in qinput:
		    thisyear = qinput['year']
		    newfilter['YEAR'] = thisyear
		engdata = translatedvocabulary(newfilter)
		units = translatedvocabulary({"vocabulary": "ERRHS_Vocabulary_units"})
        	for item in units:
                    engdata[item] = units[item]

    sql = {}   
    sql['condition'] = ''
    knownfield = {}
    sql['groupby'] = ''
    sql['where'] = ''
    sql['internal'] = ''
    if qinput:
        for name in qinput:
            if not name in forbidden:
                value = str(qinput[name])
                if value in engdata:
                    #value = engdata[value]['class_rus']
		    value = engdata[value]
		#sql['where']+="%s='%s' AND1 " % (name, value)
		sql['where']+="%s AND " % get_sql_query(name, value)
		sql['condition']+="%s, " % name
		knownfield[name] = value

            elif name == 'path':
                fullpath = qinput[name]
                topsql = 'AND ('
                for path in fullpath:
                    tmpsql = ' ('
		    sqllocal = {}
		    clearpath = {}
		    for xkey in path:
			value = path[xkey]
			if value != '.':
			    clearpath[xkey] = value

                    for xkey in clearpath:
                        value = path[xkey]
                        if value in engdata:
                            #value = engdata[value]['class_rus']
			    value = engdata[value]
		        sqllocal[xkey]="%s='%s', " % (xkey, value)
			if not knownfield.has_key(xkey):
			    knownfield[xkey] = value
			    sql['condition']+="%s, " % xkey

                        if value in engdata:
                            #value = str(engdata[value]['class_rus'])
			    value = str(engdata[value])
                        try:
                            tmpsql+= " %s = '%s' AND " % (xkey, value.decode('utf-8'))
                        except:
                            tmpsql+= " %s = '%s' AND " % (xkey, value)
                    tmpsql+='1=1 ) '
                    topsql+=tmpsql + " OR "
		    if sqllocal:
			sql['internal']+=' ('
		        for key in sqllocal:
			    sqllocal[key] = sqllocal[key][:-2]
			    sql['internal']+="%s AND " % sqllocal[key]
			sql['internal'] = sql['internal'][:-4]
			sql['internal']+=') OR'
    sql['internal'] = sql['internal'][:-3]

#select sum(cast(value as double precision)), value_unit from russianrepository where datatype = '1.02' and year='2002' and histclass2 = '' and histclass1='1' group by histclass1, histclass2, value_unit;
    sqlquery = "select count(*) as datarecords, count(*) - count(value) as dataactive, sum(cast(value as double precision)) as total, value_unit, ter_code"
    if sql['where']:
	sqlquery+=", %s" % sql['condition']
        sqlquery = sqlquery[:-2]
        sqlquery+=" from russianrepository where %s" % sql['where']
	sqlquery = sqlquery[:-4]
    if sql['internal']:
	sqlquery+=" AND (%s) " % sql['internal']
    
    sqlquery+=" group by value_unit, ter_code, "
    for f in knownfield:
	sqlquery+="%s," % f
    sqlquery = sqlquery[:-1]
    #return sqlquery
#    forbidden = {'dataactive', 0, 'datarecords', 1}
    if sqlquery:
        cursor.execute(sqlquery)
        sqlnames = [desc[0] for desc in cursor.description]

        # retrieve the records from the database
        data = cursor.fetchall()
	finaldata = []
	for item in data:
	    finalitem = []
	    for i, thisname in enumerate(sqlnames):
		value = item[i]
	    #for value in item:
	        if value in engdata:
		    value = value.encode('UTF-8')
		    value = engdata[value]
		if thisname not in forbidden:
		    finalitem.append(value)
	    finaldata.append(finalitem)
	jsondata = json_generator(cursor, 'data', finaldata)
	return Response(jsondata,  mimetype='application/json; charset=utf-8')

    return str('{}')

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

#	return str(engdata)
	for key in qinput:
	    if key not in forbidden:
	        value = qinput[key]
                if sqlfields:
                    sqlfields = "%s, %s" % (sqlfields, key)
                    sqlkeys = "%s, %s" % (sqlkeys, key)
                else:
                    sqlfields = key
                    sqlkeys = key

        sql = "select sum(cast(value as double precision)) as value, value_unit, territory, year, histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10, %s from russianrepository where 1=1" % sqlfields
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
	regions = {}
        class inchain(object):
    	    def __init__(self,name):
                self.name = name

	hclasses = {}
	for row in data:
	    lineitem = {}
	    dataitem = {}
            for i in range(0, len(sqlnames)):
                if row[i] != None:
                    value = row[i]
	            dataitem[sqlnames[i]] = value

	    if dataitem['value'] != None:
		location = dataitem['territory']
		if location in engdata:
		    location = engdata[location]['class_eng']

		if location in regions:
		    regions[location]+=dataitem['value']
		else:
		    regions[location] = dataitem['value']

	    for i in range(0, len(sqlnames)):
		if row[i] != None:
		    value = row[i]
                    if value in engdata:
                        value = engdata[value]['class_eng']

		    if sqlnames[i] == 'value':
			if float(value).is_integer():
			    value = int(value)
	            lineitem[sqlnames[i]] = value 
            try:
                total+=float(lineitem['value'])
            except:
                itotal = 'NA'

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
	final['url'] = 'http://data.sandbox.socialhistoryservices.org/service/download?id=1144&filetype=excel'
	final['total'] = total
	final['regions'] = regions
	final['data'] = result

        return Response(json.dumps(final),  mimetype='application/json; charset=utf-8')

def loadjson(apiurl):
    jsondataurl = apiurl

    req = urllib2.Request(jsondataurl)
    opener = urllib2.build_opener()
    f = opener.open(req)
    dataframe = simplejson.load(f)
    return dataframe

@app.route('/download')
def download():
    clioinfra = Configuration()

    if request.args.get('id'):
        host = "datasets.socialhistory.org"
        url = "https://%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, request.args.get('id'), clioinfra.config['ristatkey'])
        f = urllib2.urlopen(url)
        pdfdata = f.read()
	filetype = "application/pdf"
	if request.args.get('filetype') == 'excel':
	    filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return Response(pdfdata, mimetype=filetype)
    if request.args.get('key'):
        clientcache = MongoClient()
	datafilter = {}
	datafilter['key'] = request.args.get('key')
	(lexicon, regions) = preprocessor(datafilter)
	fullpath = "/home/dpe/tmp/data/%s.xlsx" % request.args.get('key')
        filename = aggregate_dataset(fullpath, lexicon, regions)
	with open(filename, 'rb') as f:
            datacontents = f.read()
        filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return Response(datacontents, mimetype=filetype)

        dbcache = clientcache.get_database('datacache')
	result = dbcache.data.find({"key": str(request.args.get('key')) })
	for item in result:
	    del item['key']
	    del item['_id']
	    dataset = json.dumps(item, encoding="utf8", ensure_ascii=False, sort_keys=True, indent=4)
	    return Response(dataset,  mimetype='application/json; charset=utf-8')
    else:
	return 'Not found'

@app.route('/documentation')
def documentation():
    cursor = connect()
    clioinfra = Configuration()
    host = "datasets.socialhistory.org"
    connection = Connection(host, clioinfra.config['ristatkey'])
    dataverse = connection.get_dataverse('RISTAT')
    settings = DataFilter(request.args)
    papers = []
    for item in dataverse.get_contents():
        handle = str(item['protocol']) + ':' + str(item['authority']) + "/" + str(item['identifier'])
	if handle == clioinfra.config['ristatdocs']:
            datasetid = item['id']
            url = "https://" + str(host) + "/api/datasets/" + str(datasetid) + "/?&key=" + str(clioinfra.config['ristatkey'])
	    dataframe = loadjson(url)
	    for files in dataframe['data']['latestVersion']['files']:
		paperitem = {}
		paperitem['id'] = str(files['datafile']['id'])
	        paperitem['name'] = str(files['datafile']['name'])
		paperitem['url'] = "http://data.sandbox.socialhistoryservices.org/service/download?id=%s" % paperitem['id']
		try:
		    if 'lang' in settings.datafilter:
		        varpat = r"(_%s)" % (settings.datafilter['lang'])
                        pattern = re.compile(varpat, re.IGNORECASE)
                        found = pattern.findall(paperitem['name'])
                        if found:
                            papers.append(paperitem)

		    if 'topic' in settings.datafilter:
		        varpat = r"(_%s_.+_\d+_+%s.|class|region)" % (settings.datafilter['topic'], settings.datafilter['lang'])
		        pattern = re.compile(varpat, re.IGNORECASE)
		        found = pattern.findall(paperitem['name'])
		        if found:
			    papers.append(paperitem)
		    else:
		 	if 'lang' not in settings.datafilter: 
			    papers.append(paperitem)
		except:
		    if 'lang' not in settings.datafilter:
	                papers.append(paperitem)

    return Response(json.dumps(papers),  mimetype='application/json; charset=utf-8')

@app.route('/classes')
def classes():
    cursor = connect()
    #data = load_classes(cursor)
    data = load_vocabulary('modern')
    return Response(data,  mimetype='application/json; charset=utf-8')

@app.route('/years')
def years():
    cursor = connect()
    settings = DataFilter(request.args)
    datatype = ''
    if 'datatype' in settings.datafilter:
	datatype = settings.datafilter['datatype']
    data = load_years(cursor, datatype)
    return Response(data,  mimetype='application/json; charset=utf-8')

# REGIONS
@app.route('/regions')
def regions():
    cursor = connect()
    #data = load_regions(cursor)
    data = load_vocabulary("ERRHS_Vocabulary_regions")
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
