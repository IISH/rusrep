# -*- coding: utf-8 -*-

"""
VT-07-Jul-2016 latest change by VT
FL-12-Dec-2016 use datatype in function documentation()
FL-20-Jan-2017 utf8 encoding
FL-12-Jun-2017 

TODO
- cleanup_downloads()   read timeout from config
- documentation()       read host from config

def connect():
def classcollector( keywords ):
def json_generator( cursor, json_dataname, data, download_key = None ):
def json_cache( json_list, language, json_dataname, download_key, qinput = {} ):
def collect_docs( qinput, download_dir, download_key ):
def translated_vocabulary( vocab_filter, classification = None ):
def translatedclasses( cursor, classinfo ):
def load_years( cursor, datatype ):
def sqlfilter( sql ):
def sqlconstructor( sql ):
def topic_counts():
def load_topics():
def datasetfilter( data, sql_names, classification ):
def zap_empty_classes( item ):
def translateitem( item, eng_data ):
def load_vocabulary( vocname ):
def load_data( cursor, year, datatype, region, debug ):
def rdfconvertor( url ):
def get_sql_query( name, value ):
def loadjson( apiurl ):
def filecat_subtopic( cursor, datatype, base_year ):
def process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx ):
def aggregation_1year( qinput, download_key ):
def cleanup_downloads( download_dir ):

@app.route( '/' )                                               def test():
@app.route( "/export" )                                         def export():
@app.route( "/topics" )                                         def topics():
@app.route( "/filecatalog", methods = [ 'POST', 'GET' ] )       def filecatalog():
@app.route( "/filecatalogdata", methods = [ 'POST', 'GET' ]  )  def filecatalogdata():
@app.route( "/filecatalogget", methods = [ 'POST', 'GET' ] )    def filecatalogget():
@app.route( "/vocab" )                                          def vocab():
@app.route( "/aggregation", methods = ["POST", "GET" ] )        def aggregation():
@app.route( "/indicators", methods = [ "POST", "GET" ] )        def indicators():
@app.route( "/download" )                                       def download():
@app.route( "/documentation" )                                  def documentation():
@app.route( "/histclasses" )                                    def histclasses():
@app.route( "/classes" )                                        def classes():
@app.route( "/years" )                                          def years():
@app.route( "/regions" )                                        def regions():
@app.route( "/translate" )                                      def translate():
@app.route( "/filter", methods = [ "POST", "GET" ] )            def login( settings = '' ):
@app.route( "/maps" )                                           def maps():
"""

from __future__ import absolute_import      # VT
"""
# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )
"""

import sys
reload( sys )
sys.setdefaultencoding( "utf8" )

import collections
import ConfigParser
import csv
import datetime
import json
import logging
import os
import pandas as pd
import random
import re
import shutil
import simplejson
import time
import urllib
import urllib2
import psycopg2
import psycopg2.extras
import zipfile

from datetime import date
from io import BytesIO
from flask import Flask, jsonify, Response, request, send_from_directory, send_file
from jsonmerge import merge
from pymongo import MongoClient
from socket import gethostname
from StringIO import StringIO
from sys import exc_info
from rdflib import Graph, Literal, term

from dataverse import Connection
from excelmaster import aggregate_dataset, preprocessor
from ristatcore.configutils import Configuration, DataFilter

sys.path.insert( 0, os.path.abspath( os.path.join( os.path.dirname( "__file__" ), './' ) ) )

forbidden = [ "classification", "action", "language", "path" ]


def connect():
    logging.debug( "connect()" )
    configparser = ConfigParser.RawConfigParser()
    
    RUSSIANREPO_CONFIG_PATH = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]
    logging.info( "RUSSIANREPO_CONFIG_PATH: %s" % RUSSIANREPO_CONFIG_PATH )
    
    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    logging.info( "using configuration: %s" % configpath )
    
    configparser.read( configpath )
    logging.debug( "configpath: %s" % configpath )
    host     = configparser.get( 'config', 'dbhost' )
    dbname   = configparser.get( 'config', 'dbname' )
    user     = configparser.get( 'config', 'dblogin' )
    password = configparser.get( 'config', 'dbpassword' )
    logging.debug( "host:       %s" % host )
    logging.debug( "dbname:     %s" % dbname )
    logging.debug( "user:       %s" % user )
    #logging.debug( "password:   %s" % password )
    
    connection_string = "host='%s' dbname='%s' user='%s' password='%s'" % ( host, dbname, user, password )
    
    # get a connection, if a connect cannot be made an exception will be raised here
    connection = psycopg2.connect( connection_string )
    
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = connection.cursor()
    
    return cursor



def classcollector( keywords ):
    logging.debug( "classcollector()" )
    logging.debug( "keywords: %s" % keywords )
    
    classdict  = {}
    normaldict = {}
    
    for item in keywords:
        logging.debug( "item: %s" % item )
        classmatch = re.search( r'class', item )
        if classmatch:
            logging.debug( "class: %s" % item )
            classdict[ item ] = keywords[ item ]
        else:
            normaldict[ item ] = keywords[ item ]
    
    logging.debug( "classdict:  %s" % classdict )
    logging.debug( "normaldict: %s" % normaldict )
    return ( classdict, normaldict )



def json_generator( cursor, json_dataname, data, download_key = None ):
    logging.debug( "json_generator() cursor: %s, json_dataname: %s" % ( cursor, json_dataname ) )
    logging.debug( "data: %s" % data )
    
    classification = "unknown"
    language       = "EN"
    datatype       = ""
    datatype_      = "0_00"
    base_year      = ""
    path_list      = []
    
    try:
        qinput = json.loads( request.data )
        logging.debug( "# of keys: %d" % len( qinput ) )
        for k in qinput:
            logging.debug( "k: %s, v: %s" % ( k, qinput[ k ] ) )
        
        classification = qinput.get( "classification" )
        language       = qinput.get( "language" )
        datatype       = qinput.get( "datatype" )
        datatype_      = datatype[ 0 ] + "_00"
        base_year      = qinput.get( "base_year" )
        path_list      = qinput.get( "path" )
        
        logging.debug( "classification : %s" % classification )
        logging.debug( "language       : %s" % language )
        logging.debug( "datatype       : %s" % datatype )
        logging.debug( "base_year      : %s" % base_year )
        logging.debug( "path_list      : %s" % path_list )
    except:
        pass

    logging.debug( "# entries in path_list: %d" % len( path_list ) )
    for path_entry in path_list:
        logging.debug( path_entry )

    sql_names  = [ desc[ 0 ] for desc in cursor.description ]
    forbidden = { 'data_active', 0, 'datarecords', 1 }
    
    json_list = []
    
    logging.debug( "# values in data: %d" % len( data ) )
    for value_str in data:
        logging.debug( "value_str: %s" % str( value_str ) )
        data_keys   = {}
        extravalues = {}
        for i in range( len( value_str ) ):
            name  = sql_names[ i ]
            value = value_str[ i ]
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, thisname, value ) )
                # ". " marks a trailing dot in histclass or class: skip
                continue
            else:
                try:
                    num_value = float( value )
                    if num_value < 0.0:
                        logging.debug( "negative value: %f" % num_value )
                        logging.debug( "in value_str: %s" % value_str )
                except:
                    pass
            
            if name not in forbidden:
                data_keys[ name ] = value
            else:
                extravalues[ name ] = value

        # If aggregation check data output for 'NA' values
        if 'total' in data_keys:
            if extravalues[ 'data_active' ]:
                if language == "en":
                    data_keys[ 'total' ] = 'NA'
                elif language == "rus":
                    data_keys = "непригодный"
        
        ( path, output ) = classcollector( data_keys )
        output[ 'path' ] = path
        json_list.append( output )
    
    value_unit = ''
    logging.debug( "# of entries in json_list: %d" % len( json_list ) )
    for json_entry in json_list:
        logging.debug( "json_entry: %s" % json_entry )
        value_unit = json_entry.get( "value_unit" )
        entry_path = json_entry.get( "path" )
        logging.debug( "entry_path: %s" % entry_path )
        try:
            path_list.remove( entry_path )
        except:
            pass
    
    if len( path_list ) != 0:
        # pure '.' dot entries are not returned from db
        logging.debug( "missing path entries: %d" % len( path_list ) )
        for path_entry in path_list:
            logging.debug( path_entry )
            new_entry = {}
            # also want to see 'NA" entries in preview and download
            new_entry[ "path" ]       = path_entry
            new_entry[ "base_year" ]  = base_year
            new_entry[ "value_unit" ] = value_unit
            new_entry[ "datatype" ]   = datatype
            new_entry[ "count" ]      = ''
            new_entry[ "ter_code" ]   = ''
            json_list.append( new_entry )
    
    return json_list



def json_cache( json_list, language, json_dataname, download_key, qinput = {} ):
    # cache json_list in mongodb with download_key as key
    logging.debug( "json_cache() # entries in list: %d" %  len( json_list ) )
    
    configparser = ConfigParser.RawConfigParser()
    
    RUSSIANREPO_CONFIG_PATH = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]
    logging.info( "RUSSIANREPO_CONFIG_PATH: %s" % RUSSIANREPO_CONFIG_PATH )
    
    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    configparser.read( configpath )
    
    json_hash = {}
    json_hash[ "language" ] = language
    json_hash[ json_dataname ] = json_list
    
    json_hash[ "url" ] = "%s/service/download?key=%s" % ( configparser.get( 'config', 'root' ), download_key )
    logging.debug( "json_hash: %s" % json_hash )
    
    json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    
    try:
        this_data = json_hash
        del this_data[ 'url' ]
        this_data[ 'key' ] = download_key
        this_data[ 'language' ] = language
        if qinput is not None:
            this_data[ 'qinput' ] = qinput
        
        logging.debug( "dbcache.data.insert with key: %s" % download_key )
        clientcache = MongoClient()
        dbcache = clientcache.get_database( 'datacache' )
        result = dbcache.data.insert( this_data )
    except:
        logging.error( "caching failed" )

    return json_string



def collect_docs( qinput, download_dir, download_key ):
    # collect the accompanying docs in the download dir
    logging.debug( "collect_docs() %s" % download_key )
    
    for key in qinput:
        logging.debug( "key: %s, value: %s" % ( key, qinput[ key ] ) )
        
    classification = qinput.get( "classification" )
    language       = qinput.get( "language" )
    datatype       = qinput.get( "datatype" )
    
    if language is None:
        language = "en"
    
    logging.debug( "classification: %s, language: %s, datatype: %s" % ( classification, language, datatype ) )
    
    if datatype is not None:
        datatype0 = datatype[ 0 ]
        datatype_ = datatype0 + "_00"
    else:
        datatype0 = ""
        datatype_ = ""
    
    logging.debug( "datatype0: %s, datatype_: %s" % ( datatype0, datatype_ ) )
    
    clioinfra = Configuration()
    tmp_dir = clioinfra.config[ 'tmppath' ]
    
    doc_dir = os.path.join( tmp_dir, "dataverse", "doc", "hdl_documentation" )
    doc_list = os.listdir( doc_dir )
    doc_list.sort()
    
    get_list = []   # docs for zipping
    for doc in doc_list:
        if doc.find( language.upper() ) != -1:                  # language
            if doc.find( "Introduction" ) != -1 or \
               doc.find( "regions" ) != -1:                     # string
                get_list.append( doc )
            
            if classification == "historical":                  # only base_year docs
                if doc.find( "NACE 1.1_Classification") != -1 and datatype0 in [ "3", "4", "5"]:
                    get_list.append( doc )
                
                base_year = qinput.get( "base_year" )
                if doc.find( str( base_year ) ) != -1:          # base_year
                    if doc.find( "GovReports" ) != -1:          # string
                        get_list.append( doc )
                    
                    if doc.find( datatype_ ) != -1:             # datatype
                        get_list.append( doc )
            
            elif classification == "modern":                    # modern: most lang docs
                if doc.find( "NACE 1.1_Classification") != -1 and datatype0 in [ "3", "4", "5"]:
                    get_list.append( doc )
                
                if doc.find( "GovReports" ) != -1:              # string
                    get_list.append( doc )
                
                if doc.find( datatype_ ) != -1:                 # datatype
                    get_list.append( doc )
            
            elif classification == "data":                      # filecatalog data
                subtopics = qinput.get( "subtopics" )
                for subtopic in subtopics:
                    datatype  = subtopic[ :4 ]
                    datatype0 = datatype[ 0 ]
                    datatype_ = datatype0 + "_00"
                    base_year = subtopic[ 5: ]
                    
                    if doc.find( "NACE 1.1_Classification") != -1 and datatype0 in [ "3", "4", "5"]: 
                        if doc not in get_list:                 # add only once
                            get_list.append( doc )
                    
                    if doc.find( "Modern_Classification") != -1 and doc.find( datatype_ ) != -1:
                        if doc not in get_list:                 # add only once
                            get_list.append( doc )
                    
                    if doc.find( "GovReports" ) != -1 and doc.find( str( base_year ) ) != -1:
                        if doc not in get_list:                 # add only once
                            get_list.append( doc )
                    
                    # files that must match both datatype & base_year
                    if doc.find( str( datatype_ ) ) != -1 and doc.find( str( base_year ) ) != -1:
                        if doc not in get_list:                 # add only once
                            get_list.append( doc )
        
    for doc in get_list:
        doc_path = os.path.join( doc_dir, doc )
        shutil.copy2( doc_path, download_dir )



def translated_vocabulary( vocab_filter, classification = None ):
    logging.debug( "translated_vocabulary()" )
    logging.debug( "vocab_filter: %s" % str( vocab_filter ) )
    
    client = MongoClient()
    dbname = 'vocabulary'
    db = client.get_database( dbname )
    
    if classification == "modern":
        del vocab_filter[ "YEAR" ]
    
    if vocab_filter:
        vocab = db.data.find( vocab_filter )
    else:
        vocab = db.data.find()
    
    data     = {}
    histdata = {}
    for item in vocab:
        if 'RUS' in item:
            try:
                item[ 'RUS' ] = item[ 'RUS' ].encode( 'UTF-8' )
                item[ 'EN' ]  = item[ 'EN' ] .encode( 'UTF-8' )
                
                if item[ 'RUS' ].startswith( '"' ) and item[ 'RUS' ].endswith( '"' ):
                    item[ 'RUS' ] = string[ 1:-1 ]
                if item[ 'EN' ].startswith( '"' ) and item[ 'EN' ].endswith( '"' ):
                    item[ 'EN' ] = string[ 1:-1 ]
                
                item[ 'RUS' ] = re.sub( r'"', '', item[ 'RUS' ] )
                item[ 'EN' ]  = re.sub( r'"', '', item[ 'EN' ] )
                
                data[ item[ 'RUS' ] ] = item[ 'EN' ]
                data[ item[ 'EN' ] ]  = item[ 'RUS' ] 
                
                logging.debug( "EN: %s RUS: %s" % ( item[ 'EN' ], item[ 'RUS' ] ) )
            except:
                skip = 1
    
    return data



def translatedclasses( cursor, classinfo ):
    logging.debug( "translatedclasses()" )
    dictdata = {}
    sql = "select * from datasets.classmaps"; # where class_rus in ";
    sqlclass = ''
    for classname in classinfo:
        if sqlclass:
            sqlclass = "%s, '%s'" % ( sqlclass, classinfo[ classname ] )
        else:
            sqlclass = "'%s'" % classinfo[ classname ]
    sql = "%s (%s)" % ( sql, sqlclass )

    sql = "select * from datasets.regions"
    cursor.execute( sql )
    data = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    if data:
        for value_str in data:
            data_keys = {}
            for i in range( len( value_str ) ):
                name  = sql_names[ i ]
                value = value_str[ i ]
                if name == 'region_name':
                    name = 'class_rus'
                if name == 'region_name_eng':
                    name = 'class_eng'
                data_keys[ name ] = value

            dictdata[ data_keys[ 'class_eng' ] ] = data_keys
            dictdata[ data_keys[ 'class_rus' ] ] = data_keys
        
    sql = "select * from datasets.valueunits";
    cursor.execute( sql )
    data = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    if data:
        for value_str in data:
            data_keys = {}
            for i in range( len( value_str ) ):
                name  = sql_names[ i ]
                value = value_str[ i ]
                data_keys[name] = value
            dictdata[ data_keys[ 'class_rus' ] ] = data_keys
            dictdata[ data_keys[ 'class_eng' ] ] = data_keys

    # FIX
    sql = "select * from datasets.classmaps"
    cursor.execute( sql )
    data = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    if data:
        for value_str in data:
            data_keys = {}
            for i in range( len( value_str ) ):
               name  = sql_names[ i ]
               value = value_str[ i ]
               data_keys[name] = value
            dictdata[ data_keys[ 'class_rus' ] ] = data_keys
            dictdata[ data_keys[ 'class_eng' ] ] = data_keys

    return dictdata



def load_years( cursor, datatype ):
    logging.debug( "load_years()" )
    clioinfra = Configuration()
    years = clioinfra.config[ 'years' ].split( ',' )
    data = {}
    sql = "select base_year, count(*) as c from russianrepository where 1=1"
    if datatype:
        sql=sql + " and datatype='%s'" % datatype 
    sql= sql + " group by base_year";
    cursor.execute( sql )
    
    # retrieve the records from the database
    data = cursor.fetchall()
    result = {}
    for val in data:
        if val[ 0 ]:
            result[ val[ 0 ] ] = val[ 1 ]
    for year in years:
        if int( year ) not in result:
            result[ int( year ) ] = 0
    
    json_string = json.dumps( result, encoding = "utf-8" )

    return json_string



def sqlfilter( sql ):
    logging.debug( "sqlfilter()" )
    items     = ''
    sqlparams = ''

    for key, value in request.args.items():
        items = request.args.get( key, '' )
        itemlist = items.split( "," )
        if key == 'basisyear':
            sql += " AND %s LIKE '%s" % ( 'region_code', itemlist[ 0 ] )
            sql += "%'"
        else:
            for item in itemlist:
                sqlparams = "\'%s\',%s" % ( item, sqlparams )
            sqlparams = sqlparams[ :-1 ]
            sql += " AND %s in (%s)" % ( key, sqlparams )
    return sql



def sqlconstructor( sql ):
    logging.debug( "sqlconstructor()" )
    items     = ''
    sqlparams = ''

    for key, value in request.args.items():
        items = request.args.get( key, '' )
        itemlist = items.split( "," )
        if key == 'language':
            skip = 1
        elif key == 'classification':
            skip = 1
        elif key == 'basisyear':
            sql += " AND %s like '%s'" % ( 'region_code', sqlparams )
        else:
            for item in itemlist:
                sqlparams = "\'%s\'" % item
            sql += " AND %s in (%s)" % ( key, sqlparams )
    return sql



def topic_counts():
    logging.info( "topic_counts()" )
    
    configpath = RUSSIANREPO_CONFIG_PATH = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]
    
    if not os.path.isfile( configpath ):
        logging.error( "in %s" % __file__ )
        logging.error( "configpath %s FILE DOES NOT EXIST" % configpath )
        logging.error( "EXIT" )
        sys.exit( 1 )
    
    logging.info( "using configuration: %s" % configpath )

    configparser = ConfigParser.RawConfigParser()
    configparser.read( configpath )
    
    host     = configparser.get( 'config', 'dbhost' )
    dbname   = configparser.get( 'config', 'dbname' )
    user     = configparser.get( 'config', 'dblogin' )
    password = configparser.get( 'config', 'dbpassword' )
    
    connection_string = "host = '%s' dbname = '%s' user = '%s' password = '%s'" % ( host, dbname, user, password )
    logging.info( "connection_string: %s" % connection_string )

    connection = psycopg2.connect( connection_string )
    cursor = connection.cursor( cursor_factory = psycopg2.extras.NamedTupleCursor )

    sql_topics  = "SELECT datatype, topic_name FROM datasets.topics"
    sql_topics += " ORDER BY datatype"
    logging.info( sql_topics )
    cursor.execute( sql_topics )
    resp = cursor.fetchall()
    
    #skip_list = [ "1", "2", "3", "4", "5", "6", "7" ]
    skip_list = []
    all_cnt_dict = {}
    for record in resp:
        datatype   = record.datatype
        topic_name = record.topic_name
        if datatype not in skip_list:
            #print( datatype, topic_name )
            sql_count  = "SELECT base_year, COUNT(*) AS count FROM russianrepository"
            sql_count += " WHERE datatype = '%s'" % datatype
            sql_count += " GROUP BY base_year ORDER BY base_year"
            logging.debug( sql_count )
            
            cursor.execute( sql_count )
            cnt_resp = cursor.fetchall()
            cnt_dict = {}
            for cnt_rec in cnt_resp:
                #print( cnt_rec )
                cnt_dict[ cnt_rec.base_year ] = int( cnt_rec.count )    # strip trailing 'L'
            
            #print( cnt_dict )
            all_cnt_dict[ datatype ] = cnt_dict
            logging.debug( "datatype: %s , topic_name: %s, counts: %s" % ( datatype, topic_name, str( cnt_dict ) ) )
        else:
            #print( "skip:", datatype, topic_name )
            pass
    
    #connection.commit()     # SELECT does not change anything
    cursor.close()
    connection.close()

    return all_cnt_dict



def load_topics():
    logging.debug( "load_topics()" )
    
    all_cnt_dict = topic_counts()
    
    sql = "SELECT * FROM datasets.topics"
    sql = sqlfilter( sql ) 
    logging.debug( "sql: %s" % sql )
    
    cursor = connect()
    cursor.execute( sql )       # execute

    data = cursor.fetchall()    # retrieve the records from the database
    json_list_in = json_generator( cursor, "data", data )
    json_list_out = []
    for topic_dict in json_list_in:
        logging.debug( topic_dict )
        datatype = topic_dict[ "datatype" ]
        topic_dict[ "byear_counts" ] = all_cnt_dict[ datatype ]
        json_list_out.append(topic_dict )
    
    return json_list_out



def datasetfilter( data, sql_names, classification ):
    logging.debug( "datasetfilter()" )
    if data:
        # retrieve the records from the database
        datafilter = []
        for dataline in data:
            datarow = {}
            active  = ''
            for i in range( len( sql_names ) ):
                name = sql_names[ i ]
                if classification == 'historical':
                    if name.find( "class", 0 ):
                        try:
                            nextvalue = dataline[ i+1 ]
                        except:
                            nextvalue = '.'
                        
                        if ( dataline[ i ] == '.' and nextvalue == '.' ):
                            skip = 'yes'
                        else:
                            toplevel = re.search( "(\d+)", name )
                            if name.find( "histclass10", 0 ):
                                datarow[ name ] = dataline[ i ]
                                if toplevel:
                                    datarow[ "levels" ] = toplevel.group( 0 )
                if classification == 'modern':
                    if name.find( "histclass", 0 ):
                        try:
                            nextvalue = dataline[ i+1 ]
                        except:
                            nextvalue = '.'
                        
                        if ( dataline[i] == '.' and nextvalue == '.' ):
                            skip = 'yes'
                        else:
                            toplevel = re.search( "(\d+)", name )
                            if name.find( "class10", 0 ):
                                datarow[ name ] = dataline[ i ]
                                if toplevel:
                                    if toplevel.group( 0 ) != '10':
                                        datarow[ "levels" ] = toplevel.group( 0 )
            try:
                if datarow[ "levels" ] > 0:
                    datafilter.append( datarow )
            except:
                skip = 'yes'

        if classification:
            #return datafilter
            return json.dumps( datafilter, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )



def zap_empty_classes( item ):
    logging.debug( "zap_empty_classes()" )
    # trailing empty classes have value ". ", skip them; 
    # bridging empty classes have value '.', keep them; 
    
    new_item = {}
    for name in item:
        value = item[ name ].encode( 'UTF-8' )
        # skip trailing ". " in hist & modern classes
        if ( name.startswith( "histclass" ) or name.startswith( "class" ) ) and value == ". ":
            #logging.debug( "name: %s, value: %s" % ( name, value ) )
            #value = ""
            pass
        else:
            new_item[ name ] = value
    
    return new_item



def translateitem( item, eng_data ):
    logging.debug( "translateitem()" )
    logging.debug( item )
    logging.debug( eng_data )
    
    # Translate first
    newitem = {}
    if eng_data:
        for name in item:
            value = item[ name ].encode( 'UTF-8' )
            if value in eng_data:
                value = eng_data[ value ]
            newitem[ name ] = value
        item = newitem
    
    return item



def load_vocabulary( vocname ):
    logging.debug( "load_vocabulary() vocname: %s" % vocname )
    logging.debug( "request.args: %s" % str( request.args ) )
    
    client = MongoClient()
    dbname = 'vocabulary'
    db = client.get_database( dbname )
    newfilter = {}
    eng_data = {}
    
    if request.args.get( 'classification' ):
        vocname = request.args.get( 'classification' )
        if vocname == 'historical':
            newfilter[ 'vocabulary' ] = 'ERRHS_Vocabulary_histclasses'
        else:
            newfilter[ 'vocabulary' ] = 'ERRHS_Vocabulary_modclasses'
        logging.debug( "newfilter: %s" % newfilter )

    if request.args.get( 'language' ) == 'en':
        thisyear = ''
        vocab_filter = {}
        if request.args.get( 'base_year' ):
            if vocname == 'historical':
                base_year = request.args.get( "base_year" )
                if base_year:
                    vocab_filter[ "YEAR" ] = base_year
                datatype = request.args.get( "datatype" )
                if datatype:
                    vocab_filter[ "DATATYPE" ] = datatype
                
        eng_data = translated_vocabulary( vocab_filter )
        units    = translated_vocabulary( { "vocabulary": "ERRHS_Vocabulary_units" } )
        for item in units:
            eng_data[ item ] = units[ item ]

    params = { "vocabulary": vocname }
    for name in request.args:
        if name not in forbidden:
            params[ name ] = request.args.get( name )

    if vocname:
        vocab = db.data.find( params )
    else:
        vocab = db.data.find()

    data = []
    uid = 0
    logging.debug( "processing %d items in vocab %s" % ( vocab.count(), vocname ) )
    for item in vocab:
        del item[ '_id' ]
        del item[ 'vocabulary' ]
        regions = {}
        
        if vocname == "ERRHS_Vocabulary_regions":
            uid += 1
            regions[ 'region_name' ] = item[ 'RUS' ]
            regions[ 'region_name_eng' ] = item[ 'EN' ]
            regions[ 'region_code' ] = item[ 'ID' ]
            regions[ 'region_id' ] = uid
            regions[ 'region_ord' ] = 189702
            regions[ 'description' ] = regions[ 'region_name' ]
            regions[ 'active' ] = 1
            item = regions
            data.append( item )
        elif vocname == 'modern':
            item = zap_empty_classes( item )
            if eng_data:
                item = translateitem( item, eng_data )
            data.append( item )
        elif vocname == 'historical':
            item = zap_empty_classes( item )
            if eng_data:
                item = translateitem( item, eng_data )
            data.append( item )
        else:
            # Translate first
            newitem = {}
            if eng_data:
                for name in item:
                    value = item[ name ]
                    if value in eng_data: 
                        value = eng_data[ value ]
                    newitem[ name ] = value
                item = newitem
            
            ( path, output ) = classcollector( item )
            if path:
                output[ 'path' ] = path
                data.append( output )
            else:
                data.append( item )

    json_hash = {}
    if vocname == "ERRHS_Vocabulary_regions":
        json_hash[ "regions" ] = data
    elif vocname == "modern":
        json_hash = data
    elif vocname == "historical":
        json_hash = data
    else:
        json_hash[ "data" ] = data
        json_data = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
        return json_data

    return json_hash



def load_data( cursor, year, datatype, region, debug ):
    logging.debug("load_data()")
    data = {}

    query = "select * from russianrepository WHERE 1 = 1 "
    query = sqlfilter( query )
    if debug:
        print( "DEBUG " + query + " <br>\n" )
    query += ' order by territory asc'
    
    cursor.execute( query )
    records = cursor.fetchall()
    
    row_count = 0
    i = 0
    for row in records:
        i = i + 1
        data[ i ] = row
    
    json_list = json_generator( cursor, "data", records )
    
    return json_list



def rdfconvertor( url ):
    logging.debug( "rdfconvertor()" )
    f = urllib.urlopen( url )
    data = f.read()
    csvio = StringIO( str( data ) )
    dataframe = pd.read_csv( csvio, sep = '\t', dtype = 'unicode' )
    finalsubset = dataframe
    columns = finalsubset.columns
    rdf = "@prefix ristat: <http://ristat.org/api/vocabulary#> .\n"
    #vocab_uri = "http://ristat.org/service/vocab#"
    vocab_uri = "http://data.sandbox.socialhistoryservices.org/service/vocab#"
    g = Graph()

    for ids in finalsubset.index:
        item = finalsubset.ix[ ids ]
        uri = term.URIRef( "%s%s" % ( vocab_uri, str( item[ 'ID' ] ) ) )
        if uri:
            for col in columns:
                if col is not 'ID':
                    if item[ col ]:
                        c = term.URIRef( col )
                        g.add( ( uri, c, Literal( str( item[ col ] ) ) ) )
                        rdf += "ristat:%s " % item[ 'ID' ]
                        rdf += "ristat:%s ristat:%s." % ( col, item[ col ] )
                    rdf += "\n"
    return g



def get_sql_query( name, value ):
    logging.debug( "get_sql_query() name: %s, value: %s" % ( name, value ) )
    
    sql_query = ''
    result = re.match( "\[(.+)\]", value )
    if result:
        query = result.group( 1 )
        ids = query.split( ',' )
        for param in ids:
            param=re.sub( "u'", "'", str( param ) )
            sql_query += "%s," % param
        if sql_query:
            sql_query = sql_query[ :-1 ]
            sql_query = "%s in (%s)" % ( name, sql_query )
    else:
        sql_query = "%s = '%s'" % ( name, value )
    
    logging.debug( "sql_query: %s" % sql_query )
    return sql_query



def loadjson( apiurl ):
    logging.debug( "loadjson()" )
    json_dataurl = apiurl

    req = urllib2.Request( json_dataurl )
    opener = urllib2.build_opener()
    f = opener.open( req )
    dataframe = simplejson.load( f )
    return dataframe


def filecat_subtopic( cursor, datatype, base_year ):
    logging.debug( "filecatalog_subtopic()" )
    
    query  = "SELECT * FROM russianrepository "
    query += "WHERE datatype = '%s' AND base_year = '%s'" % ( datatype, base_year )
    
    cursor.execute( query )
    records = cursor.fetchall()
    
    json_list = json_generator( cursor, "data", records )
    logging.debug( json_list )
    
    return json_list



def process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx ):
    logging.debug( "process_csv() %s" % csv_filename )
    csv_pathname = os.path.join( csv_dir, csv_filename )

    # dataverse column names
    dv_column_names = [
        "id", 
        "territory", 
        "ter_code", 
        "town", 
        "district", 
        "year", 
        "month", 
        "value", 
        "value_unit", 
        "value_label", 
        "datatype", 
        "histclass1", 
        "histclass2", 
        "histclass3", 
        "histclass4", 
        "histclass5", 
        "histclass6", 
        "histclass7", 
        "histclass8", 
        "histclass9", 
        "histclass10", 
        "class1", 
        "class2", 
        "class3", 
        "class4", 
        "class5", 
        "class6", 
        "class7", 
        "class8", 
        "class9", 
        "class10", 
        "comment_source", 
        "source", 
        "volume", 
        "page", 
        "naborshik_id", 
        "comment_naborshik", 
        "base_year"
    ]
    
    
    with open( csv_pathname, 'rb' ) as csv_file:
        csv_reader = csv.reader( csv_file, delimiter = '|' )
        for row in csv_reader:
            logging.debug( ', '.join( row ) )
    
    if to_xlsx:
        sep = str( u'|' ).encode( "utf-8" )
        kwargs_pandas = { 
            "sep" : sep 
            #,"line_terminator" : '\n'   # TypeError: parser_f() got an unexpected keyword argument 'line_terminator'
        }
        
        
        df1 = pd.read_csv( csv_pathname, **kwargs_pandas )
        root, ext = os.path.splitext( csv_filename )
        xlsx_filename = root + ".xlsx"
        xlsx_pathname = os.path.join( download_dir, xlsx_filename )
        
        writer = pd.ExcelWriter( xlsx_pathname  )
        df1.to_excel( writer, "Table", encoding = "utf-8", index = False )
        
        # Create a Pandas dataframe from the data.
        if language == "en":
            df2 = pd.DataFrame( { " ": [ 
                "",
                "",
                "Creative Commons License", 
                "This work is licensed under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License.", 
                "http://creativecommons.org/licenses/by-nc-sa/4.0/", 
                "", 
                "By downloading and using data from the Electronic Repository of Russian Historical Statistics the user agrees to the terms of this license. Providing a correct reference to the resource is a formal requirement of the license: ", 
                "Kessler, Gijs and Andrei Markevich (%d), Electronic Repository of Russian Historical Statistics, 18th - 21st centuries, http://ristat.org/" % date.today().year, 
            ] } )
            """
            c = ws_cr.cell( row = 4, column = 0 )
            c.value = "Creative Commons License"
            c = ws_cr.cell( row = 5, column = 0 )
            c.value = "This work is licensed under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License."
            c = ws_cr.cell( row = 6, column = 0 )
            c.value = "http://creativecommons.org/licenses/by-nc-sa/4.0/"
            c = ws_cr.cell( row = 8, column = 0 )
            c.value = "By downloading and using data from the Electronic Repository of Russian Historical Statistics the user agrees to the terms of this license. Providing a correct reference to the resource is a formal requirement of the license: "
            c = ws_cr.cell( row = 9, column = 0 )
            c.value = "Kessler, Gijs and Andrei Markevich (%d), Electronic Repository of Russian Historical Statistics, 18th - 21st centuries, http://ristat.org/" % date.today().year
            """
        elif language == "ru":
            df2 = pd.DataFrame( { " ": [ 
                "",
                "",
                "Лицензия Creative Commons", 
                "Это произведение доступно по лицензии Creative Commons «Attribution-NonCommercial-ShareAlike» («Атрибуция — Некоммерческое использование — На тех же условиях») 4.0 Всемирная.", 
                "http://creativecommons.org/licenses/by-nc-sa/4.0/deed.ru", 
                "",
                "Скачивая и начиная использовать данные пользователь автоматически соглашается с этой лицензией. Наличие корректно оформленной ссылки является обязательным требованием лицензии:", 
                "Кесслер Хайс и Маркевич Андрей (%d), Электронный архив Российской исторической статистики, XVIII – XXI вв., [Электронный ресурс] : [сайт]. — Режим доступа: http://ristat.org/" % date.today().year, 
            ] } )
            """
            c = ws_cr.cell( row = 4, column = 0 )
            c.value = "Лицензия Creative Commons"
            c = ws_cr.cell( row = 5, column = 0 )
            c.value = "Это произведение доступно по лицензии Creative Commons «Attribution-NonCommercial-ShareAlike» («Атрибуция — Некоммерческое использование — На тех же условиях») 4.0 Всемирная."
            c = ws_cr.cell( row = 6, column = 0 )
            c.value = "http://creativecommons.org/licenses/by-nc-sa/4.0/deed.ru"
            c = ws_cr.cell( row = 8, column = 0 )
            c.value = "Скачивая и начиная использовать данные пользователь автоматически соглашается с этой лицензией. Наличие корректно оформленной ссылки является обязательным требованием лицензии:"
            c = ws_cr.cell( row = 9, column = 0 )
            c.value = "Кесслер Хайс и Маркевич Андрей (%d), Электронный архив Российской исторической статистики, XVIII – XXI вв., [Электронный ресурс] : [сайт]. — Режим доступа: http://ristat.org/" % date.today().year
            """

        # Convert the dataframe to an XlsxWriter Excel object.
        df2.to_excel( writer, sheet_name = "Copyrights", encoding = "utf-8", index = False )
        
        writer.save()



def aggregation_1year( qinput, download_key ):
    logging.debug( "aggregation_1year() download_key: %s" % download_key )
    logging.debug( "qinput %s" % str( qinput ) )
    
    thisyear = ''
    
    try:
        language = qinput.get( "language" )
        
        logging.debug( "number of keys in request.data: %d" % len( qinput ) )
        for key in qinput:
            value = qinput[ key ]
            if key == "path":
                logging.debug( "path:" )     # debug: ≥ = u"\u2265"
                for pdict in value:
                    logging.debug( str( pdict ) )
                    for pkey in pdict:
                        pvalue = pdict[ pkey ]
                        logging.debug( "key: %s, value: %s" % ( pkey, pvalue ) )
                        if pkey == "classification":
                            classification = pvalue
            else:
                logging.debug( "key: %s, value: %s" % ( key, value ) )
    except:
        type, value, tb = exc_info()
        logging.debug( "failed: %s" % value )
        logging.debug( "no request.data" )
        return '{}'
    
    eng_data = {}
    cursor = connect()
    forbidden = [ "classification", "action", "language", "path" ]
    
    if cursor:
        if qinput.get( 'language' ) == 'en':
            # translate input english term to russian sql terms
            vocab_filter = {}
            classification = qinput.get( "classification" )
            base_year = qinput.get( "base_year" )
            if base_year and classification == "historical":
                vocab_filter[ "YEAR" ] = base_year
            
            datatype = qinput.get( "datatype" )
            if datatype:
                if classification == "historical":
                    vocab_filter[ "DATATYPE" ] = datatype
                elif classification == "modern":
                    vocab_filter[ "DATATYPE" ] = "MOD_" + datatype
            
            eng_data = translated_vocabulary( vocab_filter )
            units = translated_vocabulary( { "vocabulary": "ERRHS_Vocabulary_units" } )
            
            logging.debug( "translated_vocabulary returned %d items" % len( units ) )
            logging.debug( "vocab_filter: %s" % str( vocab_filter ) )
            logging.debug( "eng_data: %s" % str( eng_data ) )
            logging.debug( "units: %s" % str( units ) )
            for item in units:
                eng_data[ item ] = units[ item ]
    
    known_fields = {}
    sql = {}
    sql[ "condition" ] = ''
    sql[ "order_by" ]  = ''
    sql[ "group_by" ]  = ''
    sql[ "where" ]     = ''
    sql[ "internal" ]  = ''
    
    if qinput:
        for name in qinput:
            if not name in forbidden:
                value = str( qinput[ name ] )
                logging.debug( "name: %s, value: %s" % ( name, value ) )
                if value in eng_data:
                    value = eng_data[ value ]
                    logging.debug( "eng_data name: %s, value: %s" % ( name, value ) )
                
                sql[ 'where' ] += "%s AND " % get_sql_query( name, value )
                sql[ 'condition' ] += "%s, " % name
                known_fields[ name ] = value
            
            elif name == 'path':
                full_path = qinput[ name ]
                top_sql = 'AND ('
                for path in full_path:
                    sql_local = {}
                    clear_path = {}
                    
                    for xkey in path:
                        value = path[ xkey ]
                        
                        # need to retain '.' in classes, but not in summing
                        if value == '.':
                            value = 0
                        clear_path[ xkey ] = value
                        
                    for xkey in clear_path:
                        value = path[ xkey ]
                        value = str( value )    # ≥5000 : \xe2\x89\xa55000 => u'\\u22655000
                        # otherwise, it is not found in eng_data
                        logging.debug( "clear_path xkey: %s, value: %s" % ( xkey, value ) )
                        
                        if value in eng_data:
                            logging.debug( "xkey: %s, value: %s" % ( xkey, value ) )
                            value = eng_data[ value ]
                            logging.debug( "xkey: %s, value: %s" % ( xkey, value ) )
                        
                        sql_local[ xkey ] = "(%s='%s' OR %s='. '), " % ( xkey, value, xkey )
                        
                        if not known_fields.has_key( xkey ):
                            known_fields[ xkey ] = value
                            sql[ 'condition' ] += "%s, " % xkey
                    
                    if sql_local:
                        sql[ 'internal' ] += ' ('
                        for key in sql_local:
                            sql_local[ key ] = sql_local[ key ][ :-2 ]
                            sql[ 'internal' ] += "%s AND " % sql_local[ key ]
                            logging.debug( "key: %s, value: %s" % ( key, sql_local[ key ] ) )
                            
                        sql[ 'internal' ] = sql[ 'internal' ][ :-4 ]
                        sql[ 'internal' ] += ') OR'
    
    sql[ 'internal' ] = sql[ 'internal' ][ :-3 ]

    logging.debug( "condition: %s" % str( sql[ "condition" ] ) )
    logging.debug( "order_by:  %s" % str( sql[ "order_by" ]  ) )
    logging.debug( "group_by:  %s" % str( sql[ "group_by" ]  ) )
    logging.debug( "where:     %s" % str( sql[ "where" ]     ) )
    logging.debug( "internal:  %s" % str( sql[ "internal" ]  ) )

    sql_query  = "SELECT COUNT(*) AS datarecords" 
    sql_query += ", SUM(CAST(value AS DOUBLE PRECISION)) AS total"
    sql_query += ", COUNT(*) AS count"
    sql_query += ", COUNT(*) - COUNT(value) AS data_active"
    sql_query += ", value_unit, ter_code"

    classification = qinput[ "classification" ]
    logging.debug( "classification: %s" % classification )

    if sql[ 'where' ]:
        logging.debug( "where: %s" % sql[ "where" ] )
        sql_query += ", %s" % sql[ 'condition' ]
        sql_query  = sql_query[ :-2 ]
        if classification == "historical":
            sql_query += ", histclass7, histclass8, histclass9, histclass10"
        elif classification == "modern":
            sql_query += ", class7, class8, class9, class10"
        
        sql_query += " FROM russianrepository WHERE %s" % sql[ 'where' ]
        sql_query  = sql_query[ :-4 ]
        
    if sql[ 'internal' ]:
        logging.debug( "internal: %s" % sql[ "internal" ] )
        sql_query += " AND (%s) " % sql[ 'internal' ]
    
    sql_query += " AND value <> '.'"            # suppress a 'lone' "optional point", used in the table to flag missing data
    #sql_query += " AND value ~ '^\d+$'"         # regexp (~) to require that value only contains digits
    #sql_query += " AND value ~ '^\d*\.?\d*$'"
    # plus an optional single . for floating point values, and plus an optional leading sign
    sql_query += " AND value ~ '^[-+]?\d*\.?\d*$'"
    
    sql[ "group_by" ] = " GROUP BY value_unit, ter_code, "
    
    for field in known_fields:
        sql[ "group_by" ] += "%s," % field
    
    sql[ "group_by" ] = sql[ "group_by" ][ :-1 ]
    logging.debug( "group_by: %s" % sql[ "group_by" ] )
    sql_query += sql[ "group_by" ]
    if classification == "historical":
        sql_query += ", histclass7, histclass8, histclass9, histclass10"
    elif classification == "modern":
        sql_query += ", class7, class8, class9, class10"
    
    # ordering by the db: applied to the russian contents, so the ordering of 
    # the english translation will not be perfect, but at least grouped. 
    logging.debug( "known_fields: %s" % str( known_fields ) )
    sql[ "order_by" ] = " ORDER BY "
    class_list = []
    for i in range( 1, 6 ):
        ikey = u"histclass%d" % i
        if known_fields.get( ikey ):
            class_list.append( ikey )
    for i in range( 1, 6 ):
        ikey = u"class%d" % i
        if known_fields.get( ikey ):
            class_list.append( ikey )
    
    class_list.append( "value_unit" )
    for iclass in class_list:
        if sql[ "order_by" ] != " ORDER BY ":
            sql[ "order_by" ] += ", "
        sql[ "order_by" ] += "%s" % iclass
    logging.debug( "order_by: %s" % sql[ "order_by" ] )
    sql_query += " %s" % sql[ "order_by" ]
    if classification == "historical":
        sql_query += ", histclass7, histclass8, histclass9, histclass10"
    elif classification == "modern":
        sql_query += ", class7, class8, class9, class10"
    
    logging.debug( "sql_query: %s" % sql_query )

    if sql_query:
        cursor.execute( sql_query )
        sql_names = [ desc[ 0 ] for desc in cursor.description ]
        logging.debug( "%d sql_names:" % len( sql_names ) )
        logging.debug( sql_names )
        
        # retrieve the records from the database
        data = cursor.fetchall()
        logging.debug( "result # of data records: %d" % len( data ) )
        finaldata = []
        for item in data:
            finalitem = []
            for i, thisname in enumerate( sql_names ):
                value = item[ i ]
                if value == ". ":
                    #logging.debug( "i: %d, name: %s, value: %s" % ( i, thisname, value ) )
                    # ". " marks a trailing dot in histclass or class: skip
                    pass
                
            #for value in item:
                if value in eng_data:
                    value = value.encode( 'UTF-8' )
                    value = eng_data[ value ]
                if thisname not in forbidden:
                    finalitem.append( value )
            
            finaldata.append( finalitem )
        
        json_list = json_generator( cursor, "data", finaldata, download_key )
        
        return json_list

    return []



def cleanup_downloads( download_dir ):
    # remove too old downloads
    logging.debug( "cleanup_downloads() download_dir: %s" % download_dir )
    
    seconds_per_day = 60 * 60 * 24
    time_limit = seconds_per_day        # 1 day
    dt_now = datetime.datetime.now()
    
    ndeleted = 0
    dir_list = os.listdir( download_dir )
    for dir_name in dir_list:
        dir_path = os.path.abspath( os.path.join( download_dir, dir_name ) )
        mtime = os.path.getmtime( dir_path )
        dt_file = datetime.datetime.fromtimestamp( mtime )
        seconds = (dt_now - dt_file).total_seconds()
        
        if seconds >= time_limit:       # remove
            logging.debug( "delete: %s" % dir_name )
            ndeleted += 1
            for root, sdirs, files in os.walk( dir_path ):
                if files is not None:
                    files.sort()
                    for fname in files:
                        logging.debug( "delete: %s" % fname )
                        file_path = os.path.join( root, fname )
                        logging.debug( "file_path: %s" % file_path )
                        os.unlink( file_path )  # download file
                shutil.rmtree( root )           # download dir
        else:                                   # keep
            #logging.debug( "keep:   %s" % dir_name )
            pass
        
    logging.debug( "# of downloads deleted: %d" % ndeleted )

# ==============================================================================
app = Flask( __name__ )
#app.config[ "DEBUG" ] = True
#app.config[ "PROPAGATE_EXCEPTIONS" ] = True
#app.debug = True

logging.debug( __file__ )


@app.route( '/' )
def test():
    logging.debug( "test()" )
    description = 'Russian Repository API Service v.0.1<br>/service/regions<br>/service/topics<br>/service/data<br>/service/histclasses<br>/service/years<br>/service/maps (reserved)<br>'
    return description



@app.route( "/export" )
def export():
    logging.debug( "/export" )
    settings = Configuration()
    keys = [ "intro", "intro_rus", "datatype_intro", "datatype_intro_rus", "note", "note_rus", "downloadpage1", "downloadpage1_rus" "downloadclick", "downloadclick_rus", "warningblank", "warningblank_rus", "mapintro", "mapintro_rus" ]
    exportkeys = {}
    for ikey in keys:
        if ikey in settings.config:
            exportkeys[ ikey ] = settings.config[ ikey ]
    result = json.dumps( exportkeys, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    return Response( result, mimetype = "application/json; charset=utf-8" )



@app.route( "/topics" )
def topics():
    logging.debug( "/topics" )
    language = request.args.get( "language" )
    download_key = request.args.get( "download_key" )
    
    json_list = load_topics()
    json_string = json_cache( json_list, language, "data", download_key )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



@app.route( "/filecatalog", methods = [ 'POST', 'GET' ] )
def filecatalog():
    logging.debug( "/filecatalog" )
    
    # e.g.: ?lang=en&subtopics=1_01_1795x1_02_1795
    subtopic_list = []
    logging.debug( "# of arguments %s" % len( request.args ) )
    for arg in request.args:
        logging.debug( "arg: %s, value: %s" % ( arg, request.args[ arg ] ) )
        if arg.startswith( "subtopics" ):
            subtopic_list.append( request.args[ arg ] )
    
    language = request.args.get( "lang" )
    download_key = request.args.get( "download_key" )
    
    logging.debug( "lang: %s" % language )
    logging.debug( "download_key: %s" % download_key )
    logging.debug( "subtopics: %s" % subtopic_list )
    
    json_list = []
    
    if subtopic_list is not None:
        cursor = connect()
        for subtopic in subtopic_list:
            logging.debug( "subtopic: %s" % subtopic )
            if len( subtopic ) == 9:    # e.g.: 1_01_1795
                base_year = subtopic[ 5: ]
                datatype = subtopic[ :4 ]
                datatype = datatype.replace( '_', '.' )
                logging.debug( "datatype: %s, base_year: %s" % ( datatype, base_year ) )
                json_list1 = filecat_subtopic( cursor, datatype, base_year )
                #json_list.append( json_list1 )
        
    
    
    json_string = json_cache( json_list, language, "data", download_key )
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



@app.route( "/filecatalogdata", methods = [ 'POST', 'GET' ]  )
def filecatalogdata():
    logging.debug( "/filecatalogdata" )
    logging.debug( "request: %s"           % str( request ) )
    logging.debug( "request.method: %s"    % str( request.method ) )
    logging.debug( "request.full_path: %s" % str( request.full_path ) )
    logging.debug( "request.headers: %s"   % str( request.headers ) )
    logging.debug( "request.args: %s"      % str( request.args ) )
    logging.debug( "request.form: %s"      % str( request.form ) )
    logging.debug( "request.values: %s"    % str( request.values ) )
    logging.debug( "request.data: %s"      % str( request.data ) )
    
    subtopics = []
    for key in request.args:
        value = request.args[ key ]
        logging.debug( "key: %s, value: %s" % ( key, str( value ) ) )
        if key.startswith( "subtopics" ):
            subtopics.append( value )
    
    
    language = request.args.get( "lang" )
    if language is None:
        language = "en"
    
    logging.debug( "language: %s" % language )
    
    handle_names = [ 
        "hdl_errhs_population",     # ERRHS_1   39 files
        "hdl_errhs_labour",         # ERRHS_2
        "hdl_errhs_industry",       # ERRHS_3
        "hdl_errhs_agriculture",    # ERRHS_4   10 files
        "hdl_errhs_services",       # ERRHS_5
        "hdl_errhs_capital",        # ERRHS_6
        "hdl_errhs_land"            # ERRHS_7   10 files
    ]
    
    clioinfra = Configuration()
    tmp_dir = clioinfra.config[ 'tmppath' ]
    top_download_dir = os.path.join( tmp_dir, "download" )
    logging.debug( "top_download_dir: %s" % top_download_dir )
    if not os.path.exists( top_download_dir ):
        os.makedirs( top_download_dir )
    else:
        cleanup_downloads( top_download_dir )           # remove too old downloads
    
    random_key = str( "%05.8f" % random.random() )      # used as base name for zip download
    download_key = "%s-d-filecatalog-%s" % ( language, random_key[ 2: ] )
    logging.debug( "download_key: %s" % download_key )
    
    download_dir = os.path.join( top_download_dir, download_key )   # current download dir
    logging.debug( "download_dir: %s" % download_dir )
    if not os.path.exists( download_dir ):
        os.makedirs( download_dir )
    
    params = { 
        "classification" : "data",
        "language"       : language,
        "subtopics"      : subtopics
    }
    subtopics.sort()
    
    # collect the required documentation in the download dir
    collect_docs( params, download_dir, download_key )
    
    # process and collect the needed csv files
    logging.debug( "subtopics: %s" % str( subtopics ) )
    for subtopic in subtopics:
        datatype  = subtopic[ :4 ]
        base_year = subtopic[ 5: ]
        
        datatype_maj = int( datatype[ 0 ] )
        handle_name = handle_names[ datatype_maj - 1 ]
        csv_dir = os.path.join( tmp_dir, "dataverse", "csv", handle_name )
        
        
        csv_filename = "ERRHS_%s_data_%s.csv" % ( datatype, base_year )
        logging.debug( "csv_filename: %s" % csv_filename )
        csv_pathname = os.path.join( csv_dir, csv_filename )
        logging.debug( "csv_pathname: %s" % csv_pathname )

        # process csv file
        to_xlsx = True
        process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx )
        
        # copy to download dir
        shutil.copy2( csv_pathname, download_dir )
        

    # zip download dir
    zip_filename = "%s.zip" % download_key
    logging.debug( "zip_filename: %s" % zip_filename )
    zip_dirname = os.path.join( top_download_dir, download_key )
    logging.debug( "zip_dirname: %s" % zip_dirname )
    shutil.make_archive( zip_dirname, "zip", zip_dirname )
    
    hostname = gethostname()
    json_hash = { "url_zip" : hostname + "/service/filecatalogget?zip=" + zip_filename }
    json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    
    logging.debug( json_string )
    logging.debug( "/filecatalogdata before Response()" )
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



@app.route( "/filecatalogget", methods = [ 'POST', 'GET' ] )
def filecatalogget():
    logging.debug( "/filecatalogget" )
    logging.debug( "request.args: %s" % str( request.args ) )
    zip_filename = request.args.get( "zip" )
    logging.debug( "zip_filename: %s" % zip_filename )

    clioinfra = Configuration()
    tmp_dir = clioinfra.config[ 'tmppath' ]
    top_download_dir = os.path.join( tmp_dir, "download" )
    zip_pathname = os.path.join( top_download_dir, zip_filename )
    logging.debug( "zip_pathname: %s" % zip_pathname )

    #json_string = str( {} )
    #return Response( json_string, mimetype = "application/json; charset=utf-8" )

    #return send_file( zip_pathname )
    return send_file( zip_pathname, attachment_filename = zip_filename, as_attachment = True )



@app.route( "/vocab" )
def vocab():
    logging.debug( "/vocab" )
    url = "https://datasets.socialhistory.org/api/access/datafile/586?&key=6f07ea5d-be76-444a-8a20-0ee2f02fda21&show_entity_ids=true&q=authorName:*"
    g = rdfconvertor( url )
    showformat = 'json'
    if request.args.get( 'format' ):
        showformat = request.args.get( 'format' )
    if showformat == 'turtle':
        jsondump = g.serialize( format = 'n3' )
        return Response( jsondump, mimetype = 'application/x-turtle; charset=utf-8' )
    else:
        jsondump = g.serialize( format = 'json-ld', indent = 4 )
        return Response( jsondump, mimetype = "application/json; charset=utf-8" )



@app.route( "/aggregation", methods = ["POST", "GET" ] )
def aggregation():
    logging.debug( "/aggregation" )

    qinput = simplejson.loads( request.data )
    language = qinput.get( "language" )
    classification = qinput.get( "classification" )
    datatype = qinput.get( "datatype" )
    logging.debug( "language: %s, classification: %s, datatype: %s" % ( language, classification, datatype ) )
    
    download_key = str( "%05.8f" % random.random() )  # used as base name for zip download
    # put some additional info in the key
    base_year = qinput.get( "base_year" )
    if base_year is None or base_year == "":
        base_year = "0000"
    download_key = "%s-%s-%s-%s-%s" % ( language, classification[ 0 ], datatype, base_year, download_key[ 2: ] )
    logging.debug( "download_key: %s" % download_key )
    
    clioinfra = Configuration()
    tmp_dir = clioinfra.config[ 'tmppath' ]
    download_dir = os.path.join( tmp_dir, "download", download_key )
    if not os.path.exists( download_dir ):
        os.makedirs( download_dir )
    
    if classification == "historical":
        # historical has base_year in qinput
        #json_data = aggregation_1year( qinput, download_key )
        json_list = aggregation_1year( qinput, download_key )
        json_string = json_cache( json_list, language, "data", download_key, qinput )
        logging.debug( "aggregated json_string: \n%s" % json_string )
        
        collect_docs( qinput, download_dir, download_key )  # collect doc files in download dir
        
        return Response( json_string, mimetype = "application/json; charset=utf-8" )
    
    elif classification == "modern":
        #json_datas = {}
        json_list = []
        base_years = [ "1795", "1858", "1897", "1959", "2002" ]
        #base_years = [ "1795" ]
        for base_year in base_years:
            logging.debug( "base_year: %s" % base_year )
            qinput[ "base_year" ] = base_year   # add base_year to qinput
            #json_data = aggregation_1year( qinput, download_key )
            #json_datas = merge( json_datas, json_data )
            json_list1 = aggregation_1year( qinput, download_key )
            logging.debug( "json_list1: \n%s" % str( json_list1 ) )
            json_list.extend( json_list1 )
            
        json_string = json_cache( json_list, language, "data", download_key, qinput )
        logging.debug( "aggregated json_string: \n%s" % json_string )
        
        collect_docs( qinput, download_dir, download_key )  # collect doc files in download dir
        
        return Response( json_string, mimetype = "application/json; charset=utf-8" )
    
    return str( '{}' )



@app.route( "/indicators", methods = [ "POST", "GET" ] )
def indicators():
    logging.debug( "indicators()" )
    
    logging.info( "Python version: %s" % sys.version  )

    eng_data = {}
    cursor = connect()
    
    sql_query = "SELECT datatype, base_year, COUNT(*) FROM russianrepository GROUP BY base_year, datatype;"

    if sql_query:
        cursor.execute( sql_query )
        sql_names = [ desc[ 0 ] for desc in cursor.description ]
        logging.debug( "%d sql_names:" % len( sql_names ) )
        logging.debug( sql_names )
        
        # retrieve the records from the database
        data = cursor.fetchall()
        finaldata = []
        for item in data:
            finalitem = []
            for i, thisname in enumerate( sql_names ):
                value = item[ i ]
                if value == ". ":
                    #logging.debug( "i: %d, name: %s, value: %s" % ( i, thisname, value ) )
                    # ". " marks a trailing dot in histclass or class: skip
                    pass
                
            #for value in item:
                if value in eng_data:
                    value = value.encode( 'UTF-8' )
                    value = eng_data[ value ]
                if thisname not in forbidden:
                    finalitem.append( value )
            
            finaldata.append( finalitem )
            logging.debug( str( finaldata ) )
            
        json_list = json_generator( cursor, "data", finaldata )
        
        language = request.args.get( "language" )
        download_key = request.args.get( "download_key" )
        json_string = json_cache( json_list, language, "data", download_key )
        
        logging.debug( "json_string before return Response:" )
        logging.debug( json_string )
        
        return Response( json_string, mimetype = "application/json; charset=utf-8" )

    return str( '{}' )



@app.route( "/download" )
def download():
    logging.debug( "/download" )
    logging.debug( request.args )
    
    clioinfra = Configuration()
    
    if request.args.get( 'id' ):
        logging.debug( "download() id" )
        host = "datasets.socialhistory.org"
        url = "https://%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, request.args.get('id'), clioinfra.config['ristatkey'])
        f = urllib2.urlopen( url )
        pdfdata = f.read()
        filetype = "application/pdf"
        
        if request.args.get( 'filetype' ) == 'excel':
            filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        return Response( pdfdata, mimetype = filetype )
    
    key = request.args.get( 'key' )
    logging.debug( "key: %s" % key )
    
    if key:
        zipping = True
        logging.debug( "download() zip: %s" % zipping )
    
        tmp_dir = clioinfra.config[ 'tmppath' ]
        top_download_dir = os.path.join( tmp_dir, "download" )
        logging.debug( "top_download_dir: %s" % top_download_dir )
        cleanup_downloads( top_download_dir )                   # remove too old downloads
        download_dir = os.path.join( top_download_dir, key )    # current download dir
    
        logging.debug( "download() key: %s" % key )
        clientcache = MongoClient()
        datafilter = {}
        datafilter[ 'key' ] = key
        ( lex_lands, vocab_regs_terms, sheet_header, qinput ) = preprocessor( datafilter )
        
        xlsx_name = "%s.xlsx" % key
        filename = aggregate_dataset( key, download_dir, xlsx_name, lex_lands, vocab_regs_terms, sheet_header, qinput )
        logging.debug( "filename: %s" % filename )
        with open( filename, 'rb' ) as f:
            datacontents = f.read()
        
        if not zipping:
            filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            return Response( datacontents, mimetype = filetype )
        
        else:
            """
            https://unix.stackexchange.com/questions/14705/the-zip-formats-external-file-attribute
            The high 16 bits of the external file attributes seem to be used for OS-specific permissions. The Unix values are the same as on traditional unix implementations. Other OSes use other values. Information about the formats used in a variety of different OSes can be found in the Info-ZIP source code (download or e.g in debian apt-get source [zip or unzip]) - relevant files are zipinfo.c in unzip, and the platform-specific files in zip.

            These are conventionally defined in octal (base 8); this is represented in C and python by prefixing the number with a 0.

            These values can all be found in <sys/stat.h> - link to 4.4BSD version. These are not in the POSIX standard (which defines test macros instead); but originate from AT&T Unix and BSD. (in GNU libc / Linux, the values themselves are defined as __S_IFDIR etc in bits/stat.h, though the kernel header might be easier to read - the values are all the same pretty much everywhere.)

            #define S_IFIFO  0010000  /* named pipe (fifo) */
            #define S_IFCHR  0020000  /* character special */
            #define S_IFDIR  0040000  /* directory */
            #define S_IFBLK  0060000  /* block special */
            #define S_IFREG  0100000  /* regular */
            #define S_IFLNK  0120000  /* symbolic link */
            #define S_IFSOCK 0140000  /* socket */

            And of course, the other 12 bits are for the permissions and setuid/setgid/sticky bits, the same as for chmod:

            #define S_ISUID 0004000 /* set user id on execution */
            #define S_ISGID 0002000 /* set group id on execution */
            #define S_ISTXT 0001000 /* sticky bit */
            #define S_IRWXU 0000700 /* RWX mask for owner */
            #define S_IRUSR 0000400 /* R for owner */
            #define S_IWUSR 0000200 /* W for owner */
            #define S_IXUSR 0000100 /* X for owner */
            #define S_IRWXG 0000070 /* RWX mask for group */
            #define S_IRGRP 0000040 /* R for group */
            #define S_IWGRP 0000020 /* W for group */
            #define S_IXGRP 0000010 /* X for group */
            #define S_IRWXO 0000007 /* RWX mask for other */
            #define S_IROTH 0000004 /* R for other */
            #define S_IWOTH 0000002 /* W for other */
            #define S_IXOTH 0000001 /* X for other */
            #define S_ISVTX 0001000 /* save swapped text even after use */

            As a historical note, the reason 0100000 is for regular files instead of 0 is that in very early versions of unix, 0 was for 'small' files (these did not use indirect blocks in the filesystem) and the high bit of the mode flag was set for 'large' files which would use indirect blocks. The other two types using this bit were added in later unix-derived OSes, after the filesystem had changed.

            So, to wrap up, the overall layout of the extended attributes field for Unix is

            TTTTsstrwxrwxrwx0000000000ADVSHR
            ^^^^____________________________ file type as explained above
                ^^^_________________________ setuid, setgid, sticky
                ^^^^^^^^^________________ permissions
                            ^^^^^^^^________ This is the "lower-middle byte" your post mentions
                                    ^^^^^^^^ DOS attribute bits
            """
            zip_filename = "%s.zip" % key
            memory_file = BytesIO()
            with zipfile.ZipFile( memory_file, 'w' ) as zf:
                for root, sdirs, files in os.walk( download_dir ):
                    for fname in files:
                        info = zipfile.ZipInfo( fname )
                        info.date_time = time.localtime( time.time() )[ :6 ]
                        info.compress_type = zipfile.ZIP_DEFLATED
                        # Notes from the web and zipfile sources:
                        # external_attr is 32 in size, with the unix permissions in the
                        # high order 16 bit, and the MS-DOS FAT attributes in the lower 16.
                        # man 2 stat tells us that 040755 should be a drwxr-xr-x style file,
                        # and word of mouth tells me that bit 4 marks directories in FAT.
                        info.external_attr = (0664 << 16)       # -rw-rw-r--
                        
                        file_path = os.path.join( root, fname )
                        with open( file_path, 'rb' ) as f:
                            datacontents = f.read()
                            zf.writestr( info, datacontents )
            memory_file.seek( 0 )
            return send_file( memory_file, attachment_filename = zip_filename, as_attachment = True )
        
        dbcache = clientcache.get_database( 'datacache' )
        result = dbcache.data.find( { "key": str( request.args.get( 'key' ) ) } )
        for item in result:
            del item[ 'key' ]
            del item[ '_id' ]
            dataset = json.dumps( item, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
            return Response( dataset, mimetype = "application/json; charset=utf-8" )
    else:
        return "Argument 'key' not found"



@app.route( "/documentation" )
def documentation():
    logging.debug( "/documentation" )
    #cursor = connect()
    
    # TODO should be read from config
    host = "datasets.socialhistory.org"     # with slava pythons
    #host = "data.socialhistory.org"        # virtualenv python273
    logging.debug( "host: %s" % host )
    
    clioinfra = Configuration()
    ristatkey = clioinfra.config[ "ristatkey" ]
    logging.debug( "ristatkey: %s" % ristatkey )
    
    connection = Connection( host, ristatkey )
    dataverse = connection.get_dataverse( 'RISTAT' )
    settings = DataFilter( request.args )
    papers = []
    
    logging.debug( "request.args:" )
    logging.debug( request.args )
    logging.debug( "settings:" )
    logging.debug( settings )
    
    datatype = ""
    try:
        datatype = int( request.args.get( "datatype" ) )
    except:
        pass
    
    datafilter = settings.datafilter
    
    logging.debug( "datatype: %s, type: %s" % ( datatype, type( datatype ) ) )
    logging.debug( "datafilter: %s" % str( datafilter ) )
    
    name_start = "ERRHS_" + str( datatype ) + "_"
    logging.debug( "name_start: %s" % name_start )
    
    for item in dataverse.get_contents():
        handle = str( item[ 'protocol' ] ) + ':' + str( item[ 'authority' ] ) + "/" + str( item[ 'identifier' ] )
        if handle == clioinfra.config[ 'ristatdocs' ]:
            datasetid = item[ 'id' ]
            url = "https://" + str( host ) + "/api/datasets/" + str( datasetid ) + "/?&key=" + str( clioinfra.config[ 'ristatkey' ] )
            dataframe = loadjson( url )
            for files in dataframe[ "data" ][ "latestVersion" ][ "files" ]:
                paperitem = {}
                paperitem[ 'id' ] = str( files[ 'datafile' ][ 'id' ] )
                paperitem[ 'name' ] = str( files[ 'datafile' ][ 'name' ] )
                paperitem[ 'url' ] = "http://data.sandbox.socialhistoryservices.org/service/download?id=%s" % paperitem[ 'id' ]
                logging.debug( "paperitem: %s" % paperitem )
                
                name = str( files[ 'datafile' ][ 'name' ] )
                
                if datatype != "":      # use datatype to limit the returned documents
                    # find substring between the first two underscores
                    sub_name = ""
                    p1 = name.find( "_" )
                    if p1 != -1:
                        p2 = name.find( "_", p1+1 )
                        if p2 != -1:
                            sub_name = name[ p1+1:p2 ]
                            logging.debug( "sub_name: %s" % sub_name )
                            try:
                                sub_digits = int( sub_name )
                                if sub_digits != datatype:
                                    continue    # datatype does not match: skip
                            except:
                                pass            # allow
                
                try:
                    if 'lang' in settings.datafilter:
                        varpat = r"(_%s)" % ( settings.datafilter[ 'lang' ] )
                        pattern = re.compile( varpat, re.IGNORECASE )
                        found = pattern.findall( paperitem[ 'name' ] )
                        if found:
                            papers.append( paperitem )
                    else:   # paperitem without language specified: add
                        papers.append( paperitem )
                    
                    if 'topic' in settings.datafilter:
                        varpat = r"(_%s_.+_\d+_+%s.|class|region)" % ( settings.datafilter[ 'topic' ], settings.datafilter[ 'lang' ] )
                        pattern = re.compile( varpat, re.IGNORECASE )
                        found = pattern.findall( paperitem[ 'name' ] )
                        if found:
                            papers.append( paperitem )
                    else:
                        if 'lang' not in settings.datafilter: 
                            papers.append( paperitem )
                except:
                    if 'lang' not in settings.datafilter:
                        papers.append( paperitem )
    
    logging.debug( "papers in response:" )
    for paper in papers:
        logging.debug( paper )
    
    return Response( json.dumps( papers ), mimetype = "application/json; charset=utf-8" )



@app.route( "/histclasses" )
def histclasses():
    logging.debug( "/histclasses" )
    data = load_vocabulary( 'historical' )
    logging.debug( "data: %s" % str( data ) )
    logging.debug( "histclasses() before return Response" )
    
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



@app.route( "/classes" )
def classes():
    logging.debug( "/classes" )
    data = load_vocabulary( "modern" )
    logging.debug( "data: %s" % str( data ) )
    logging.debug( "classes() before return Response" )
    
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



@app.route( "/years" )
def years():
    logging.debug( "/years" )
    cursor = connect()
    settings = DataFilter( request.args )
    datatype = ''
    if "datatype" in settings.datafilter:
        datatype = settings.datafilter[ "datatype" ]
    data = load_years( cursor, datatype )
    
    #json_data = json.dumps( data )     # GUI expects list ?
    return Response( data, mimetype = "application/json; charset=utf-8" )



@app.route( "/regions" )
def regions():
    logging.debug( "/regions" )
    cursor = connect()
    data = load_vocabulary( "ERRHS_Vocabulary_regions" )
    
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



@app.route( "/translate" )
def translate():
    logging.debug( "/translate" )
    cursor = connect()
    if cursor:
        data = {}
        sql = "select * from datasets.classmaps where 1=1";
        sql = sqlfilter( sql )
        
        cursor.execute( sql )       # execute
        
        data = cursor.fetchall()    # retrieve the records from the database
        json_list = json_generator( cursor, "data", data )
        
        language = request.args.get( "language" )
        download_key = request.args.get( "download_key" )
        json_string = json_cache( json_list, language, "data", download_key )
        
        return Response( json_string, mimetype = "application/json; charset=utf-8" )



@app.route( "/filter", methods = [ "POST", "GET" ] )
def login( settings = '' ):
    logging.debug( "login()" )
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
        classification = qinput[ 'classification' ]
    except:
        classification = 'historical'
    forbidden = [ "classification", "action", "language" ]
    for name in qinput:
        if not name in forbidden:
            sql+= " AND %s='%s'" % ( name, qinput[ name ] )

    #return sql
    if sql:
        # execute
        cursor.execute( sql )
        sql_names = [ desc[ 0 ] for desc in cursor.description ]

        data = cursor.fetchall()
        json_data = datasetfilter( data, sql_names, classification )
        return Response( json_data, mimetype = "application/json; charset=utf-8" )
    else:
        return ''



# http://bl.ocks.org/mbostock/raw/4090846/us.json
@app.route( "/maps" )
def maps():
    logging.debug( "/maps" )
    donors_choose_url = "http://bl.ocks.org/mbostock/raw/4090846/us.json"
    response = urllib2.urlopen( donors_choose_url )
    json_response = json.load( response )
    
    return Response( json_response, mimetype = "application/json; charset=utf-8" )


"""
@app.route('/')                 def test():
@app.route("/export")           def export():
@app.route("/topics")           def topics():
@app.route("/vocab")            def vocab():
@app.route("/aggregation")      def aggregation():
@app.route("/download")         def download():
@app.route("/documentation")    def documentation():
@app.route("/histclasses")      def histclasses():
@app.route("/classes")          def classes():
@app.route("/years")            def years():
@app.route("/regions")          def regions():
@app.route("/data")             def data():
@app.route("/translate")        def translate():
@app.route("/filter")           def login(settings=''):     # FL filter -> login ?
@app.route("/maps")             def maps():
"""

if __name__ == '__main__':
    app.run()

# [eof]
