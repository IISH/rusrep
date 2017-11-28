# -*- coding: utf-8 -*-

"""
VT-07-Jul-2016 latest change by VT
FL-12-Dec-2016 use datatype in function documentation()
FL-20-Jan-2017 utf8 encoding
FL-05-Aug-2017 cleanup function load_vocabulary()
FL-28-Nov-2017 

def get_configparser():
def get_connection():
def classcollector( keywords ):
def strip_subclasses( old_path ):
def json_generator( sql_names, json_dataname, data, download_key = None ):
def json_cache( entry_list, language, json_dataname, download_key, qinput = {} ):
def collect_docs( qinput, download_dir, download_key ):
def translate_vocabulary( vocab_filter, classification = None ):
def load_years( cursor, datatype ):
def sqlfilter( sql ):
def sqlconstructor( sql ):
def topic_counts():
def load_topics():
def dataset_filter( data, sql_names, classification ):
def zap_empty_classes( item ):
def translate_item( item, eng_data ):
def load_vocabulary( vocab_type ):
def get_sql_where( name, value ):
def loadjson( json_dataurl ):
def filecat_subtopic( cursor, datatype, base_year ):
def process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx ):
def aggregate_1year( qinput, count_dots, do_subclasses, separate_tc ):
def execute_1year( sql_query )
def temp_table( params, entry_list, entry_list_tc)
def cleanup_downloads( download_dir, time_limit ):
def format_secs( seconds ):

@app.route( '/' )                                               def test():
@app.route( "/documentation" )                                  def documentation():
@app.route( "/topics" )                                         def topics():
@app.route( "/years" )                                          def years():
@app.route( "/regions" )                                        def regions():
@app.route( "/histclasses" )                                    def histclasses():
@app.route( "/classes" )                                        def classes():
@app.route( "/indicators", methods = [ "POST", "GET" ] )        def indicators():
@app.route( "/aggregation", methods = ["POST", "GET" ] )        def aggregation():
@app.route( "/filecatalogdata", methods = [ 'POST', 'GET' ]  )  def filecatalogdata():
@app.route( "/filecatalogget", methods = [ 'POST', 'GET' ] )    def filecatalogget():
@app.route( "/download" )                                       def download():
@app.route( "/logfile" )                                        def getupdatelog():
"""


# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

import sys
reload( sys )
sys.setdefaultencoding( "utf8" )

import collections
import ConfigParser
import copy
import csv
import datetime
import json
import logging
import os

# matplotlib needs tmp a dir
os.environ[ "MPLCONFIGDIR" ] = "/tmp"
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
from rdflib import Graph, Literal, term
from socket import gethostname
from StringIO import StringIO
from sys import exc_info
from time import ctime, time, localtime

from dataverse import Connection
from excelmaster import aggregate_dataset, preprocessor

from configutils import DataFilter

sys.path.insert( 0, os.path.abspath( os.path.join( os.path.dirname( "__file__" ), "./" ) ) )

forbidden = [ "classification", "action", "language", "path" ]
vocab_debug = False


def get_configparser():
    RUSSIANREPO_CONFIG_PATH = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]
    logging.info( "RUSSIANREPO_CONFIG_PATH: %s" % RUSSIANREPO_CONFIG_PATH )
    
    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    logging.info( "configpath: %s" % configpath )
    
    configparser = ConfigParser.RawConfigParser()
    configparser.read( configpath )
    
    return configparser



def get_connection():
    logging.debug( "get_connection()" )
    
    configparser = get_configparser()
    
    host     = configparser.get( "config", "dbhost" )
    dbname   = configparser.get( "config", "dbname" )
    user     = configparser.get( "config", "dblogin" )
    password = configparser.get( "config", "dbpassword" )
    
    logging.debug( "host:       %s" % host )
    logging.debug( "dbname:     %s" % dbname )
    logging.debug( "user:       %s" % user )
    #logging.debug( "password:   %s" % password )
    
    connection_string = "host='%s' dbname='%s' user='%s' password='%s'" % ( host, dbname, user, password )
    
    # get a connection, if a connect cannot be made an exception will be raised here
    connection = psycopg2.connect( connection_string )

    return connection



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



def strip_subclasses( old_path ):
    logging.debug( "strip_subclasses()" )
    """
    FL-28-Nov-2017
    path dict entries may have a subclasses:True parameter, meaning that in the 
    GUI at level 4 the double checkbox was checked. If at least 1 entry with 
    subclasses:True is encountered, we return do_subclasses = True, otherwise 
    False. 
    So mixing checkboxes at level 4 always has the effect that all ckecked 
    boxes are taken be the double ones. In this way, a single SQL query suffices. 
    Now that we use an intermediate temp table, we might as well use 2 queries: 
    on for the subclasses:True entries, and the other for the remaning entries. 
    And collecting both results in the temp table. 
    """
    do_subclasses = False   # becomes True if subclasses were removed from path
    new_path = []
    logging.debug( "old path: (%s) %s" % ( type( old_path ), str( old_path ) ) )
    for old_entry in old_path:
        logging.debug( "old entry: %s" % old_entry )
        new_entry = copy.deepcopy( old_entry )
        
        for k in old_entry:
            v = old_entry[ k ]
            #logging.debug( "k: %s, v: %s" % ( k, v ) )
            if k == "subclasses" and v:     # True
                del new_entry[ k ]
                do_subclasses = True
            
            p = k.find( "class" )
            if p != -1:
                n = k[ p+5: ]
                #logging.debug( "n: %s" % n )
                if n in [ "5", "6" ]:
                    del new_entry[ k ]
            
        logging.debug( "new entry: %s" % new_entry )
        new_path.append( new_entry )
    logging.debug( "new path: (%s) %s" % ( type( new_path ), str( new_path ) ) )
    
    return new_path, do_subclasses



def json_generator( sql_names, json_dataname, data, download_key = None ):
    logging.debug( "json_generator() json_dataname: %s, # of data items: %d" % ( json_dataname, len( data ) ) )
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
        
        old_path_list = qinput.get( "path" )
        path_list, do_subclasses = strip_subclasses( old_path_list )
        
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
    
    forbidden = { "data_active", 0, "datarecords", 1 }
    
    entry_list = []
    
    len_data = len( data )
    logging.debug( "# values in data: %d" % len_data )
    for idx, value_str in enumerate( data ):
        logging.debug( "n: %d-of-%d, value_str: %s" % ( 1+idx, len_data, str( value_str ) ) )
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
        
        # If aggregation check data output for "NA" values
        if "total" in data_keys:
            if extravalues[ "data_active" ]:
                if language == "en":
                    data_keys[ "total" ] = "NA"
                elif language == "rus":
                    data_keys = "непригодный"
        
        ( path, output ) = classcollector( data_keys )
        output[ "path" ] = path
        entry_list.append( output )
    
    value_unit = ''
    logging.debug( "# of entries in entry_list: %d" % len( entry_list ) )
    for json_entry in entry_list:
        logging.debug( "json_entry: %s" % json_entry )
        value_unit = json_entry.get( "value_unit" )
        
        # compare qinput paths with db returned paths; add missing paths (fill with NA values). 
        entry_path = json_entry.get( "path" )
        # path_list from qinput does not contain our added [hist]classes; 
        # remove them from entry_path before comparison
        entry_path_cpy = copy.deepcopy( entry_path )
        
        delete_list = []
        
        if classification == "historical":
            delete_list = [ "histclass5", "histclass6", "histclass7", "histclass8", "histclass9", "histclass10" ]
        elif classification == "modern":
            delete_list = [ "class5", "class6", "class7", "class8", "class9", "class10" ]
        
        for e in delete_list:
            try:
                del entry_path_cpy[ e ]
            except:
                pass
        try:
            path_list.remove( entry_path_cpy )
        except:
            logging.debug( "keep entry_path: %s" % entry_path_cpy )
        else:
            logging.debug( "remove entry_path: %s" % entry_path_cpy )
    
    if len( path_list ) != 0:
        # pure '.' dot entries are not returned from db
        logging.debug( "missing path entries: %d" % len( path_list ) )
        for path_entry in path_list:
            logging.debug( "path_entry: %s" % path_entry )
            new_entry = {}
            # also want to see "NA" entries in preview and download
            new_entry[ "path" ]       = path_entry
            new_entry[ "base_year" ]  = base_year
            new_entry[ "value_unit" ] = value_unit
            new_entry[ "datatype" ]   = datatype
            new_entry[ "count" ]      = 1       # was ''
            new_entry[ "ter_code" ]   = ''
            new_entry[ "total" ]      = ''      # unknown, so not 0 or 0.0
            entry_list.append( new_entry )
    
    return entry_list



def json_cache( entry_list, language, json_dataname, download_key, qinput = {} ):
    # cache entry_list in mongodb with download_key as key
    logging.debug( "json_cache() # entries in list: %d" %  len( entry_list ) )
    
    configparser = get_configparser()
    root = configparser.get( "config", "root" )
    
    json_hash = {}
    json_hash[ "language" ] = language
    json_hash[ json_dataname ] = entry_list
    
    json_hash[ "url" ] = "%s/service/download?key=%s" % ( root, download_key )
    logging.debug( "json_hash: %s" % json_hash )
    
    json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    
    time0 = time()      # seconds since the epoch
    logging.debug( "start: %s" % datetime.datetime.now() )
    
    value = None
    try:
        this_data = json_hash
        del this_data[ "url" ]
        this_data[ "key" ] = download_key
        this_data[ "language" ] = language
        if qinput is not None:
            this_data[ "qinput" ] = qinput
        
        logging.debug( "dbcache.data.insert with key: %s" % download_key )
        clientcache = MongoClient()
        dbcache = clientcache.get_database( "datacache" )
        result = dbcache.data.insert( this_data )
    except:
        logging.error( "caching with key %s failed:" % download_key )
        type_, value, tb = sys.exc_info()
        logging.error( "%s" % value )
    
    logging.debug( "stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "caching took %s" % str_elapsed )
    
    return json_string, value



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
    
    configparser = get_configparser()
    tmp_dir = configparser.get( "config", "tmppath" )
    
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



def translate_vocabulary( vocab_filter, classification = None ):
    logging.debug( "translate_vocabulary()" )
    logging.debug( "vocab_filter: %s" % str( vocab_filter ) )
    
    client = MongoClient()
    dbname = "vocabulary"
    db = client.get_database( dbname )
    
    if classification == "modern":
        del vocab_filter[ "YEAR" ]  # single "modern" classification for all years
    
    if vocab_filter:
        vocab = db.data.find( vocab_filter )
    else:
        vocab = db.data.find()
    
    data = {}
    for item in vocab:
        if "RUS" in item:
            try:
                item[ "RUS" ] = item[ "RUS" ].encode( "utf-8" )
                item[ "EN" ]  = item[ "EN" ] .encode( "utf-8" )
                
                if item[ "RUS" ].startswith( '"' ) and item[ "RUS" ].endswith( '"' ):
                    item[ "RUS" ] = string[ 1:-1 ]
                if item[ "EN" ].startswith( '"' ) and item[ "EN" ].endswith( '"' ):
                    item[ "EN" ] = string[ 1:-1 ]
                
                item[ "RUS" ] = re.sub( r'"', '', item[ "RUS" ] )
                item[ "EN" ]  = re.sub( r'"', '', item[ "EN" ] )
                
                data[ item[ "RUS" ] ] = item[ "EN" ]
                data[ item[ "EN" ] ]  = item[ "RUS" ] 
                
                if vocab_debug:
                    logging.debug( "EN: %s RUS: %s" % ( item[ "EN" ], item[ "RUS" ] ) )
            except:
                type_, value, tb = exc_info()
                logging.error( "translate_vocabulary failed: %s" % value )
    
    d = 0
    for key in data:
        d += 1
        if vocab_debug:
            logging.debug( "%d: key: %s, value: %s" % ( d, key, data[ key ] ) )
    logging.debug( "translate_vocabulary: return %d items" % len( data ) )
    
    return data



def load_years( cursor, datatype ):
    """
    return a json dictionary with record counts from table russianrepository for the given datatype
    """
    logging.debug( "load_years()" )
    
    configparser = get_configparser()
    years = configparser.get( "config", "years" ).split( ',' )
    sql = "SELECT base_year, COUNT(*) AS cnt FROM russianrepository"
    
    if datatype:
        sql = sql + " WHERE datatype = '%s'" % datatype 
    
    sql = sql + " GROUP BY base_year";
    logging.debug( sql )
    cursor.execute( sql )
    
    sql_resp = cursor.fetchall()    # retrieve the records from the database
    result = {}
    
    for val in sql_resp:
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
        if key == "basisyear":
            sql += " AND %s LIKE '%s" % ( "region_code", itemlist[ 0 ] )
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
        if key == "language":
            skip = 1
        elif key == "classification":
            skip = 1
        elif key == "basisyear":
            sql += " AND %s like '%s'" % ( "region_code", sqlparams )
        else:
            for item in itemlist:
                sqlparams = "\'%s\'" % item
            sql += " AND %s in (%s)" % ( key, sqlparams )
    return sql



def topic_counts( schema ):
    logging.info( "topic_counts()" )

    connection = get_connection()
    cursor = connection.cursor( cursor_factory = psycopg2.extras.NamedTupleCursor )

    sql_topics = "SELECT datatype, topic_name FROM "
    
    if schema:
        sql_topics += "%s.topics" % schema
    else:
        sql_topics += "topics"
    
    sql_topics += " ORDER BY datatype"
    logging.info( sql_topics )
    cursor.execute( sql_topics )
    sql_resp = cursor.fetchall()
    
    #skip_list = [ "1", "2", "3", "4", "5", "6", "7" ]
    skip_list = []
    all_cnt_dict = {}
    for record in sql_resp:
        datatype   = record.datatype
        topic_name = record.topic_name
        if datatype not in skip_list:
            #print( datatype, topic_name )
            sql_count  = "SELECT base_year, COUNT(*) AS count FROM russianrepository"
            sql_count += " WHERE datatype = '%s'" % datatype
            sql_count += " GROUP BY base_year ORDER BY base_year"
            logging.debug( sql_count )
            
            cursor.execute( sql_count )
            sql_cnt_resp = cursor.fetchall()
            cnt_dict = {}
            for cnt_rec in sql_cnt_resp:
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
    
    #schema = "datasets"
    schema = "public"
    all_cnt_dict = topic_counts( schema )
    
    sql = "SELECT * FROM %s.topics" % schema
    
    sql = sqlfilter( sql ) 
    logging.debug( "sql: %s" % sql )
    
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute( sql )

    sql_resp = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    
    cursor.close()
    connection.close()
    
    entry_list_in = json_generator( sql_names, "data", sql_resp )
    entry_list_out = []
    for topic_dict in entry_list_in:
        logging.debug( topic_dict )
        datatype = topic_dict[ "datatype" ]
        topic_dict[ "byear_counts" ] = all_cnt_dict[ datatype ]
        entry_list_out.append(topic_dict )
    
    return entry_list_out



def dataset_filter( data, sql_names, classification ):
    logging.debug( "dataset_filter()" )
    
    datafilter = []
    
    for dataline in data:
        datarow = {}
        active  = ''
        for i in range( len( sql_names ) ):
            name = sql_names[ i ]
            
            if classification == "historical":
                if name.find( "class", 0 ):
                    try:
                        nextvalue = dataline[ i+1 ]
                    except:
                        nextvalue = '.'
                    
                    if ( dataline[ i ] == '.' and nextvalue == '.' ):
                        skip = "yes"
                    else:
                        toplevel = re.search( "(\d+)", name )
                        if name.find( "histclass10", 0 ):
                            datarow[ name ] = dataline[ i ]
                            if toplevel:
                                datarow[ "levels" ] = toplevel.group( 0 )
            
            elif classification == "modern":
                if name.find( "histclass", 0 ):
                    try:
                        nextvalue = dataline[ i+1 ]
                    except:
                        nextvalue = '.'
                    
                    if ( dataline[i] == '.' and nextvalue == '.' ):
                        skip = "yes"
                    else:
                        toplevel = re.search( "(\d+)", name )
                        if name.find( "class10", 0 ):
                            datarow[ name ] = dataline[ i ]
                            if toplevel:
                                if toplevel.group( 0 ) != "10":
                                    datarow[ "levels" ] = toplevel.group( 0 )
        
        try:
            if datarow[ "levels" ] > 0:
                datafilter.append( datarow )
        except:
            pass
    
    json_string = "{}"
    if classification:
        json_string = json.dumps( datafilter, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )

    return json_string 



def zap_empty_classes( item ):
    logging.debug( "zap_empty_classes()" )
    # trailing empty classes have value ". ", skip them; 
    # bridging empty classes have value '.', keep them; 
    
    new_item = {}
    for name in item:
        value = item[ name ].encode( "utf-8" )
        # skip trailing ". " in hist & modern classes
        if ( name.startswith( "histclass" ) or name.startswith( "class" ) ) and value == ". ":
            #logging.debug( "name: %s, value: %s" % ( name, value ) )
            #value = ""
            pass
        else:
            new_item[ name ] = value
    
    return new_item



def translate_item( item, eng_data ):
    logging.debug( "translate_item()" )
    #logging.debug( "translate_item() %s \n->\n %s" % ( item, eng_data ) )
    #logging.debug( item )
    #logging.debug( eng_data )
    
    newitem = {}
    if eng_data:
        for name in item:
            value = item[ name ].encode( "utf-8" )
            if value in eng_data:
                logging.debug( "translate_item() %s -> %s" % ( value, eng_data[ value ] ) )
                value = eng_data[ value ]
            newitem[ name ] = value
        item = newitem
    return item



def load_vocabulary( vocab_type ):
    logging.debug( "load_vocabulary() vocab_type: %s" % vocab_type )
    logging.debug( "request.args: %s" % str( request.args ) )
    
    language = request.args.get( "language" )
    datatype = request.args.get( "datatype" )
    
    basisyear = request.args.get( "basisyear" )
    base_year = request.args.get( "base_year" )
    
    vocab_filter = {}
    
    
    if vocab_type == "topics":
        vocab_name = "ERRHS_Vocabulary_topics"
    
    elif vocab_type == "regions":
        vocab_name = "ERRHS_Vocabulary_regions"
        if basisyear:
            vocab_filter[ "basisyear" ] = basisyear
    
    elif vocab_type == "historical":
        vocab_name = "ERRHS_Vocabulary_histclasses"
        if datatype:
            vocab_filter[ "DATATYPE" ] = datatype
        if base_year:
            vocab_filter[ "YEAR" ] = base_year
    
    elif vocab_type == "modern":
        vocab_name = "ERRHS_Vocabulary_modclasses"
        if datatype:
            vocab_filter[ "DATATYPE" ] = "MOD_" + datatype
    
    logging.debug( "vocab_filter: %s" % vocab_filter )
    
    eng_data = {}
    if language == "en":
        new_filter = {}
        if base_year:
            new_filter[ 'YEAR' ] = base_year    # vocab_filter also contains DATATYPE
        logging.debug( "translate_vocabulary with filter: %s" % new_filter )
        eng_data = translate_vocabulary( new_filter )
        logging.debug( "translate_vocabulary eng_data items: %d" % len( eng_data ) )
        #logging.debug( "eng_data: %s" % eng_data )
        
        units = translate_vocabulary( { "vocabulary": "ERRHS_Vocabulary_units" } )
        logging.debug( "translate_vocabulary units items: %d" % len( units ) )
        #logging.debug( "units: %s" % units )
        
        for item in units:
            eng_data[ item ] = units[ item ]
            #logging.debug( "%s => %s" % ( item, units[ item ] ) )
    
    client = MongoClient()
    db_name = "vocabulary"
    db = client.get_database( db_name )
    
    params = {}
    if vocab_type == "topics":
        params[ "vocabulary" ] = vocab_name
    elif vocab_type == "regions":
        params[ "vocabulary" ] = vocab_name
        if basisyear:
            params[ "basisyear" ] = basisyear
    else:
        params[ "vocabulary" ] = vocab_type
        if base_year:
            params[ "base_year" ] = base_year
        params[ "datatype" ] = datatype
    
    logging.debug( "params: %s" % params )
    vocab = db.data.find( params )
    
    data = []
    uid = 0
    logging.debug( "processing %d items in vocab %s" % ( vocab.count(), vocab_type ) )
    for item in vocab:
        logging.debug( "item: %s" % item )
        #del item[ "basisyear" ]
        del item[ "_id" ]
        del item[ "vocabulary" ]
        topics  = {}
        regions = {}
        
        if vocab_type == "topics":
            topics[ "topic_id" ]       = item[ "TOPIC_ID" ]
            topics[ "topic_root" ]     = item[ "TOPIC_ROOT" ]
            topics[ "topic_name_rus" ] = item[ "RUS" ]
            topics[ "topic_name" ]     = item[ "EN" ]
            topics[ "datatype" ]       = item[ "DATATYPE" ]
            item = topics
            data.append( item )
        elif vocab_type == "regions":
            uid += 1
            regions[ "region_name" ]     = item[ "RUS" ]
            regions[ "region_name_eng" ] = item[ "EN" ]
            regions[ "region_code" ]     = item[ "ID" ]
            regions[ "region_id" ]       = uid
            regions[ "region_ord" ]      = 189702
            regions[ "description" ]     = regions[ "region_name" ]
            regions[ "active" ]          = 1
            item = regions
            data.append( item )
        elif vocab_type == "modern":
            item = zap_empty_classes( item )
            if eng_data:
                item = translate_item( item, eng_data )
            data.append( item )
        elif vocab_type == "historical":
            item = zap_empty_classes( item )
            if eng_data:
                item = translate_item( item, eng_data )
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
                output[ "path" ] = path
                data.append( output )
            else:
                data.append( item )
    
    logging.debug( "%d items in %s data" % ( len( data ), vocab_type ) )
    for item in data:
        logging.debug( item )
    
    json_hash = {}
    if vocab_type == "topics":
        json_hash[ "data" ] = data
    elif vocab_type == "regions":
        json_hash[ "regions" ] = data
    elif vocab_type == "modern":
        json_hash = data
    elif vocab_type == "historical":
        json_hash = data
    else:
        json_hash[ "data" ] = data
        json_data = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
        logging.debug( "load_vocabulary() return %s json_data" % vocab_type )
        logging.debug( json_data )
        return json_data

    logging.debug( "load_vocabulary() return %s json_hash" % vocab_type )
    logging.debug( json_hash )
    return json_hash



def get_sql_where( name, value ):
    logging.debug( "get_sql_where() name: %s, value: %s" % ( name, value ) )
    
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
    
    logging.debug( "sql_where: %s" % sql_query )
    return sql_query



def loadjson( json_dataurl ):
    logging.debug( "loadjson() %s" % json_dataurl )

    req = urllib2.Request( json_dataurl )
    opener = urllib2.build_opener()
    f = opener.open( req )
    dataframe = simplejson.load( f )
    return dataframe


def filecat_subtopic( cursor, datatype, base_year ):
    logging.debug( "filecatalog_subtopic()" )
    
    query  = "SELECT * FROM russianrepository"
    query += " WHERE datatype = '%s' AND base_year = '%s'" % ( datatype, base_year )
    query += " ORDER BY ter_code"
    
    cursor.execute( query )
    sql_resp = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    
    entry_list = json_generator( sql_names, "data", sql_resp )
    logging.debug( entry_list )
    
    return entry_list



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
    
    
    with open( csv_pathname, "rb" ) as csv_file:
        csv_reader = csv.reader( csv_file, delimiter = '|' )
        for row in csv_reader:
            logging.debug( ", ".join( row ) )
    
    if to_xlsx:
        sep = str( u'|' ).encode( "utf-8" )
        kwargs_pandas = { 
            "sep" : sep 
            #,"line_terminator" : '\n'   # TypeError: parser_f() got an unexpected keyword argument "line_terminator"
        }
        
        df1 = pd.read_csv( csv_pathname, **kwargs_pandas )
        
        # sort by ter_code and histclasses
        sort_columns = []
        sort_columns.append( "TER_CODE" )
        for l in range( 10 ):
            l_str = "HISTCLASS%d" % ( l + 1 )
            sort_columns.append( l_str )
        logging.debug( "sort by: %s" % sort_columns )
        df1 = df1.sort_values( by = sort_columns )
        
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



def aggregate_1year( qinput, count_dots, do_subclasses, separate_tc ):
    logging.debug( "aggregate_1year() count_dots: %s, do_subclasses: %s" % ( count_dots, do_subclasses ) )
    logging.debug( "qinput %s" % str( qinput ) )
    
    try:
        language = qinput.get( "language" )
        
        logging.debug( "number of keys in request.data: %d" % len( qinput ) )
        k = 0
        for key in qinput:
            value = qinput[ key ]
            if key == "path":
                logging.debug( "%d: path:" % k )    # debug: ≥ = u"\u2265"
                for pdict in value:
                    logging.debug( str( pdict ) )
                    for pkey in pdict:
                        pvalue = pdict[ pkey ]
                        logging.debug( "key: %s, value: %s" % ( pkey, pvalue ) )
                        if pkey == "classification":
                            classification = pvalue
            else:
                logging.debug( "%d: key: %s, value: %s" % ( k, key, value ) )
            k += 1
    except:
        type_, value, tb = exc_info()
        logging.error( "failed, no request.data: %s" % value )
        msg = "failed: %s" % value
        return str( { "msg": "%s" % msg } )
    
    classification = qinput[ "classification" ]
    logging.debug( "classification: %s" % classification )
    
    forbidden = [ "classification", "action", "language", "path" ]
    
    eng_data = {}
    if qinput.get( "language" ) == "en":
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
        
        eng_data = translate_vocabulary( vocab_filter )
        units = translate_vocabulary( { "vocabulary": "ERRHS_Vocabulary_units" } )
        
        logging.debug( "translate_vocabulary returned %d items" % len( units ) )
        logging.debug( "vocab_filter: %s" % str( vocab_filter ) )
        logging.debug( "eng_data: %s" % str( eng_data ) )
        logging.debug( "units: %s" % str( units ) )
        for item in units:
            eng_data[ item ] = units[ item ]
    
    sql = {}
    
    # ter_code separate, in order to make 2 sql queries, with and without ter_code
    sql[ "where_tc" ]     = ''
    sql[ "condition_tc" ] = ''
    known_fields_tc       = {}
    
    sql[ "where" ]     = ''
    sql[ "condition" ] = ''
    known_fields       = {}
    
    sql[ "internal" ]  = ''
    sql[ "group_by" ]  = ''
    sql[ "order_by" ]  = ''
    
    if qinput:
        for name in qinput:
            if not name in forbidden:
                value = str( qinput[ name ] )
                logging.debug( "name: %s, value: %s" % ( name, value ) )
                if value in eng_data:
                    value = eng_data[ value ]
                    logging.debug( "eng_data name: %s, value: %s" % ( name, value ) )
                
                if separate_tc and name == "ter_code":
                    sql[ "where_tc" ] += "%s AND " % get_sql_where( name, value )
                    sql[ "condition_tc" ] += "%s, " % name
                    known_fields_tc[ name ] = value
                else:
                    sql[ "where" ] += "%s AND " % get_sql_where( name, value )
                    sql[ "condition" ] += "%s, " % name
                    known_fields[ name ] = value
            
            elif name == "path":
                full_path = qinput[ name ]
                top_sql = "AND ("
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
                        
                        #if separate_tc:...
                        if not known_fields.has_key( xkey ):
                            known_fields[ xkey ] = value
                            sql[ "condition" ] += "%s, " % xkey
                    
                    if sql_local:
                        sql[ "internal" ] += " ("
                        for key in sql_local:
                            sql_local[ key ] = sql_local[ key ][ :-2 ]
                            sql[ "internal" ] += "%s AND " % sql_local[ key ]
                            logging.debug( "key: %s, value: %s" % ( key, sql_local[ key ] ) )
                            
                        sql[ "internal" ] = sql[ "internal" ][ :-4 ]
                        sql[ "internal" ] += ") OR"
    
    sql[ "internal" ] = sql[ "internal" ][ :-3 ]
    
    logging.debug( "sql: %s" % str( sql ) )

    for key in sql:
        logging.debug( "sql key: %s, sql value: %s" % ( key, str( sql[ key ] ) ) )
    
    extra_classes = []
    if do_subclasses:   # 5&6 were removed from path; add them all here
        if classification == "historical":
            extra_classes = [ "histclass5", "histclass6", "histclass7", "histclass8", "histclass9", "histclass10" ]
        elif classification == "modern":
            extra_classes = [ "class5", "class6", "class7", "class8", "class9", "class10" ]
        logging.debug( "extra_classes: %s" % extra_classes )
    
    if count_dots:
        sql_query  = "SELECT COUNT(*) AS datarecords"
        sql_query += ", COUNT(*) AS total"
    else:
        sql_query  = "SELECT COUNT(*) AS datarecords" 
        sql_query += ", SUM(CAST(value AS DOUBLE PRECISION)) AS total"
    
    sql_query += ", COUNT(*) AS count"
    sql_query += ", COUNT(*) - COUNT(value) AS data_active"
    
    if classification == "modern":  # "ter_code" keyword not in qinput, but we always need it
        logging.debug( "modern classification: adding ter_code to SELECT" )
        sql_query += ", ter_code"
    
    sql_query += ", value_unit"
    logging.debug( "sql_query 0: %s" % sql_query )

    if len( extra_classes ) > 0:
        for field in extra_classes:
            sql_query += ", %s" % field
        logging.debug( "sql_query 1: %s" % sql_query )
    
    #if separate_tc:...
    if sql[ "where" ]:
        logging.debug( "where: %s" % sql[ "where" ] )
        sql_query += ", %s" % sql[ "condition" ]
        sql_query  = sql_query[ :-2 ]
        logging.debug( "sql_query 2: %s" % sql_query )
        
        sql_query += " FROM russianrepository WHERE %s" % sql[ "where" ]
        sql_query  = sql_query[ :-4 ]
        logging.debug( "sql_query 3: %s" % sql_query )
    
    if count_dots:
        sql_query += " AND value = '.'"             # only dots
    else:
        sql_query += " AND value <> ''"             # suppress empty values
        sql_query += " AND value <> '.'"            # suppress a 'lone' "optional point", used in the table to flag missing data
        # plus an optional single . for floating point values, and plus an optional leading sign
        sql_query += " AND value ~ '^[-+]?\d*\.?\d*$'"
    
    logging.debug( "sql_query 4: %s" % sql_query )
    
    if sql[ "internal" ]:
        logging.debug( "internal: %s" % sql[ "internal" ] )
        sql_query += " AND (%s) " % sql[ "internal" ]
        logging.debug( "sql_query 5: %s" % sql_query )
    
    sql[ "group_by" ] = " GROUP BY value_unit, ter_code"
    
    #if separate_tc:...
    for field in known_fields:
        sql[ "group_by" ] += ", %s" % field
    for field in extra_classes:
        sql[ "group_by" ] += ", %s" % field
    
    logging.debug( "group_by: %s" % sql[ "group_by" ] )
    sql_query += sql[ "group_by" ]
    logging.debug( "sql_query 6: %s" % sql_query )
    
    # ordering by the db: applied to the russian contents, so the ordering of 
    # the english translation will not be perfect, but at least grouped. 
    #if separate_tc:...
    logging.debug( "known_fields: %s" % str( known_fields ) )
    sql[ "order_by" ] = " ORDER BY "
    class_list = []
    for i in range( 1, 4 ):
        ikey = u"histclass%d" % i
        if known_fields.get( ikey ):
            class_list.append( ikey )
    for i in range( 1, 4 ):
        ikey = u"class%d" % i
        if known_fields.get( ikey ):
            class_list.append( ikey )
    
    class_list.append( "value_unit" )
    for iclass in class_list:
        if sql[ "order_by" ] != " ORDER BY ":
            sql[ "order_by" ] += ", "
        sql[ "order_by" ] += "%s" % iclass
    
    for field in extra_classes:
        sql[ "order_by" ] += ", %s" % field
    
    logging.debug( "order_by: %s" % sql[ "order_by" ] )
    sql_query += " %s" % sql[ "order_by" ]
    
    logging.debug( "sql_query 7 = complete: %s" % sql_query )

    return sql_query, eng_data



def execute_1year( sql_query, eng_data, download_key ):
    logging.debug( "execute_1year()" )
    
    entry_list = []
    
    if sql_query:
        time0 = time()      # seconds since the epoch
        logging.debug( "query execute start: %s" % datetime.datetime.now() )
        
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute( sql_query )
        logging.debug( "query execute stop: %s" % datetime.datetime.now() )
        str_elapsed = format_secs( time() - time0 )
        logging.info( "sql_query execute took %s" % str_elapsed )
        
        sql_names = [ desc[ 0 ] for desc in cursor.description ]
        logging.debug( "%d sql_names:" % len( sql_names ) )
        logging.debug( sql_names )
        
        # retrieve the records from the database
        sql_resp = cursor.fetchall()
        logging.debug( "result # of data records: %d" % len( sql_resp ) )
        
        cursor.close()
        connection.close()
        
        final_data = []
        for idx, item in enumerate( sql_resp ):
            logging.debug( "%d: %s" % ( idx, item ) )
            final_item = []
            for i, thisname in enumerate( sql_names ):
                value = item[ i ]
                if value == ". ":
                    #logging.debug( "i: %d, name: %s, value: %s" % ( i, thisname, value ) )
                    # ". " marks a trailing dot in histclass or class: skip
                    pass
                
                if value in eng_data:
                    value = value.encode( "utf-8" )
                    value = eng_data[ value ]
                if thisname not in forbidden:
                    final_item.append( value )
            
            final_data.append( final_item )
        
        entry_list = json_generator( sql_names, "data", final_data, download_key )
        
    return entry_list



def temp_table( params, entry_list, entry_list_tc ):
    logging.debug( "temp_table()" )
    logging.debug( "# of entries in entry_list: %d" % len( entry_list ) )
    logging.debug( "# of entries in entry_list_tc: %d" % len( entry_list_tc ) )
    """
    params = {
        "language"       : language,
        "classification" : classification,
        "datatype"       : datatype,
        "base_year"      : base_year,
        "ter_codes"      : ter_codes
    }
    """
    # number of asked regions
    ter_codes = params[ "ter_codes" ]
    nregions = len( ter_codes )
    logging.debug( "# of regions requested: %d" % nregions )

    # number of unique records in path_list_tc
    path_list_tc = []
    for entry in entry_list_tc: 
        path = entry[ "path" ]
        if path not in path_list_tc:
            path_list_tc.append( path )
    logging.debug( "# of unique records in path_tc result: %d" % len( path_list_tc ) )
    
    # we only need to keep the entry_list entries that have counts, the others 
    # are contained in entry_list_tc
    logging.debug( "# of records in path result: %d" % len( entry_list ) )
    entry_list_cnt = []
    for e, entry in enumerate( entry_list ):
        logging.debug( "entry %d: %s" % ( e, entry ) )
        if entry[ "total" ] != '':
            entry_list_cnt.append( entry )
    logging.debug( "# of records in path result with count: %d" % len( entry_list_cnt ) )
     
    for e, entry in enumerate( entry_list_cnt ):
        logging.debug( "%d total: %s, %s" % ( e, entry[ "total" ], entry[ "path" ] ) )
    
    # number of levels
    path    = entry_list_tc[ 0 ][ "path" ]
    path_tc = entry_list_tc[ 0 ][ "path" ]
    nlevels    = len( path.keys() )
    nlevels_tc = len( path_tc.keys() )
    logging.debug( "# of levels: %d" % nlevels )
    logging.debug( "# of levels_tc: %d" % nlevels_tc )
    
    # historical or modern?
    level_prefix = "class"
    if params.get( "classification" ) == "historical":
        level_prefix = "hist" + level_prefix
    
    connection = get_connection()
    cursor = connection.cursor( cursor_factory = psycopg2.extras.DictCursor )
    
    use_temp_table = False
    sql_delete = None
    sql_create = ""
    
    # TEMP TABLE: Both table definition and data are visible to the current session only 
    table_name = "temp_aggregate"
    if use_temp_table:
        sql_create  = "CREATE TEMP TABLE %s (" % table_name 
    else:
        sql_delete = "DROP TABLE %s" % table_name 
        sql_create  = "CREATE TABLE %s (" % table_name 
    
    for column in range( 1, nlevels_tc + 1 ):
        sql_create += "%s%d VARCHAR(1024)," % ( level_prefix, column )
    
    sql_create += "unit VARCHAR(1024),"
    sql_create += "count VARCHAR(1024),"
    
    ntc = len( ter_codes )
    for tc, ter_code in enumerate( ter_codes ):
        sql_create += "tc_%s VARCHAR(1024)" % ter_code
        if tc + 1 < ntc:
            sql_create += ","
    
    sql_create += ")"
    
    # on production server: use DROP
    if use_temp_table:
        sql_create += "ON COMMIT PRESERVE ROWS;"     # default
        #sql_create += "ON COMMIT DELETE ROWS"       # delete all rows
        #sql_create += "ON COMMIT DROP"              # drop table
    
    logging.debug( "sql_create: %s" % sql_create )
    
    if sql_delete:
        cursor.execute( sql_delete )
    cursor.execute( sql_create )
    
    # fill table
    for p, path in enumerate( path_list_tc ):
        logging.debug( "%d-of-%d path: %s" % ( p, len( path_list_tc ), path ) )
        columns = ""
        values  = ""
        for k, key in enumerate( path ):
            value = path[ key ]
            if k > 0:
                columns += ","
                values  += ","
            columns += key
            values  += "'%s'" % value
        
        unit = '?'
        ncounts = 0
        for ter_code in ter_codes:
            logging.debug( "ter_code: %s" % ter_code )
            # search for path + ter_code in list with counts
            value = "NA"
            for entry in entry_list_cnt:
                if path == entry[ "path" ] and ter_code == entry[ "ter_code" ]:
                    ncounts += 1
                    unit  = entry[ "value_unit" ]
                    value = entry[ "total" ]
                    break
            
            columns += ",tc_%s" % ter_code
            values  += ",'%s'"  % value
        
        logging.debug( "columns: %s" % columns )
        logging.debug( "values:  %s" % values )
        
        columns += ",unit"
        values  += ",'%s'" % unit
        
        columns += ",count"
        values  += ",'%d/%d'" % ( ncounts, nregions )
        
        sql_insert = "INSERT INTO %s (%s) VALUES (%s);" % ( table_name , columns, values ) 
        logging.debug( sql_insert )
        cursor.execute( sql_insert )
    
    order_by = ""
    for l in range( 1, 1 + nlevels_tc ):
        if l > 1:
            order_by += ','
        order_by += "%s%d" % ( level_prefix, l )
            
    sql_query = "SELECT * FROM %s ORDER BY %s" % ( table_name, order_by )
    logging.debug( sql_query )
    cursor.execute( sql_query )
    sql_resp = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    logging.debug( "%d sql_names: \n%s" % ( len( sql_names ), sql_names ) )
    
    entry_list_sorted = []
    for r, row in enumerate( sql_resp ):
        record = dict( row )
        logging.debug( "%d: %s" % ( r, record ) )
        
        # in: names: ['histclass1', 'histclass2', 'histclass3', 'histclass4', 'histclass5', 'unit', 'count', 'tc_1858_28', 'tc_1858_59']
        # in record 0: ('Military estates', 'Indefinitely furloughed', '.', '.', 'Both sexes', 'persons', '1/2', '10522.0', 'NA')
        # in: Record(histclass1='Military estates', histclass2='Indefinitely furloughed', histclass3='.', histclass4='.', histclass5='Both sexes', unit='persons', count='1/2', tc_1858_28='10522.0', tc_1858_59='NA')
        # out: {'count': 1L, 'total': 84.0, 'datatype': '1.05', 'base_year': 1858, u'path': {'histclass4': 'Kronshtadt', 'histclass5': 'Children of both sexes', 'histclass2': 'Indefinitely furloughed', 'histclass3': '.', 'histclass1': 'Military estates'}, 'ter_code': '1858_59', 'value_unit': 'persons'}

        # 1 entry per ter_code
        for ter_code in ter_codes:
            new_entry = {
                "datatype"  : params[ "datatype" ],
                "base_year" : params[ "base_year" ],
                "ter_code"  : ter_code,
            }
            
            path = {}
            
            for key in record:
                value = record[ key ]
                if "class" in key:
                    path[ key ] = value
                
                
                
            #new_entry[ "value_unit" ] = record[ "" ]
            #new_entry[ "total" ]      = record[ "" ]
            #new_entry[ "ter_code" ]   = record[ "" ]
            
            logging.debug( new_entry )
            
            entry_list_sorted.append( new_entry )
    
    connection.commit()
    cursor.close()
    connection.close()
    
    return entry_list_sorted



def cleanup_downloads( download_dir, time_limit ):
    # remove too old downloads
    logging.debug( "cleanup_downloads() time_limit: %d, download_dir: %s" % ( time_limit, download_dir ) )
    
    dt_now = datetime.datetime.now()
    
    f_deleted = 0
    d_deleted = 0
    file_list = os.listdir( download_dir )
    
    for file_name in file_list:
        file_path = os.path.abspath( os.path.join( download_dir, file_name ) )
        
        if os.path.isdir( file_path ):
            dir_name = file_name
            dir_path = file_path
            mtime = os.path.getmtime( dir_path )
            dt_file = datetime.datetime.fromtimestamp( mtime )
            seconds = (dt_now - dt_file).total_seconds()
            
            if seconds >= time_limit:       # remove
                logging.debug( "seconds : %d, delete: %s" % ( seconds, dir_name ) )
                d_deleted += 1
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
                logging.debug( "seconds : %d, keep:   %s" % ( seconds, dir_name ) )
                pass
        else:
            mtime = os.path.getmtime( file_path )
            dt_file = datetime.datetime.fromtimestamp( mtime )
            seconds = (dt_now - dt_file).total_seconds()
            
            if seconds >= time_limit:       # remove
                logging.debug( "seconds : %d, delete: %s" % ( seconds, file_name ) )
                f_deleted += 1
                logging.debug( "delete: %s" % file_name )
                os.unlink( file_path )  # download file
            else:                                   # keep
                logging.debug( "seconds : %d, keep:   %s" % ( seconds, file_name ) )
                pass
    
    if f_deleted > 0:
        logging.info( "# of files deleted: %d" % f_deleted )
    if d_deleted > 0:
        logging.info( "# of download dirs deleted: %d" % d_deleted )



def format_secs( seconds ):
    nmin, nsec  = divmod( seconds, 60 )
    nhour, nmin = divmod( nmin, 60 )

    if nhour > 0:
        str_elapsed = "%d:%02d:%02d (hh:mm:ss)" % ( nhour, nmin, nsec )
    else:
        if nmin > 0:
            str_elapsed = "%02d:%02d (mm:ss)" % ( nmin, nsec )
        else:
            str_elapsed = "%d (sec)" % nsec

    return str_elapsed



# ==============================================================================
app = Flask( __name__ )
#app.config[ "DEBUG" ] = True
#app.config[ "PROPAGATE_EXCEPTIONS" ] = True
#app.debug = True

logging.debug( __file__ )


@app.route( '/' )
def test():
    logging.debug( "test()" )
    #description = 'Russian Repository API Service v.0.1<br>/service/regions<br>/service/topics<br>/service/data<br>/service/histclasses<br>/service/years<br>/service/maps (reserved)<br>'
    description = "Russian Repository API Service v.1.0<br>"
    return description


# Documentation - Get documentation needed to show files in help pop-up at GUI step 1
@app.route( "/documentation" )
def documentation():
    logging.debug( "/documentation" )
    logging.debug( "Python version: %s" % sys.version  )

    configparser   = get_configparser()
    dataverse_root = configparser.get( "config", "dataverse_root" )
    api_root       = configparser.get( "config", "api_root" )
    ristatkey      = configparser.get( "config", "ristatkey" )
    ristatdocs     = configparser.get( "config", "hdl_documentation" )
    
    #logging.info( "dataverse_root: %s" % dataverse_root )
    #logging.info( "ristatkey: %s" % ristatkey )
    connection = Connection( dataverse_root, ristatkey )
    dataverse = connection.get_dataverse( "RISTAT" )
    
    settings = DataFilter( request.args )
    papers = []
    
    logging.debug( "request.args: %s" % request.args )
    logging.debug( "settings: %s" % settings )
    
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
        handle = str( item[ "protocol" ] ) + ':' + str( item[ "authority" ] ) + '/' + str( item[ "identifier" ] )
        if handle == ristatdocs:
            datasetid = item[ "id" ]
            url = "http://" + dataverse_root + "/api/datasets/" + str( datasetid ) + "/?&key=" + str( ristatkey )
            logging.debug( "url: %s" % url )
            dataframe = loadjson( url )
            for files in dataframe[ "data" ][ "latestVersion" ][ "files" ]:
                paperitem = {}
                paperitem[ "id" ] = str( files[ "datafile" ][ "id" ] )
                paperitem[ "name" ] = str( files[ "datafile" ][ "name" ] )
                paperitem[ "url" ] = "%s/service/download?id=%s" % ( api_root, paperitem[ "id" ] )
                logging.debug( "paperitem: %s" % paperitem )
                
                name = str( files[ "datafile" ][ "name" ] )
                
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
                    if "lang" in settings.datafilter:
                        varpat = r"(_%s)" % ( settings.datafilter[ "lang" ] )
                        pattern = re.compile( varpat, re.IGNORECASE )
                        found = pattern.findall( paperitem[ "name" ] )
                        if found:
                            papers.append( paperitem )
                    else:   # paperitem without language specified: add
                        papers.append( paperitem )
                    
                    if "topic" in settings.datafilter:
                        varpat = r"(_%s_.+_\d+_+%s.|class|region)" % ( settings.datafilter[ "topic" ], settings.datafilter[ "lang" ] )
                        pattern = re.compile( varpat, re.IGNORECASE )
                        found = pattern.findall( paperitem[ "name" ] )
                        if found:
                            papers.append( paperitem )
                    else:
                        if "lang" not in settings.datafilter: 
                            papers.append( paperitem )
                except:
                    if "lang" not in settings.datafilter:
                        papers.append( paperitem )
    
    logging.debug( "papers in response:" )
    for paper in papers:
        logging.debug( paper )
    
    return Response( json.dumps( papers ), mimetype = "application/json; charset=utf-8" )



# Topics - Get topics an process them as terms for GUI
@app.route( "/topics" )
def topics():
    logging.debug( "/topics" )
    logging.debug( "topics() request.args: %s" % str( request.args ) )
    # uses a vocabulary file from dataverse
    data = load_vocabulary( "topics" )
    
    logging.debug( "/topics return Response" )
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



# Years - Get available years for year selection at GUI step 2
@app.route( "/years" )
def years():
    logging.debug( "/years" )
    
    logging.debug( "request.args: %s" % str( request.args ) )
    settings = DataFilter( request.args )
    
    datatype = ''
    if "datatype" in settings.datafilter:
        datatype = settings.datafilter[ "datatype" ]
    
    connection = get_connection()
    cursor = connection.cursor()
    # json_string = dictionary with record counts from table russianrepository for the given datatype
    json_string = load_years( cursor, datatype )
    cursor.close()
    connection.close()
    
    logging.debug( "/years return Response" )
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



# Regions - Get regions to build the region selector at GUI step 3 
@app.route( "/regions" )
def regions():
    logging.debug( "/regions" )
    logging.debug( "regions() request.args: %s" % str( request.args ) )
    data = load_vocabulary( "regions" )
    
    logging.debug( "/regions return Response" )
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



# Histclasses - Get historical indicators for the indicator selection at GUI step 4
@app.route( "/histclasses" )
def histclasses():
    logging.debug( "/histclasses" )
    logging.debug( "histclasses() request.args: %s" % str( request.args ) )
    data = load_vocabulary( "historical" )
    
    logging.debug( "/histclasses return Response" )
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



# Classes - Get modern indicators for the indicator selection at GUI step 4
@app.route( "/classes" )
def classes():
    logging.debug( "/classes" )
    logging.debug( "classes() request.args: %s" % str( request.args ) )
    data = load_vocabulary( "modern" )
    
    logging.debug( "/classes return Response" )
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



# Indicators - Is this used by the GUI?
@app.route( "/indicators", methods = [ "POST", "GET" ] )
def indicators():
    logging.debug( "indicators()" )

    connection = get_connection()
    cursor = connection.cursor()
    
    sql_query = "SELECT datatype, base_year, COUNT(*) FROM russianrepository GROUP BY base_year, datatype;"
    cursor.execute( sql_query )
    
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    logging.debug( "%d sql_names:" % len( sql_names ) )
    logging.debug( sql_names )
    
    sl_resp = cursor.fetchall()    # retrieve the records from the database
    cursor.close()
    connection.close()
    
    eng_data = {}
    final_data = []
    
    for item in sl_resp:
        final_item = []
        for i, thisname in enumerate( sql_names ):
            value = item[ i ]
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, thisname, value ) )
                # ". " marks a trailing dot in histclass or class: skip
                pass
            
            if value in eng_data:
                value = value.encode( "utf-8" )
                value = eng_data[ value ]
            if thisname not in forbidden:
                final_item.append( value )
        
        final_data.append( final_item )
        logging.debug( str( final_data ) )
    
    entry_list = json_generator( sql_names, "data", final_data )
    
    language = request.args.get( "language" )
    download_key = request.args.get( "download_key" )
    json_string, cache_except = json_cache( entry_list, language, "data", download_key )
    
    logging.debug( "json_string before return Response:" )
    logging.debug( json_string )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



# Aggregation - Preview User selection post and gets data to build preview in GUI step 5 
@app.route( "/aggregation", methods = ["POST", "GET" ] )
def aggregation():
    logging.debug( "/aggregation" )
    logging.debug( request.data )
    
    time0 = time()      # seconds since the epoch
    logging.debug( "start: %s" % datetime.datetime.now() )
    
    try:
        qinput = simplejson.loads( request.data )
        logging.debug( qinput )
    except:
        type_, value, tb = exc_info()
        msg = "failed: %s" % value
        return str( { "msg": "%s" % msg } )
    
    language = qinput.get( "language" )
    classification = qinput.get( "classification" )
    datatype = qinput.get( "datatype" )
    logging.debug( "language: %s, classification: %s, datatype: %s" % ( language, classification, datatype ) )
    
    # strip [hist]class5 & 6 from path (no longer needed)
    old_path = qinput.get( "path" )
    new_path, do_subclasses = strip_subclasses( old_path )
    qinput[ "path" ] = new_path                 # replace
    
    download_key = str( "%05.8f" % random.random() )  # used as base name for zip download
    # put some additional info in the key
    base_year = qinput.get( "base_year" )
    if base_year is None or base_year == "":
        base_year = "0000"
    download_key = "%s-%s-%s-%s-%s" % ( language, classification[ 0 ], datatype, base_year, download_key[ 2: ] )
    logging.debug( "download_key: %s" % download_key )
    
    configparser = get_configparser()
    tmp_dir = configparser.get( "config", "tmppath" )
    download_dir = os.path.join( tmp_dir, "download", download_key )
    if not os.path.exists( download_dir ):
        os.makedirs( download_dir )
    
    count_dots  = False         # value = '.'
    separate_tc = False         # True: ter_code constraint separate: 2 queries
    json_string = str( "{}" )
    
    if classification == "historical":
        # historical has base_year in qinput
        
        # Two queries for temp table:
        # - without ter_code (actually implies all ter_code's) for all wanted rows
        # - with ter_code for the wanted regions
        qinput_tc = copy.deepcopy( qinput )
        ter_codes = qinput_tc.pop( "ter_code", None )
        logging.error( "ter_code: %s" % ter_codes )
        
        sql_query, eng_data = aggregate_1year( qinput, count_dots, do_subclasses, separate_tc )
        entry_list = execute_1year( sql_query, eng_data, download_key )
        
        sql_query_tc, eng_data_tc = aggregate_1year( qinput_tc, count_dots, do_subclasses, separate_tc )
        entry_list_tc = execute_1year( sql_query_tc, eng_data_tc, download_key )
        
        params = {
            "language"       : language,
            "classification" : classification,
            "datatype"       : datatype,
            "base_year"      : base_year,
            "ter_codes"      : ter_codes
        }
        entry_list_sorted = temp_table( params, entry_list, entry_list_tc )
        
        json_string, cache_except = json_cache( entry_list, language, "data", download_key, qinput )
        
        if cache_except is not None:
            logging.error( "caching of aggregation data failed" )
            logging.error( "length of json string: %d" % len( json_string ) )
            # try to show the error in download sheet
            #entry_list_ = [ { "cache_except" : cache_except } ]
            #json_string, cache_except = json_cache( entry_list_, language, "data", download_key, qinput )
        
        logging.debug( "aggregated json_string: \n%s" % json_string )
        
        collect_docs( qinput, download_dir, download_key )  # collect doc files in download dir
    
    elif classification == "modern":
        # modern does not have a base_year in qinput, wants all years; 
        # add base_years one-by-one to qinput, and accumulate results.
        entry_list = []
        base_years = [ "1795", "1858", "1897", "1959", "2002" ]
        for base_year in base_years:
            logging.debug( "base_year: %s" % base_year )
            qinput[ "base_year" ] = base_year   # add base_year to qinput
            sql_query1, eng_data1 = aggregate_1year( qinput, count_dots, do_subclasses, separate_tc )
            entry_list1 = execute_1year( sql_query1, eng_data1, download_key )
            logging.debug( "entry_list1 for %s: \n%s" % ( base_year, str( entry_list1 ) ) )
            entry_list.extend( entry_list1 )
            
        json_string, cache_except = json_cache( entry_list, language, "data", download_key, qinput )
        
        if cache_except is not None:
            logging.error( "caching of aggregation data failed" )
            logging.error( "length of json string: %d" % len( json_string ) )
        logging.debug( "aggregated json_string: \n%s" % json_string )
        
        collect_docs( qinput, download_dir, download_key )  # collect doc files in download dir
    
    logging.debug( "stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "aggregation took %s" % str_elapsed )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



# Filecatalog - Create filecatalog download link
@app.route( "/filecatalogdata", methods = [ "POST", "GET" ]  )
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
    
    args = {}
    if request.method == "GET":
        args = request.args
    elif request.method == "POST":
        args = request.form
    
    for key in args:
        value = args[ key ]
        logging.debug( "key: %s, value: %s" % ( key, str( value ) ) )
        if key.startswith( "subtopics" ):
            subtopics.append( value )
    
    language = args.get( "lang" )
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
    
    configparser = get_configparser()
    try:
        config_fname = configparser.config.get( "config_fname" )
        logging.debug( "config_fname: %s" % config_fname )
    except:
        logging.debug( "configparser: %s" % str( configparser ) )
    
    tmp_dir    = configparser.get( "config", "tmppath" )
    time_limit = int( configparser.get( "config", "time_limit" ) )
    
    top_download_dir = os.path.join( tmp_dir, "download" )
    logging.debug( "top_download_dir: %s" % top_download_dir )
    if not os.path.exists( top_download_dir ):
        os.makedirs( top_download_dir )
    else:
        logging.debug( "time_limit: %d" % time_limit )
        cleanup_downloads( top_download_dir, time_limit )   # remove too old downloads
    
    random_key = str( "%05.8f" % random.random() )          # used as base name for zip download
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
        
        if language == "en":
            csv_subdir = "csv-en"
            extra = "-en"
        else:
            csv_subdir = "csv"
            extra = ""
        
        csv_dir = os.path.join( tmp_dir, "dataverse", csv_subdir, handle_name )
        csv_filename = "ERRHS_%s_data_%s%s.csv" % ( datatype, base_year, extra )
        
        logging.debug( "csv_filename: %s" % csv_filename )
        csv_pathname = os.path.join( csv_dir, csv_filename )
        logging.debug( "csv_pathname: %s" % csv_pathname )

        # process csv file
        to_xlsx = True
        process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx )
        
        if to_xlsx:
            logging.debug( "skipped for download: %s" % csv_filename )
        else:
            # also copy csv for download
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



# Filecatalog - Get filecatalog download zip
@app.route( "/filecatalogget", methods = [ "POST", "GET" ] )
def filecatalogget():
    logging.debug( "/filecatalogget" )
    logging.debug( "request.args: %s" % str( request.args ) )
    zip_filename = request.args.get( "zip" )
    logging.debug( "zip_filename: %s" % zip_filename )

    configparser = get_configparser()
    tmp_dir = configparser.get( "config", "tmppath" )
    top_download_dir = os.path.join( tmp_dir, "download" )
    zip_pathname = os.path.join( top_download_dir, zip_filename )
    logging.debug( "zip_pathname: %s" % zip_pathname )

    #json_string = str( {} )
    #return Response( json_string, mimetype = "application/json; charset=utf-8" )

    #return send_file( zip_pathname )
    return send_file( zip_pathname, attachment_filename = zip_filename, as_attachment = True )



# Still in use?
@app.route( "/download" )
def download():
    logging.debug( "/download %s" % request.args )
    
    configparser = get_configparser()
    dataverse_root = configparser.get( "config", "dataverse_root" )
    ristatkey = configparser.get( "config", "ristatkey" )
    
    logging.debug( "dataverse_root: %s" % dataverse_root )
    logging.debug( "ristatkey: %s" % ristatkey )
    
    id_ = request.args.get( "id" )
    logging.debug( "id_: %s" % id_ )
    
    if id_:
        url = "https://%s/api/access/datafile/%s?key=%s&show_entity_ids=true&q=authorName:*" % ( dataverse_root, id_, ristatkey )
        
        logging.debug( "url: %s" % url )
        
        f = urllib2.urlopen( url )
        data = f.read()
        filetype = "application/pdf"
        
        if request.args.get( "filetype" ) == "excel":
            filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
		
        logging.debug( "filetype: %s" % filetype )
        return Response( data, mimetype = filetype )
    
    key = request.args.get( "key" )
    logging.debug( "key: %s" % key )
    
    if key:
        zipping = True
        logging.debug( "download() zip: %s" % zipping )
    
        tmp_dir    = configparser.get( "config", "tmppath" )
        time_limit = int( configparser.get( "config", "time_limit" ) )
        top_download_dir = os.path.join( tmp_dir, "download" )
        logging.debug( "top_download_dir: %s" % top_download_dir )
        cleanup_downloads( top_download_dir, time_limit )       # remove too old downloads
        download_dir = os.path.join( top_download_dir, key )    # current download dir
    
        logging.debug( "download() key: %s" % key )
        clientcache = MongoClient()
        datafilter = {}
        datafilter[ "key" ] = key
        ( lex_lands, vocab_regs_terms, sheet_header, topic_name, qinput ) = preprocessor( datafilter )
        
        xlsx_name = "%s.xlsx" % key
        pathname, msg = aggregate_dataset( key, download_dir, xlsx_name, lex_lands, vocab_regs_terms, sheet_header, topic_name, qinput )
        if os.path.isfile( pathname ):
            logging.debug( "pathname: %s" % pathname )
        else:
            return str( { "msg": "%s" % msg } )
        
        with open( pathname, "rb" ) as f:
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
                        info.date_time = localtime( time() )[ :6 ]
                        info.compress_type = zipfile.ZIP_DEFLATED
                        # Notes from the web and zipfile sources:
                        # external_attr is 32 in size, with the unix permissions in the
                        # high order 16 bit, and the MS-DOS FAT attributes in the lower 16.
                        # man 2 stat tells us that 040755 should be a drwxr-xr-x style file,
                        # and word of mouth tells me that bit 4 marks directories in FAT.
                        info.external_attr = (0664 << 16)       # -rw-rw-r--
                        
                        file_path = os.path.join( root, fname )
                        with open( file_path, "rb" ) as f:
                            datacontents = f.read()
                            zf.writestr( info, datacontents )
            memory_file.seek( 0 )
            return send_file( memory_file, attachment_filename = zip_filename, as_attachment = True )
        
        dbcache = clientcache.get_database( "datacache" )
        result = dbcache.data.find( { "key": str( request.args.get( "key" ) ) } )
        for item in result:
            del item[ "key" ]
            del item[ "_id" ]
            dataset = json.dumps( item, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
            return Response( dataset, mimetype = "application/json; charset=utf-8" )
    else:
        return str( { "msg" : "Argument 'key' not found" } )



# Get cron autoupdate log to inspect for errors
@app.route( "/logfile" )
def getupdatelog():
    logging.debug( "/logfile" )
    
    configparser = get_configparser()
    etl_dir = configparser.get( "config", "etlpath" )
    
    log_filename = "autoupdate.log"
    log_pathname = os.path.join( etl_dir, log_filename )
        
    return send_file( log_pathname, attachment_filename = log_filename, as_attachment = True )



if __name__ == "__main__":
    app.run()

# [eof]
