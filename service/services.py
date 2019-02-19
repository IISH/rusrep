# -*- coding: utf-8 -*-

"""
VT-07-Jul-2016 latest change by VT
FL-12-Dec-2016 use datatype in function documentation()
FL-20-Jan-2017 utf8 encoding
FL-05-Aug-2017 cleanup function load_vocabulary()
FL-06-Feb-2018 reordering optional
FL-06-Mar-2018 reorder sql_query building
FL-26-Mar-2018 handle dataverse connection failure
FL-04-Apr-2018 rebuilt postgres query
FL-17-Apr-2018 group pg items by identifier
FL-07-May-2018 GridFS for large BSON
FL-08-May-2018 /years URL now with extra classification parameter
FL-18-Sep-2018 /topics new implementation
FL-30-Oct-2018 document make_query msg options
FL-06-Nov-2018 continue with group_tercodes = True
FL-19-Nov-2018 RUSREPS-216
FL-27-Nov-2018 collect_fields() & collect_records()
FL-10-Dec-2018 handle /documentation exception
FL-11-Feb-2019 optional suppression of trailing dots in db queries
FL-12-Feb-2019 separate functions for old/new historic/modern
FL-19-Feb-2019 main query split by subclasses, other 2 by indicator length

def get_configparser():
def get_connection():
def class_collector( keywords ):
def strip_subclasses( path ):
def group_levels( path_list ):
def json_generator( params, sql_names, json_dataname, data, qkey_set = None ):
def json_cache( entry_list, params, download_key ):
def collect_docs( params, download_dir, download_key ):
def load_years( cursor, datatype, classification ):
def sqlfilter( sql ):
def sqlconstructor( sql ):
#def topic_counts( schema ):    # obsolete, remove
#def load_topics( qinput ):     # obsolete, remove
def topic_counts( language, datatype ):
def dataset_filter( data, sql_names, classification ):
def zap_empty_classes( item ):
def translate_item( item, eng_data ):
def load_vocabulary( vocab_type ):
def translate_vocabulary( vocab_filter, classification = None ):
def get_sql_where( name, value ):
def loadjson( json_dataurl ):
#def filecat_subtopic( qinput, cursor, datatype, base_year ):
def process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx ):
def aggregate_year( params, add_subclasses, value_total = True, value_numerical = True ):
def execute_year( params, sql_query, eng_data ):
def execute_only( sql_query, dict_cursor = False ):
def collect_fields(  params, eng_data, sql_names, sql_resp ):
def collect_records( record_dict_total, prefix, path_dict, params, eng_data, sql_names, sql_resp ):
def add_missing( record_dict_total, params ):
def sort_records( record_dict_total, params ):
def records2oldentries( records_dict, params ):
#def merge_3records( record_dict_num, record_dict_ntc, record_dict_none ):
#def merge_2records( record_dict_total, record_dict_path ):
def show_record_dict( dict_name, record_dict, sort = False ):
def show_record_list( list_name, record_list ):
def add_unique_items( language, list_name, entry_list_collect, entry_list_none ):
def add_unique_items_grouped( language, dict_name, entry_dict_collect, entry_dict_none )
def remove_dups( entry_list_collect ):
def sort_entries( datatype, entry_list ):
#def reorder_entries( params, entry_list_ntc, entry_list_none, entry_list = None)
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
@app.route( "/aggregation", methods = ["POST" ] )               def aggregation():
def aggregate_historic_redun( params ):
def aggregate_historic( params )
def aggregate_modern_redun( params )
def aggregate_modern( params )
def make_identifier( path, value_unit ):
def group_by_ident( entry_list ):
def make_query( prefix, params, add_subclasses, value_total = True, value_numerical = True ):
def show_path_dict( num_path_lists, pd, path_dict ):
def show_params( info, params ):
def show_entries( info, entries ):
def extend_nodups( tot_list, add_list ):

@app.route( "/filecatalogdata", methods = [ 'POST', 'GET' ]  )  def filecatalogdata():
@app.route( "/filecatalogget", methods = [ 'POST', 'GET' ] )    def filecatalogget():
@app.route( "/download" )                                       def download():
@app.route( "/logfile" )                                        def getupdatelog():
"""


# future-0.17.1 imports for Python 2/3 compatibility
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
import gridfs
import json
import logging
import os
os.environ[ "MPLCONFIGDIR" ] = "/tmp"   # matplotlib (used by pandas) needs tmp a dir
import pandas as pd
import random
import re
import shutil
import simplejson
import sqlite3
import time
import uuid
import urllib
import urllib2
import psycopg2
import psycopg2.extras
import zipfile

from datetime import date
from io import BytesIO
from flask import Flask, jsonify, Response, request, send_from_directory, send_file, session
from jsonmerge import merge
from operator import itemgetter
from pymongo import MongoClient
from socket import gethostname
#from sortedcontainers import SortedDict
from StringIO import StringIO
from sys import exc_info
from time import ctime, time, localtime

from dataverse import Connection
from excelmaster import aggregate_dataset_fields, aggregate_dataset_records, preprocessor

from configutils import DataFilter

sys.path.insert( 0, os.path.abspath( os.path.join( os.path.dirname( "__file__" ), "./" ) ) )

use_gridfs = True

forbidden = [ "classification", "action", "language", "path" ]
entry_debug = False
vocab_debug = False

#do_translate = True
#do_translate = False   # read from English db for language = "en"


def get_configparser():
    RUSSIANREPO_CONFIG_PATH = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]
    logging.debug( "RUSSIANREPO_CONFIG_PATH: %s" % RUSSIANREPO_CONFIG_PATH )
    
    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    logging.debug( "configpath: %s" % configpath )
    
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



def class_collector( keywords, key_list ):
    logging.debug( "class_collector()" )
    logging.debug( "keywords: %s" % keywords )
    logging.debug( "key_list: %s" % key_list )
    
    nkeys = len( key_list )
    class_dict  = {}
    normal_dict = {}
    
    for item in keywords:
        logging.debug( "item: %s" % item )
        class_match = re.search( r'class', item )
        
        if class_match:
            logging.debug( "class: %s" % item )
            class_dict[ item ] = keywords[ item ]
        else:
            normal_dict[ item ] = keywords[ item ]
    
    # key_list contains all path keys from the input query. 
    # append items with empty path keys if they are 'missing'; 
    # because missing path elements disturb sorting
    class_keys = class_dict.keys()
    if len( class_keys ) < nkeys:
        for key in key_list:
            if not key in class_keys:
                class_dict[ key ] = ''  # add 'missing' key with empty value
    
    logging.debug( "class_dict:  %s" % class_dict )
    logging.debug( "normal_dict: %s" % normal_dict )
    return ( class_dict, normal_dict )



def strip_subclasses( path ):
    logging.debug( "strip_subclasses()" )
    """
    FL-28-Nov-2017
    path dict entries may have a subclasses:True parameter, meaning that in the 
    GUI at level 4 the double checkbox was checked. If at least 1 entry with 
    subclasses:True is encountered, we return add_subclasses = True, otherwise 
    False. 
    """
    
    logging.debug( "path: (%s) %s" % ( type( path ), str( path ) ) )
    
    path_stripped = []      # "subclasses" param stripped
    key_set = set()
    
    add_subclasses = False   # becomes True if subclasses were removed
    
    for old_entry in path:
        logging.debug( "old entry: %s" % old_entry )
        stripped_entry = copy.deepcopy( old_entry )
        #stripped_entry = collections.OrderedDict( sorted( stripped_entry.items() ) )
        
        for k in old_entry:
            v = old_entry[ k ]
            #logging.debug( "k: %s, v: %s" % ( k, v ) )
            
            if k == "subclasses" and v:     # True
                del stripped_entry[ k ]
                add_subclasses = True
            
            p = k.find( "class" )
            if p != -1:
                n = k[ p+5: ]
                #logging.debug( "n: %s" % n )
                if n in [ "5", "6" ]:
                    del stripped_entry[ k ]
            
        logging.debug( "new entry: %s" % stripped_entry )
        path_stripped.append( stripped_entry )
        
        keys = stripped_entry.keys()
        for k in keys:
            key_set.add( k )
        
    key_set = sorted( key_set )     # like them [hist]class1, 2, 3 ...
    
    logging.debug( "new path: (%s) %s" % ( type( path_stripped ), str( path_stripped ) ) )
    logging.debug( "key_set: %s" % key_set )
    logging.debug( "strip_subclasses() return" )
    
    return add_subclasses, path_stripped, key_set



def group_levels( path_list ):
    logging.debug( "group_levels()" )
    """
    Split the path into groups of the same 'length': 
    Level 1, Level 1+2, Level 1+2+3, ...
    """
    
    path_list1 = []
    path_list2 = []
    path_list3 = []
    path_list4 = []
    path_list5 = []
    
    for path in path_list:
        keys = path.keys()
        nkeys = len( path.keys() )
        
        subclasses = False
        if "subclasses" in keys:
            subclasses = True
            del path[ "subclasses" ]
        
        if nkeys == 1:
            path_list1.append( path )
        elif nkeys == 2:
            path_list2.append( path )
        elif nkeys == 3:
            path_list3.append( path )
        elif nkeys == 4:
            path_list4.append( path )
        elif nkeys == 5:    # now 4, subclasses has been dropped
            path_list5.append( path )
    
    """
    logging.debug( "path_list1: %d entries with 1 indicator key" % len( path_list1 ) )
    for p, path in enumerate( path_list1 ):
        logging.debug( "%d: %s" % ( p+1, str( path ) ) )
    
    logging.debug( "path_list2: %d entries with 2 indicator keys" % len( path_list2 ) )
    for p, path in enumerate( path_list2 ):
        logging.debug( "%d: %s" % ( p+1, str( path ) ) )
    
    logging.debug( "path_list3: %d entries with 3 indicator keys" % len( path_list3 ) )
    for p, path in enumerate( path_list3 ):
        logging.debug( "%d: %s" % ( p+1, str( path ) ) )
    
    logging.debug( "path_list4: %d entries with 4 indicator keys" % len( path_list4 ) )
    for p, path in enumerate( path_list4 ):
        logging.debug( "%d: %s" % ( p+1, str( path ) ) )
    
    logging.debug( "path_list5: %d entries with 5 indicator keys" % len( path_list5 ) )
    for p, path in enumerate( path_list5 ):
        logging.debug( "%d: %s" % ( p+1, str( path ) ) )
    """
    
    path_lists_bylen = []
    
    if len( path_list1 ) > 0:
        path_lists_bylen.append( { "nkeys" : 1, "subclasses" : False, "path_list" : path_list1 } )
    if len( path_list2 ) > 0:
        path_lists_bylen.append( { "nkeys" : 2, "subclasses" : False, "path_list" : path_list2 } )
    if len( path_list3 ) > 0:
        path_lists_bylen.append( { "nkeys" : 3, "subclasses" : False, "path_list" : path_list3 } )
    if len( path_list4 ) > 0:
        path_lists_bylen.append( { "nkeys" : 4, "subclasses" : False, "path_list" : path_list4 } )
    if len( path_list5 ) > 0:
        path_lists_bylen.append( { "nkeys" : 5, "subclasses" : True,  "path_list" : path_list5 } )
    
    path_list_subyes = []
    path_list_subno = []
    
    for path_dict in path_lists_bylen:
        logging.debug( "%s" % str( path_dict ) )
        nkeys      = path_dict[ "nkeys" ]
        subclasses = path_dict[ "subclasses" ]
        path_list  = path_dict[ "path_list" ]
        npaths     = len( path_list )
        
        logging.debug( "npaths: %d, nkeys: %d, subclasses: %s" % ( npaths, nkeys, subclasses ) )
        for p, path in enumerate( path_list ):
            logging.debug( "%d: %s" % ( p+1, str( path ) ) )
    
        if subclasses:
            path_list_subyes.extend( path_list )
        else:
            path_list_subno.extend( path_list )
    
    
    logging.debug( "npaths with    subclasses: %d" % len( path_list_subyes ) )
    logging.debug( "npaths without subclasses: %d" % len( path_list_subno ) )
    
    path_lists_bysub = []
    # "nkeys" : -1 == variable
    if len( path_list_subyes ) > 0:
        path_lists_bysub.append( { "nkeys" : -1, "subclasses" : True,  "path_list" : path_list_subyes } )
    if len( path_list_subno ) > 0:
        path_lists_bysub.append( { "nkeys" : -1, "subclasses" : False, "path_list" : path_list_subno } )
    
    return path_lists_bylen, path_lists_bysub



def json_generator( params, sql_names, json_dataname, data, qkey_set = None ):
    time0 = time()      # seconds since the epoch
    logging.debug( "generator() start: %s" % datetime.datetime.now() )
    
    logging.debug( "json_generator() json_dataname: %s, # of data items: %d" % ( json_dataname, len( data ) ) )
    logging.debug( "data: %s" % data )
    
    language       = params.get( "language" )
    classification = params.get( "classification" )
    datatype       = params.get( "datatype" )
    datatype_      = datatype[ 0 ] + "_00"
    base_year      = params.get( "base_year" )
    path_list      = params.get( "path" )
    add_subclasses = params.get( "add_subclasses" )
    etype          = params.get( "etype", "" )
    
    logging.debug( "language       : %s" % language )
    logging.debug( "classification : %s" % classification )
    logging.debug( "datatype       : %s" % datatype )
    logging.debug( "base_year      : %s" % base_year )
    logging.debug( "add_subclasses : %s" % add_subclasses )
    
    if path_list:
        logging.debug( "# entries in path_list: %d" % len( path_list ) )
        for pe, path_entry in enumerate( path_list ):
            logging.debug( "%d %s" % ( pe+1, path_entry ) )
    else:
        logging.debug( "NO path_list" )
    
    forbidden = { "data_active", 0, "datarecords", 1 }
    
    # collect all data class keys
    key_set = set()
    dkey_set = set()
    len_data = len( data )
    for idx, value_str in enumerate( data ):
        logging.debug( "n: %d-of-%d, value_str: %s" % ( 1+idx, len_data, str( value_str ) ) )
        for i in range( len( value_str ) ):
            name  = sql_names[ i ]
            value = value_str[ i ]
            
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, name, value ) )
                # ". " marks a trailing dot in histclass or class: skip
                continue
            else:
                if "class" in name and len( value ) > 0:
                    dkey_set.add( name )
    
    entry_list = []
    logging.debug( "# values in data: %d" % len_data )
    for idx, value_str in enumerate( data ):
        logging.debug( "n: %d-of-%d, value_str: %s" % ( 1+idx, len_data, str( value_str ) ) )
        data_keys    = {}
        extra_values = {}
        for i in range( len( value_str ) ):
            name  = sql_names[ i ]
            value = value_str[ i ]
            
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, name, value ) )
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
                extra_values[ name ] = value
        
        # If aggregation check data output for "NA" values
        if "total" in data_keys:
            if extra_values[ "data_active" ]:
                if language == "en":
                    data_keys[ "total" ] = "NA"
                elif language == "rus":
                    data_keys = "непригодный"
        
        if len( dkey_set ) > len( qkey_set ):
            key_set = dkey_set
        else:
            key_set = qkey_set
        logging.debug( "qkey_set: %s, dkey_set: %s" % ( qkey_set, dkey_set ) )
        
        ( path, output ) = class_collector( data_keys, key_set )
        output[ "path" ] = path
        #output[ "etype" ] = etype
        
        entry_list.append( output )
    
    #value_unit = ''
    logging.debug( "# of entries in entry_list: %d" % len( entry_list ) )
    for json_entry in entry_list:
        logging.debug( "json_entry: %s" % json_entry )
        
        # value_unit may vary, so we cannot use it for entries created by ourselves
        #value_unit = json_entry.get( "value_unit" )
        
        # compare qinput paths with db returned paths; add missing paths (fill with NA values). 
        entry_path = json_entry.get( "path" )
        # path_list from qinput does not contain our added [hist]classes; 
        # remove our additions to sql from entry_path before comparison
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
    
    """
    if len( path_list ) != 0:
        # pure '.' dot entries are not returned from db
        logging.debug( "missing path entries: %d" % len( path_list ) )
        for path_entry in path_list:
            logging.debug( "path_entry: %s" % path_entry )
            new_entry = {}
            # also want to see "NA" entries in preview and download
            
            new_path = path_entry
            entry_keys = path_entry.keys()
            for key in key_set:
                if not key in entry_keys:
                    new_path[ key ] = ''    # add 'missing' key wth empty value
            
            new_entry[ "path" ]       = new_path
            new_entry[ "base_year" ]  = base_year
            new_entry[ "value_unit" ] = '?'     # value_unit
            new_entry[ "datatype" ]   = datatype
            new_entry[ "count" ]      = 1       # was ''
            new_entry[ "ter_code" ]   = ''
            new_entry[ "total" ]      = ''      # unknown, so not 0 or 0.0
            entry_list.append( new_entry )
    """
    
    logging.debug( "json_generator() done, %d entries" % len( entry_list ) )
    for e, entry in enumerate( entry_list ):
        logging.debug( " %d: %s" % ( e, str( entry ) ) )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "json_generator() caching took %s" % str_elapsed )
    
    return entry_list



def json_cache( entry_list, params, download_key ):
    # cache entry_list in mongodb with download_key as key
    
    time0 = time()      # seconds since the epoch
    logging.debug( "json_cache() start: %s" % datetime.datetime.now() )
    
    logging.debug( "json_cache() # entries in entry_list: %d" %  len( entry_list ) )
    logging.debug( "json_cache() params: %s" % str( params ) )
    
    configparser = get_configparser()
    root = configparser.get( "config", "root" )
    
    json_hash = {}
    json_hash[ "language" ] = params[ "language" ]
    json_dataname = "data"
    json_hash[ json_dataname ] = entry_list
    
    if not params[ "redundant" ]:   # new
        json_hash[ "classification" ] = params[ "classification" ]
        json_hash[ "datatype" ]       = params[ "datatype" ]
        json_hash[ "base_year" ]      = params[ "base_year" ]
    
    json_hash[ "url" ] = "%s/service/download?key=%s" % ( root, download_key )
    logging.debug( "json_hash: %s" % json_hash )
    
    json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    
    exc_value = None
    try:
        cache_data = json_hash
        del cache_data[ "url" ]
        
        cache_data[ "key" ] = download_key
        if params:
            cache_data[ "params" ] = params
        
        logging.debug( "# keys in cache_data: %s" % len( cache_data.keys() ) )
        for key, value in cache_data.iteritems():
            #logging.debug( "key: %s, value: %s" % ( key, value ) )
            if isinstance( value, list ):
                logging.debug( "key %s: value type: %s, # of elements: %d" % ( key, type( value ), len( value ) ) )
            elif isinstance( value, dict ):
                logging.debug( "key %s: value type: %s, # of keys: %d" % ( key, type( value ), len( value ) ) )
            else:
                logging.debug( "key %s: value type: %s" % ( key, type( value ) ) )
        
        logging.debug( "cache data with key: %s" % download_key )
        clientcache = MongoClient()
        db_cache = clientcache.get_database( "datacache" )
        
        if use_gridfs:
            json_str = json.dumps( cache_data, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
            fs_cache = gridfs.GridFS( db_cache )
            gridfs_id = fs_cache.put( json_str, encoding = "utf8", _id = download_key )
            if gridfs_id != download_key:
                logging.error( "gridfs key: %s" % gridfs_id )
            else:
                logging.debug( "gridfs key: %s" % gridfs_id )
        else:
            result = db_cache.data.insert( cache_data )
        
    except:
        logging.error( "caching with key %s failed:" % download_key )
        type_, exc_value, tb = sys.exc_info()
        logging.error( "%s" % exc_value )
        
        exc_value_str = repr( exc_value )
        
        if exc_value_str.startswith( "DocumentTooLarge" ):   # use GridFS
            logging.error( "DocumentTooLarge" )
            json_hash = {}
            json_hash[ "key" ] = download_key
            json_hash[ "params" ] = params
            json_hash[ "msg" ] = exc_value_str
            json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
        else:
            logging.error( "%s" % exc_value_str )
    
    logging.debug( "stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "json_cache() caching took %s" % str_elapsed )
    
    return json_string, exc_value



def collect_docs( qinput, download_dir, download_key ):
    # collect the accompanying docs in the download dir
    time0 = time()      # seconds since the epoch
    logging.debug( "collect_docs() start: %s" % datetime.datetime.now() )
    logging.info( "collect_docs() key = %s, dir = %s" % ( download_key, download_dir ) )
    
    for key in qinput:
        logging.debug( "key: %s, value: %s" % ( key, qinput[ key ] ) )
        
    classification = qinput.get( "classification" )
    language       = qinput.get( "language" )
    datatype       = qinput.get( "datatype" )
    
    #if language is None:
    #    language = "en"
    
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
                #if doc.find( "NACE 1.1_Classification") != -1 and datatype0 in [ "3", "4", "5"]:
                #    get_list.append( doc )
                
                base_year = qinput.get( "base_year" )
                if doc.find( str( base_year ) ) != -1:          # base_year
                    if doc.find( "GovReports" ) != -1:          # string
                        get_list.append( doc )
                    
                    if doc.find( datatype_ ) != -1:             # datatype
                        get_list.append( doc )
            
            elif classification == "modern":                    # modern: most lang docs
                if doc.find( "NACE 1.1_Classification") != -1 and ( datatype == "2.03" or datatype0 in [ "3", "4", "5"] ):
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

    logging.debug( "stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "collect_docs() caching took %s" % str_elapsed )



def load_years( cursor, datatype, classification ):
    """
    return a json dictionary with record counts from table russianrepository for the given datatype
    """
    logging.debug( "load_years()" )
    
    config_parser = get_configparser()
    years = config_parser.get( "config", "years" ).split( ',' )
    
    language = "en"
    dbtable_name = "dbtable" + '_' + language
    dbtable  = config_parser.get( "config", dbtable_name )
    
    sql = "SELECT base_year, COUNT(*) AS cnt FROM %s" % dbtable
    
    if datatype:
        sql += " WHERE datatype = '%s'" % datatype 
    
    if classification == "modern":
        sql += " AND class1 <> 'no modern classification for this datatype'"
    
    sql += " GROUP BY base_year ORDER BY base_year;"
    
    logging.info( sql )
    
    cursor.execute( sql )
    resp = cursor.fetchall()
    result = collections.OrderedDict()
        
    for val in resp:
        if val[ 0 ]:
            result[ val[ 0 ] ] = val[ 1 ]
    for year in years:
        if int( year ) not in result:
            result[ int( year ) ] = 0
    logging.info( "result: %s" % result )
    
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


"""
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
"""

"""
def load_topics( qinput ):
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
    
    entry_list_in = json_generator( qinput, sql_names, "data", sql_resp )
    
    entry_list_out = []
    for topic_dict in entry_list_in:
        logging.debug( topic_dict )
        datatype = topic_dict[ "datatype" ]
        topic_dict[ "byear_counts" ] = all_cnt_dict[ datatype ]
        entry_list_out.append(topic_dict )
    
    return entry_list_out
"""


def topic_counts( language, datatype ):
    logging.debug( "topic_counts() datatype: %s" % datatype )

    connection = get_connection()
    cursor = connection.cursor( cursor_factory = psycopg2.extras.NamedTupleCursor )

    sql_count  = "SELECT base_year, COUNT(*) AS count FROM russianrepo_%s" % language
    sql_count += " WHERE datatype = '%s'" % datatype
    sql_count += " GROUP BY base_year ORDER BY base_year"
    logging.debug( sql_count )
    
    cursor.execute( sql_count )
    sql_count_resp = cursor.fetchall()
    count_dict = {}
    for count_rec in sql_count_resp:
        logging.debug( "count_rec: %s" % str( count_rec ) )
        count_dict[ count_rec.base_year ] = int( count_rec.count )    # strip trailing 'L'
    
    logging.debug( "count_dict: %s" % count_dict )
    
    return count_dict



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



def load_vocabulary( vocab_type, language, datatype, base_year ):
    logging.debug( "load_vocabulary() vocab_type: %s, language: %s, datatype: %s, base_year: %s" % 
        ( vocab_type, language, datatype, base_year ) )
    
    vocab_filter = {}
    
    if vocab_type == "topics":
        vocab_name = "ERRHS_Vocabulary_topics"
    
    elif vocab_type == "regions":
        vocab_name = "ERRHS_Vocabulary_regions"
        if base_year:
            vocab_filter[ "basisyear" ] = base_year
    
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
    
    """
    if do_translate and language == "en":
    #if language == "en":
        eng_data = translate_vocabulary( vocab_filter )
        logging.debug( "translate_vocabulary eng_data items: %d" % len( eng_data ) )
        logging.debug( "eng_data: %s" % eng_data )
        
        units = translate_vocabulary( { "vocabulary": "ERRHS_Vocabulary_units" } )
        logging.debug( "translate_vocabulary units items: %d" % len( units ) )
        logging.debug( "units: %s" % units )
        
        for item in units:
            eng_data[ item ] = units[ item ]
            #logging.debug( "%s => %s" % ( item, units[ item ] ) )
    """
    
    params = {}
    if vocab_type == "topics":
        params[ "vocabulary" ] = vocab_name
    elif vocab_type == "regions":
        params[ "vocabulary" ] = vocab_name
        if base_year:
            params[ "basisyear" ] = base_year
    else:
        params[ "vocabulary" ] = vocab_type
        if base_year:
            params[ "base_year" ] = base_year
        params[ "datatype" ] = datatype
    
    logging.debug( "params: %s" % params )
    
    client = MongoClient()
    db_name = "vocabulary"
    if "classes" in vocab_name:
        db_name += ( '_' + language )
    logging.debug( "db_name: %s" % db_name )
    
    db = client.get_database( db_name )     # get the vocabulary db
    vocab = db.data.find( params )          # apply the filter parameters
    
    data = []
    uid = 0
    logging.debug( "processing %d items in vocab %s" % ( vocab.count(), vocab_type ) )
    for item in vocab:
        logging.debug( "item: %s" % item )
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
            
            ( path, output ) = class_collector( item )
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
    for i, item in enumerate( vocab ):
        logging.debug( "%d: %s" % ( i, item ) )
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
                
                # 2-way key/values; E->R & R->E
                data[ item[ "RUS" ] ] = item[ "EN" ]
                data[ item[ "EN" ] ]  = item[ "RUS" ] 
                
                if vocab_debug:
                    logging.debug( "%d EN: |%s| RUS: |%s|" % ( i, item[ "EN" ], item[ "RUS" ] ) )
            except:
                type_, value, tb = exc_info()
                logging.error( "translate_vocabulary failed: %s" % value )
    
    if vocab_debug:
        for k, key in enumerate( data ):
            logging.debug( "%d: key: %s, value: %s" % ( k, key, data[ key ] ) )
    
    logging.debug( "translate_vocabulary: return %d items" % len( data ) )
    
    return data



def get_sql_where( name, value ):
    logging.debug( "get_sql_where() name: %s, value: %s" % ( name, value ) )
    
    sql_query = ''
    #result = re.match( "\[(.+)\]", value )
    result = re.match( "\[(.+)\]", str( value ) )
    
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


"""
def filecat_subtopic( qinput, cursor, datatype, base_year ):
    logging.debug( "filecatalog_subtopic()" )
    
    query  = "SELECT * FROM russianrepository"
    query += " WHERE datatype = '%s' AND base_year = '%s'" % ( datatype, base_year )
    query += " ORDER BY ter_code"
    
    cursor.execute( query )
    sql_resp = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    
    entry_list = json_generator( qinput, sql_names, "data", sql_resp )
    logging.debug( entry_list )
    
    return entry_list
"""


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
    
    delimiter = b'|'    # leading b required with __future__, otherwise: TypeError: "delimiter" must be an 1-character string
    with open( csv_pathname, "rb" ) as csv_file:
        csv_reader = csv.reader( csv_file, delimiter = delimiter )
        for row in csv_reader:
            logging.debug( ", ".join( row ) )
    
    if to_xlsx:
        #sep = str( u'|' ).encode( "utf-8" )
        #sep = str( delimiter ).encode( "utf-8" )       # "encode method has been disabled in newbytes"
        sep = delimiter
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
        
        writer = pd.ExcelWriter( xlsx_pathname )
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



def aggregate_year( params, add_subclasses, value_total = True, value_numerical = True ):
    logging.debug( "aggregate_year() add_subclasses: %s" % add_subclasses )
    logging.debug( "params %s" % str( params ) )
    
    language       = params.get( "language" )
    datatype       = params.get( "datatype" ) 
    classification = params.get( "classification" )
    base_year      = params.get( "base_year" )
    
    #forbidden = [ "classification", "action", "language", "path" ]
    #forbidden = [ "classification", "action", "language", "path", "ter_codes", "add_subclasses" ]
    forbidden = [ "classification", "action", "language", "path", "add_subclasses", "etype" ]
    
    eng_data = {}
    
    """
    if do_translate and language == "en":
        # translate input english term to russian sql terms
        vocab_filter = {}
    
        if base_year and classification == "historical":
            vocab_filter[ "YEAR" ] = base_year
        
        if datatype:
            if classification == "historical":
                vocab_filter[ "DATATYPE" ] = datatype
            elif classification == "modern":
                vocab_filter[ "DATATYPE" ] = "MOD_" + datatype
        logging.debug( "vocab_filter: %s" % str( vocab_filter ) )
        
        eng_data = translate_vocabulary( vocab_filter )
        logging.debug( "translate_vocabulary returned %d eng_data items" % len( eng_data ) )
        #logging.debug( "eng_data: %s" % str( eng_data ) )
        for i, item in enumerate( eng_data ):
            logging.debug( "%d: %s" % ( i, item ) )
        
        units = translate_vocabulary( { "vocabulary": "ERRHS_Vocabulary_units" } )
        logging.debug( "translate_vocabulary returned %d units items" % len( units ) )
        #logging.debug( "units: %s" % str( units ) )
        for item in units:
            eng_data[ item ] = units[ item ]
    """
    
    sql = {}
    
    sql[ "where" ]     = ''
    sql[ "condition" ] = ''
    known_fields       = {}
    
    sql[ "internal" ]  = ''
    sql[ "group_by" ]  = ''
    sql[ "order_by" ]  = ''
    
    for name in params:
        logging.info( "name: %s" % name )
        if not name in forbidden:
            value = params[ name ]
            logging.info( "value: %s" % value )
            
            #if value in eng_data:
            #    value = eng_data[ value ]
            #    logging.debug( "eng_data name: %s, value: %s" % ( name, value ) )
            
            # temporary fix, sql composition must be overhauled
            name_ = name
            if name == "ter_codes":
                name_ = "ter_code"      # name of db column
            
            sql[ "where" ] += "%s AND " % get_sql_where( name_, value )
            sql[ "condition" ] += "%s, " % name_
            known_fields[ name_ ] = value
        
        elif name == "path":
            full_path = params[ name ]
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
                    #value = str( value )    # ≥5000 : \xe2\x89\xa55000 => u'\\u22655000
                    value = value.encode( "utf-8" ) # otherwise, "≥5000 inhabitants" is not found in eng_data
                    
                    logging.debug( "clear_path xkey: %s, value: %s" % ( xkey, value ) )
                    
                    if value in eng_data:
                        logging.debug( "xkey: %s, value: %s" % ( xkey, value ) )
                        value = eng_data[ value ]
                        logging.debug( "xkey: %s, value: %s" % ( xkey, value ) )
                    else:
                        logging.debug( "not found: value: %s" % value )
                        #logging.warning( "not found: value: %s" % value )
                        
                    sql_local[ xkey ] = "(%s='%s' OR %s='. '), " % ( xkey, value, xkey )
                    
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
    if add_subclasses:   # 5&6 were removed from path; add them all here
        if classification == "historical":
            extra_classes = [ "histclass5", "histclass6", "histclass7", "histclass8", "histclass9", "histclass10" ]
        elif classification == "modern":
            extra_classes = [ "class5", "class6", "class7", "class8", "class9", "class10" ]
        logging.debug( "extra_classes: %s" % extra_classes )
    
    sql_query  = "SELECT COUNT(*) AS datarecords" 
    sql_query += ", COUNT(*) - COUNT(value) AS data_active"
    
    if value_total:
        sql_query += ", SUM(CAST(value AS DOUBLE PRECISION)) AS total"
    
    if classification == "modern":  # "ter_code" keyword not in qinput, but we always need it
        logging.debug( "modern classification: adding ter_code to SELECT" )
        sql_query += ", ter_code"
    
    sql_query += ", value_unit"
    logging.debug( "sql_query 0: %s" % sql_query )

    if len( extra_classes ) > 0:
        for field in extra_classes:
            sql_query += ", %s" % field
        logging.debug( "sql_query 1: %s" % sql_query )
    
    if sql[ "where" ]:
        logging.debug( "where: %s" % sql[ "where" ] )
        sql_query += ", %s" % sql[ "condition" ]
        sql_query  = sql_query[ :-2 ]
        logging.debug( "sql_query 2: %s" % sql_query )
        
        dbtable = "russianrepo_%s" % language
        sql_query += " FROM %s WHERE %s" % ( dbtable, sql[ "where" ] )
        sql_query  = sql_query[ :-4 ]
        logging.debug( "sql_query 3: %s" % sql_query )
    
    if value_numerical:
        sql_query += " AND value <> ''"             # suppress empty values
        sql_query += " AND value <> '.'"            # suppress a 'lone' "optional point", used in the table to flag missing data
        # plus an optional single . for floating point values, and plus an optional leading sign
        sql_query += " AND value ~ '^[-+]?\d*\.?\d*$'"
    else:
        sql_query += " AND (value = '' OR value = ' ' OR value = '.' OR value = '. ' OR value = NULL)"
        
    logging.debug( "sql_query 4: %s" % sql_query )
    
    if sql[ "internal" ]:
        logging.debug( "internal: %s" % sql[ "internal" ] )
        sql_query += " AND (%s) " % sql[ "internal" ]
        logging.debug( "sql_query 5: %s" % sql_query )
    
    sql[ "group_by" ] = " GROUP BY value_unit"
    
    if not "ter_code" in known_fields: 
        sql[ "group_by" ] += ", ter_code"
    
    for field in known_fields:
        sql[ "group_by" ] += ", %s" % field
    for field in extra_classes:
        sql[ "group_by" ] += ", %s" % field
    
    logging.debug( "group_by: %s" % sql[ "group_by" ] )
    sql_query += sql[ "group_by" ]
    logging.debug( "sql_query 6: %s" % sql_query )
    
    # ordering by the db: applied to the russian contents, so the ordering of 
    # the english translation will not be perfect, but at least grouped. 
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
    
    class_list.append( "ter_code" )
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



def execute_year( key_set, params, sql_query, eng_data = {} ):
    logging.info( "execute_year()" )
    
    time0 = time()      # seconds since the epoch
    logging.debug( "execute_year() start: %s" % datetime.datetime.now() )
    
    connection = get_connection()
    cursor = connection.cursor()
    sql_query = cursor.mogrify( sql_query )     # needed if single quote has been escaped by repeating it
    cursor.execute( sql_query )
    
    logging.debug( "query execute stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.debug( "execute_year() sql_query took %s" % str_elapsed )
    
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    logging.debug( "%d sql_names:" % len( sql_names ) )
    logging.debug( sql_names )
    
    sql_resp = cursor.fetchall()
    nsql_resp = len( sql_resp )
    logging.debug( "result # of data records: %d" % nsql_resp )
    
    cursor.close()
    connection.close()
    
    final_data = []
    for idx, item in enumerate( sql_resp ):
        logging.debug( "%d-of-%d: %s" % ( idx+1, nsql_resp, item ) )
        final_item = []
        for i, column_name in enumerate( sql_names ):
            value = item[ i ]
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, column_name, value ) )
                pass           # ". " marks a trailing dot in histclass or class: skip
                
                # No: do not set empty value, that sometimes gives complete empty indicator columns
                #value = ''      # keep as empty element: otherwise sorting by path is disrupted, i.e. not what we want
            
            if value in eng_data:
                value = value.encode( "utf-8" )
                value = eng_data[ value ]
                
            if column_name not in forbidden:
                if column_name == "base_year":
                    value = str( value )        # switch to strings
                
                final_item.append( value )
        
        logging.debug( "final_item: %s" % final_item )
        final_data.append( final_item )
    
    params_ = copy.deepcopy( params )       # params path sometimes disrupted by json_generator() ???
    key_set = params[ "key_set" ]
    entry_list = json_generator( params_, sql_names, "data", final_data, key_set )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "execute_year() took %s" % str_elapsed )
    
    return entry_list



def execute_only( sql_query, dict_cursor = False ):
    logging.debug( "execute_only () dict_cursor: %s" % dict_cursor )
    # only sql execute part, response handling separate collect_fields()

    time0 = time()      # seconds since the epoch
    logging.debug( "execute_only() start: %s" % datetime.datetime.now() )

    connection = get_connection()
    
    if dict_cursor:
        cursor = connection.cursor( cursor_factory = psycopg2.extras.RealDictCursor )
    else:
        cursor = connection.cursor()
    
    sql_query = cursor.mogrify( sql_query )     # needed if single quote has been escaped by repeating it
    cursor.execute( sql_query )
    
    logging.debug( "query execute_only stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.debug( "query execute_only() sql_query took %s" % str_elapsed )
    
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    logging.debug( "%d sql_names:" % len( sql_names ) )
    logging.debug( sql_names )
    
    if dict_cursor:
        sql_resp = [ json.dumps( dict( record ) ) for record in cursor ]    # it calls .fetchone() in loop
    else:
        sql_resp = cursor.fetchall()
    logging.debug( "result # of data records: %d" % len( sql_resp ) )
    
    cursor.close()
    connection.close()

    str_elapsed = format_secs( time() - time0 )
    logging.info( "execute_only() took %s" % str_elapsed )

    return sql_names, sql_resp



def collect_fields( params, eng_data, sql_names, sql_resp ):
    # sql_names & sql_resp from execute_only()
    
    time0 = time()      # seconds since the epoch
    logging.debug( "collect_fields() start: %s" % datetime.datetime.now() )
    
    final_data = []
    for idx, item in enumerate( sql_resp ):
        logging.debug( "%d: %s" % ( idx, item ) )
        final_item = []
        
        for i, column_name in enumerate( sql_names ):
            value = item[ i ]
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, column_name, value ) )
                pass           # ". " marks a trailing dot in histclass or class: skip
                
                # No: do not set empty value, that sometimes gives complete empty indicator columns
                #value = ''      # keep as empty element: otherwise sorting by path is disrupted, i.e. not what we want
            
            if value in eng_data:
                value = value.encode( "utf-8" )
                value = eng_data[ value ]
                
            if column_name not in forbidden:
                if column_name == "base_year":
                    value = str( value )        # switch to strings
                
                final_item.append( value )
        
        logging.debug( "final_item: %s" % final_item )
        final_data.append( final_item )
    
    params_ = copy.deepcopy( params )       # params path sometimes disrupted by json_generator() ???
    key_set = params[ "key_set" ]
    entry_list = json_generator( params_, sql_names, "data", final_data, key_set )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "collect_fields() took %s" % str_elapsed )

    return entry_list



def collect_records( records_dict, sql_prefix, path_dict, params, sql_names, sql_resp ):
    logging.debug( "collect_records() sql_prefix: %s" % sql_prefix )
    # sql_names & sql_resp from execute_only()
    logging.debug( "sql_resp:  %d records" % len( sql_resp ) )
    if len( sql_resp ) == 0:
        return

    time0 = time()      # seconds since the epoch
    logging.debug( "collect_records() start: %s" % datetime.datetime.now() )
    
    key_set = params[ "key_set" ]
    logging.debug( "key_set:   %s" % str( key_set ) )
    logging.debug( "sql_names: %s" % str( sql_names ) )
    
    nkeys = path_dict[ "nkeys" ]
    path_prev = None
    
    language       = params.get( "language" )
    classification = params.get( "classification" )
    ter_codes_req  = params.get( "ter_codes" )      # requested ter_codes (only historical)
    
    logging.debug( "collect_records() requested ter_codes: %s" % str( ter_codes_req ) )
    
    # value strings for empty and combined ter_code values
    value_na   = ""
    value_none = ""
    if language.upper() == "EN":
        value_na   = "na"
        value_none = "cannot aggregate at this level"
    elif language.upper() == "RU":
        value_na   = "нет данных"
        value_none = "агрегация на этом уровне невозможна"
    
    class_prefix = "class"
    if classification == "historical":
        class_prefix = "hist" + class_prefix 
    
    nsql_resp = len( sql_resp )
    for r, rec in enumerate( sql_resp ):
        logging.debug( "sql_resp %d-of-%d, %s: %s" % ( r+1, nsql_resp, type( rec ), rec ) )
        rec_dict = json.loads( rec )
        
        # path+unit combinations in sql record
        # ordered for proper sorting, but: to json: dicts => tuples
        path_unit_dict = collections.OrderedDict()
        #path_unit_dict = {}
        
        for hc in range( 1, 11 ):   # [1,...,10]
            key = "%s%d" % ( class_prefix, hc )
            value = rec_dict.get( key )
            if not value or value == ". ":
                break               # ". " marks a trailing dot in histclass or class: skip remains
            else:
                path_unit_dict[ key ] = rec_dict[ key ]
        
        value_unit = rec_dict[ "value_unit" ]
        path_unit_dict[ "value_unit" ] = value_unit
        path_unit_str = json.dumps( path_unit_dict )
        
        if path_unit_str not in records_dict.keys():        # new path+unit combi
            logging.debug( "add path_unit_str: %s" % path_unit_str )
            #records_dict[ path_unit_str ] = SortedDict()    # json: dicts => tuples
            records_dict[ path_unit_str ] = {}              # add dict for the { ter_code : value } pairs
        
        # ter_codes
        ter_code_dict = records_dict[ path_unit_str ]       # already collected ter_codes for this path_unit_str
        ter_code = rec_dict[ "ter_code" ]                   # new ter_code from sql response

        if classification == "historical":      # only add requested ter_codes
            if ter_code in ter_codes_req:       # wanted ter_code (plus its value)
                logging.debug( "requested ter_code %s" % ter_code )
                total = rec_dict.get( "total" )
                
                try:
                    total = float( total )
                except:
                    total = value_na
                
                try:
                    old_val = ter_code_dict[ ter_code ]
                    dup = True      # existing ter_code
                except:
                    ter_code_dict[ ter_code ] = total
                    dup = False     # new ter_code
                
                if dup:             # combine values
                    try:
                        float( old_val )
                        if total in [ value_na, value_none ]:
                            ter_code_dict[ ter_code ] = value_none
                        else:
                            ter_code_dict[ ter_code ] = old_val + total
                    except:     # not both float
                        if total == value_na:
                            ter_code_dict[ ter_code ] = value_na
                        else:
                            ter_code_dict[ ter_code ] = value_none
            else:
                logging.debug( "skip ter_code %s" % ter_code )
        else:
            logging.debug( "modern: skip path_unit_str %s" % path_unit_str )
    
    nrecords = len( records_dict.keys() )
    logging.debug( "paths in records_dict %d" % nrecords )
    #show_record_dict( sql_prefix, records_dict )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "collect_records() took %s" % str_elapsed )
    
    return nrecords



def add_missing( records_dict, params ):
    time0 = time()      # seconds since the epoch
    logging.debug( "add_missing_tc() start: %s" % datetime.datetime.now() )

    # value strings for empty fields
    language = params.get( "language" )
    value_na   = ""
    value_none = ""
    if language.upper() == "EN":
        value_na = "na"
    elif language.upper() == "RU":
        value_na = "нет данных"

    ter_codes_req = params.get( "ter_codes" )      # requested ter_codes (only historical)

    for path_unit_str in records_dict:
        ter_code_dict = records_dict[ path_unit_str ]
        for ter_code in ter_codes_req:
            try:
                ter_code_dict[ ter_code ]
            except:
                ter_code_dict[ ter_code ] = value_na
    
    logging.debug( "final paths in records_dict %d" % len( records_dict.keys() ) )
    #show_record_dict( "all", records_dict )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "add_missing() took %s" % str_elapsed )
    
    return records_dict


def sort_records( records_dict, params ):
    time0 = time()      # seconds since the epoch
    logging.debug( "sort_records() start: %s" % datetime.datetime.now() )
    
    entry_list = []
    
    path_unit_strs = records_dict.keys()        # unsorted
    path_unit_strs = sorted( path_unit_strs )
    
    for path_unit_str in path_unit_strs:
        logging.debug( "sort_records() path_unit_str: %s" % path_unit_str )
        path_unit_dict = json.loads( path_unit_str )
        
        value_unit = path_unit_dict[ "value_unit" ]
        path = path_unit_dict
        del path[ "value_unit" ]
        
        # collect ter_codes for path_unit_str
        ter_code_list = []
        ter_code_dict = records_dict[ path_unit_str ]
        
        for ter_code in ter_code_dict:
            tc_dict = { "ter_code" : ter_code, "total" : ter_code_dict.get( ter_code ) } 
            ter_code_list.append( tc_dict )
        
        record_dict = {
            "path" : path,
            "value_unit" : value_unit,
            "ter_codes" : ter_code_list
        }
        
        entry_list.append( record_dict )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "sort_records() took %s" % str_elapsed )
    
    return entry_list



def records2oldentries( records_dict, params ):
    # compatibility check with old response structure
    time0 = time()      # seconds since the epoch
    logging.info( "records2oldentries() start: %s" % datetime.datetime.now() )

    datatype  = params[ "datatype" ]
    base_year = params[ "base_year" ]

    entry_list = []
    
    for path_unit_str in records_dict:
        logging.info( "records2oldentries() path_unit_str: %s" % path_unit_str )
        path_unit_dict = json.loads( path_unit_str )
        
        value_unit = path_unit_dict[ "value_unit" ]
        path = path_unit_dict
        del path[ "value_unit" ]
        
        ter_code_dict = records_dict[ path_unit_str ]
        for ter_code in ter_code_dict:
            #total = ter_code_dict[ ter_code ]
            total = ter_code_dict.get( ter_code )
            
            logging.info( "records2oldentries() type: %s" % type( total ) )
            
            entry = {}
            entry[ "base_year" ]  = base_year,
            entry[ "datatype" ]   = datatype,
            entry[ "path" ]       = path, 
            entry[ "ter_code" ]   = ter_code,
            entry[ "total" ]      = total, 
            entry[ "value_unit" ] = value_unit
            entry_list.append( entry )
    
    logging.info( "records2oldentries() # %d" % len( entry_list ) )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "records2oldentries() took %s" % str_elapsed )
    
    return entry_list


"""
def merge_3records( record_dict_num, record_dict_ntc, record_dict_none ):
    time0 = time()      # seconds since the epoch
    logging.info( "merge_3records() start: %s" % datetime.datetime.now() )
    
    show_record_dict( "record_dict_num", record_dict_num )
    show_record_dict( "record_dict_ntc", record_dict_ntc )
    show_record_dict( "record_dict_none", record_dict_none )
    
    record_dict = copy.deepcopy( record_dict_num )
    
    for key, val in record_dict_ntc.items():
        try:        # old key: append list
            record_dict[ key ]
            record_dict[ key ].append( val )
        except:     # new key
            record_dict[ key ] = val
    
    for key, val in record_dict_none.items():
        try:        # old key: append list
            record_dict[ key ]
            record_dict[ key ].append( val )
        except:     # new key
            record_dict[ key ] = val

    logging.info( "record_dict: %d records" % len( record_dict ) )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "merge_records() took %s" % str_elapsed )
    
    return record_dict
"""

"""
def merge_2records( record_dict, record_dict_path ):
    time0 = time()      # seconds since the epoch
    logging.info( "merge_2records() start: %s" % datetime.datetime.now() )
    
    for key, val in record_dict_path.items():
        try:        # old key: append list
            record_dict[ key ]
            record_dict[ key ].append( val )
        except:     # new key
            record_dict[ key ] = val

    logging.info( "record_dict: %d records" % len( record_dict ) )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "merge_2records() took %s" % str_elapsed )
    
    return record_dict
"""


def show_record_dict( prefix, record_dict, sort = False ):
    logging.info( "show_record_dict() sort %s" % sort )
    logging.info( "%s # of records: %d" % ( prefix, len( record_dict ) ) )
    
    if sort:
        record_list = sorted( record_dict.items() )
        for t, tup in enumerate( record_list ):
            logging.info( "# %d path_unit: %s" % ( t, tup[ 0 ] ) )
            logging.info( "# %d ter_codes: %s" % ( t, str( tup[ 1 ] ) ) )
    else:
        for r, ( key, val ) in enumerate( record_dict.items() ):
            logging.info( "# %d path_unit: %s" % ( r, key ) )
            logging.info( "# %d ter_codes: %s" % ( r, str( val ) ) )



def show_record_list( list_name, record_list ):
    logging.info( "show_record_list()" )
    logging.info( "%s # of records: %d" % ( list_name, len( record_list ) ) )
    for r, record in enumerate( record_list ):
        path_unit_str = record[ "path_unit_str" ]
        ter_codes  = record[ "ter_codes" ]
        logging.info( "# %d path_unit: %s" % ( r, path_unit_str ) )
        logging.info( "# %d ter_codes: %s" % ( r, str( ter_codes ) ) )



def add_unique_items( language, list_name, entry_list_collect, entry_list_extra ):
    # collect unique paths in entry_list_collect
    logging.debug( "add_unique_items()" )
    
    paths = []
    for entry_collect in entry_list_collect:
        path = entry_collect.get( "path" )
        if path not in paths:
            paths.append( path )
    
    logging.debug( "# of input path elements: %s" % len( paths ) )
    for p, path in enumerate( paths ):
        logging.debug( "%d: %s" % ( p, path ) )
    
    # value strings for empty and combined fields
    value_na   = ""
    value_none = ""
    if language.upper() == "EN":
        value_na   = "na"
        value_none = "cannot aggregate at this level"
    elif language.upper() == "RU":
        value_na   = "нет данных"
        value_none = "агрегация на этом уровне невозможна"
    
    nadded = 0
    nmodified = 0
    entry_list_modify = []
    
    for entry_extra in entry_list_extra:
        logging.debug( "entry_extra: %s" % str( entry_extra ) )
        path_extra = entry_extra[ "path" ]
        
        if path_extra not in paths:
            if not entry_extra.get( "total" ):          # we need the field "total", 
                entry_extra[ "total" ] = value_na       # otherwise the GUI shows "undefined" javascript variable
            entry_list_collect.append( entry_extra )
            nadded += 1
            logging.debug( "adding path: %s" % path_extra )
            paths.append( path_extra )      # also add to paths, to prevent adding a new path more than once
        else:
            if list_name == "entry_list_none":
                for entry_collect in entry_list_collect:
                    path_collect       = entry_collect.get( "path" )
                    value_unit_collect = entry_collect.get( "value_unit" )
                    ter_code_collect   = entry_collect.get( "ter_code" )
                    
                    value_unit_extra = entry_extra.get( "value_unit" )
                    ter_code_extra   = entry_extra.get( "ter_code" )
                    
                    if path_extra == path_collect and value_unit_extra == value_unit_collect and ter_code_extra == ter_code_collect:
                        #logging.debug( "modify entry_collect: %s" % str( entry_collect ) )
                        entry_list_modify.append( entry_collect )
                        nmodified += 1
    
    logging.debug( "nadded: %d, nmodified: %d" % ( nadded, nmodified ) )
    #logging.debug( "modify entry_collect: %s" % str( entry_collect ) )
    
    for e, entry_modify in enumerate( entry_list_modify ):
        entry_new = copy.deepcopy( entry_modify )
        
        total = entry_modify.get( "total" )
        try:
            float( total )
            entry_new[ "total" ] = value_none
        except:
            entry_new[ "total" ] = value_na
        
        # remove entry/entries with (same path + value_unit + ter_code)
        path_m = entry_modify[ "path" ]
        unit_m = entry_modify[ "value_unit" ]
        terc_m = entry_modify[ "ter_code" ]
        
        for entry in entry_list_collect:
            if entry[ "path" ] == path_m  and entry[ "value_unit" ] == unit_m and entry[ "ter_code" ] == terc_m:
                logging.debug( "remove from entry_collect: %s" % str( entry ) )
                entry_list_collect.remove( entry )
        
        # add modified entry
        logging.debug( "append to entry_collect: %s" % str( entry_new ) )
        entry_list_collect.append( entry_new )
    
    return entry_list_collect



def add_unique_items_grouped( language, dict_name, entry_dict_collect, entry_dict_extra ):
    # collect unique paths in entry_dict_collect
    logging.info( "add_unique_items_grouped()" )
    
    entry_dict_path_ig = {}
    """
    logging.info( "entry_dict_collect: %s, len: %d" % ( type( entry_dict_collect ), len( entry_dict_collect ) ) )
    for key, value in entry_dict_collect.iteritems():
        logging.info( "key: %s\nvalue: %s" % ( key, value ) )
    """

    logging.info( "entry_dict_extra: %s, len: %d" % ( type( entry_dict_extra ), len( entry_dict_extra ) ) )
    for key, entry_extra in entry_dict_extra.iteritems():
        logging.info( "key: %s\nentry_extra: %s" % ( key, entry_extra ) )
        entry_collect = entry_dict_collect.get( key )
        if entry_collect:
            logging.info( "merge key: %s\nentry_extra: %s\nentry_collect: %s" % ( key, entry_extra, entry_collect ) )
    
    return entry_dict_path_ig



def remove_dups( entry_list_collect ):
    logging.debug( "remove_dups()" )
    time0 = time()      # seconds since the epoch
    
    # remove duplicates
    #list( set( entry_list_collect ) )      # fails: dicts not hashable
    
    # [i for n, i in enumerate(d) if i not in d[n + 1:]]    # list comprehension
    # Here since we can use dict comparison, we only keep the elements that are not in the rest of the 
    # initial list (this notion is only accessible through the index n, hence the use of enumerate).
    entry_list_nodups = [ i for n, i in enumerate( entry_list_collect ) if i not in entry_list_collect[ n + 1: ] ]
    
    logging.info( "remove_dups() %d items removed" % ( len( entry_list_collect ) - len( entry_list_nodups ) ) )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "remove_dups() took %s" % str_elapsed )
    
    return entry_list_nodups



def sort_entries( datatype, entry_list ):
    logging.info( "sort_entries()" )
    time0 = time()      # seconds since the epoch
        
    # sorting with sorted() + path key only gives the desired result if all items 
    # have the same (number of) keys. So add missing keys with empty values as needed. 
    path_keys = []
    for item in entry_list:
        path = item.get( "path" )
        if len( path.keys() ) > len( path_keys ):
            path_keys = path.keys()
    len_path_keys = len( path_keys )
    
    entry_list1 = []
    for item in entry_list:
        itm = copy.deepcopy( item )
        path = itm.get( "path" )
        if len( path.keys() ) < len_path_keys:
            for key in path_keys:
                if not path.get( key ):
                    path[ key ] = ''
            itm[ "path" ] = path
        
        entry_list1.append( itm )
    
    entry_list2 = entry_list1
    # apparently, we loose the prefixed spaces somewhere down the line
    """
    entry_list2 = []
    if datatype != "1.02":
        entry_list2 = entry_list1
    else:
        # prefix level1 with leading space as needed (for sorting)
        for item in entry_list1:
            itm = copy.deepcopy( item )
            path = itm.get( "path" )
            for key, value in path.iteritems():
                if "class1" in key:
                    try:
                        ivalue = int( value )
                        if ivalue < 10:
                            #v = '_' + value      # wrong sort
                            v = '0' + value     # this works, but is not wanted
                            #v = ' ' + value    # we loose the space somewhere
                            path[ key ] = v
                            #logging.debug( path )
                            break
                    except:
                        pass
            
            logging.info( itm )
            entry_list2.append( itm )
    """
    
    # sometimes the value_unit string is not constant, so first sort by path, next by value_unit
    entry_list_sorted = sorted( entry_list2, key = itemgetter( 'path', 'value_unit' ) )  
    
    #for e, entry in enumerate( entry_list_sorted ):
    #    logging.info( "%d: %s" % ( e, str( entry ) ) )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "sort_entries() took %s" % str_elapsed )
    
    return entry_list_sorted


"""
def reorder_entries( params, entry_list_ntc, entry_list_none, entry_list = None ):
    logging.info( "reorder_entries()" )
    # params params.keys() = [ "language", "classification", "datatype", "base_year", "ter_codes" ]
    # no "ter_codes" for modern classification in params, get from entries
    # for modern, reorder_entries is called separate for each base_year
    
    time0 = time()      # seconds since the epoch
    language  = params.get( "language" )
    classification = params.get( "classification" )
    
    # aggregation table settings
    use_temp_table = True       # on production server
    #use_temp_table = False     # 'manual' cleanup
    
    # on production server: use DROP
    #on_commit =  " ON COMMIT PRESERVE ROWS"    # Default, No special action is taken at the ends of transactions
    #on_commit =  " ON COMMIT DELETE ROWS"      # All rows in the temporary table will be deleted at the end of each transaction block
    on_commit =  " ON COMMIT DROP"             # The temporary table will be dropped at the end of the current transaction block

    skip_empty = True     # only return entries in response that have a specfied region count
    #skip_empty = False    # return all region fields (may exhibit performance problem)

    nlevels_use = 0
    entry_list_asked = []       # entries requested
    entry_list_cnt = []         # entries with counts
    ter_codes = []              # region codes
    
    if classification == "modern":
        level_prefix = "class"
        
        for entry in entry_list_ntc:
            ter_code = entry[ "ter_code" ]          # ter_codes from entries
            if ter_code not in ter_codes:
                ter_codes.append( ter_code )
            
            value_unit = entry[ "value_unit" ]
            
            total_str = entry[ "total" ]
            try:
                total_float = float( total_str )
                entry_list_cnt.append( entry )
            except:
                pass
            
    else:       # historical
        level_prefix = "histclass"
        nlevels = 0
        #path_list = []          # lists of unique paths
        path_unit_list = []     # lists of unique (paths + value_unit)
        ter_codes = params.get( "ter_codes" )       # ter_codes provided
        
        # only "historical" has entry_list
        logging.debug( "# of entries in [historical] entry_list: %d" % len( entry_list ) )
        for entry in entry_list:
            logging.debug( "entry: %s" % entry )
            path = entry[ "path" ]
            
            value_unit = entry[ "value_unit" ]
            
            path_unit = { "path" : path, "value_unit" : value_unit }
            if path_unit not in path_unit_list:
                path_unit_list.append( path_unit )
                nlevels = max( nlevels, len( path.keys() ) )
            
            total_str = entry[ "total" ]
            try:
                total_float = float( total_str )
                entry_list_cnt.append( entry )
            except:
                pass
        
        logging.debug( "# of levels: %d" % nlevels )
        nlevels_use = nlevels
    
    # both "historical" and "modern" have entry_list_ntc
    nlevels_ntc = 0
    path_unit_list_ntc = []     # lists of unique (paths + value_unit)
    logging.debug( "# of entries in entry_list_ntc: %d" % len( entry_list_ntc ) )
    
    for entry in entry_list_ntc:
        logging.debug( "entry: %s" % entry )
        path = entry[ "path" ]
        
        value_unit = entry[ "value_unit" ]
        path_unit = { "path" : path, "value_unit" : value_unit }
        if path_unit not in path_unit_list_ntc:
            path_unit_list_ntc.append( path_unit )
            nlevels_ntc = max( nlevels_ntc, len( path.keys() ) )
    
    logging.info( "# of levels_ntc: %d" % nlevels_ntc )
    
    if classification == "modern":
        nlevels_use = 10        # always use the max, don't care about possible empty columns
    else:
        nlevels_use = max( nlevels_use, nlevels_ntc )
    logging.info( "# of levels used: %d" % nlevels_use )
    
    #logging.info( "# of unique records in path_list_ntc result: %d" % len( path_list_ntc ) )
    logging.info( "# of unique records in path_unit_list_ntc result: %d" % len( path_unit_list_ntc ) )
    logging.info( "# of records in path result with count: %d" % len( entry_list_cnt ) )
    
    nregions = len( ter_codes )
    logging.info( "# of regions requested: %d" % nregions )
    
    
    connection = get_connection()
    cursor = connection.cursor( cursor_factory = psycopg2.extras.DictCursor )
    
    sql_delete = None
    sql_create = ""
    
    table_name = "temp_aggregate"
    if use_temp_table:          # TEMP TABLEs are not visible to other sessions
        sql_create  = "CREATE TEMP TABLE %s (" % table_name 
    else:                       # debugging
        sql_delete = "DROP TABLE %s;" % table_name
        #sql_create = "CREATE UNLOGGED TABLE %s (" % table_name 
        sql_create = "CREATE TABLE %s (" % table_name 
    
    for column in range( 1, nlevels_use + 1 ):
        sql_create += "%s%d VARCHAR(1024)," % ( level_prefix, column )
    
    sql_create += "value_unit VARCHAR(1024),"
    sql_create += "count VARCHAR(1024)"
    
    ntc = len( ter_codes )
    for tc, ter_code in enumerate( ter_codes ):
        sql_create += ",tc_%s VARCHAR(1024)" % ter_code
    
    sql_create += ")"
    
    if use_temp_table:
        sql_create += on_commit 
    sql_create += ";" 
        
    logging.info( "sql_create: %s" % sql_create )
    
    try:
        cursor.execute( sql_create )
    except:
        logging.error( "creating temp table %s failed:" % table_name )
        type_, value, tb = sys.exc_info()
        logging.error( "%s" % value )
    
    # fill table
    # value strings for empty and combined fields
    value_na   = ""
    value_none = ""
    if language.upper() == "EN":
        value_na   = "na"
        value_none = "cannot aggregate at this level"
    elif language.upper() == "RU":
        value_na   = "нет данных"
        value_none = "агрегация на этом уровне невозможна"
    
    levels_str = []
    ter_codes_str = []
    num_path = len( path_unit_list_ntc )
    for pu, path_unit in enumerate( path_unit_list_ntc ):
        path = path_unit[ "path" ]
        value_unit = path_unit[ "value_unit" ]
        logging.debug( "%d-of-%d unit: %s, path: %s" % ( pu+1, num_path, value_unit, path ) )
        columns = []
        values  = []
        for key, value in path.items():
            columns.append( key )
            values .append( value )
            if key not in levels_str:
                levels_str.append( key )
        
        ncounts = 0
        #unit = '?'
        for ter_code in ter_codes:
            logging.debug( "ter_code: %s" % ter_code )
            value = value_na
            
            # search for path + ter_code in list with counts
            for entry in entry_list_cnt:
                #logging.debug( "entry: %s" % entry )
                if path == entry[ "path" ] and value_unit == entry[ "value_unit" ] and ter_code == entry[ "ter_code" ]:
                    ncounts += 1
                    total = entry[ "total" ]        # double from aggregate sql query
                    logging.debug( "ncounts: %d, total: %s, value_unit: %s, ter_code: %s, path: %s" % ( ncounts, total, value_unit, ter_code, path ) )
                    if round( total ) == total:     # only 0's after .
                        total = int( total )        # suppress trailing .0...
                    value = total
                    #unit = entry[ "value_unit" ]
                    
                    # check for presence in non-number list
                    for entry_none in entry_list_none:
                        logging.debug( "entry_none: %s" % entry_none )
                        if path == entry_none[ "path" ] and value_unit == entry[ "value_unit" ] and ter_code == entry_none[ "ter_code" ]:
                            value = value_none
                            break
                    break
            
            if skip_empty and value in [ value_na, '' ]:
                continue        # do not return empty values in response
            
            ter_code_str = "tc_%s" % ter_code
            if ter_code_str not in ter_codes_str:
                ter_codes_str.append( ter_code_str )
            columns.append( ter_code_str )
            values .append( value )
        
        logging.debug( "columns: %s" % columns )
        logging.debug( "values:  %s" % values )
        
        columns.append( "value_unit" )
        values .append( value_unit )
        
        columns.append( "count" )
        values .append( "%d/%d" % ( ncounts, nregions ) )
        
        logging.debug( "columns: %s" % columns )
        logging.debug( "values:  %s" % values )
        
        # improve this with psycopg2.sql – SQL string composition, see http://initd.org/psycopg/docs/sql.html
        fmt = "%s," * len ( columns )
        fmt = fmt[ :-1 ]    #  remove trailing comma
        columns_str = ','.join( columns )
        sql_insert = "INSERT INTO %s (%s) VALUES ( %s );" % ( table_name, columns_str, fmt )
        logging.debug( "sql_insert: %d: %s" % ( pu, sql_insert ) )
        
        try:
            cursor.execute( sql_insert, ( values ) )
        except:
            logging.error( "insert into temp table %s failed:" % table_name )
            type_, value, tb = sys.exc_info()
            logging.error( "%s" % value )
    
    # fetch result sorted
    order_by = ""
    #for l in range( 1, 1 + nlevels_ntc ):
    for l in range( 1, 1 + nlevels_use ):
        if l > 1:
            order_by += ','
        order_by += "%s%d" % ( level_prefix, l )
    
    try:
        sql_query = "SELECT * FROM %s ORDER BY %s;" % ( table_name, order_by )
    except:
        logging.error( "select from temp table %s failed:" % table_name )
        type_, value, tb = sys.exc_info()
        logging.error( "%s" % value )
        
    logging.info( sql_query )
    cursor.execute( sql_query )
    sql_resp = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    logging.debug( "%d sql_names: \n%s" % ( len( sql_names ), sql_names ) )
    
    if sql_delete:
        try:
            cursor.execute( sql_delete )
        except:
            logging.error( "deleting temp table %s failed:" % table_name )
            type_, value, tb = sys.exc_info()
            logging.error( "%s" % value )
    
    connection.commit()
    cursor.close()
    connection.close()
    
    entry_list_sorted = []
    for r, row in enumerate( sql_resp ):
        record = dict( row )
        logging.debug( "%d: record: %s" % ( r, record ) )
        
        # 1 entry per ter_code
        for ter_code in ter_codes:
            new_entry = {
                "datatype"   : params[ "datatype" ],
                "base_year"  : params[ "base_year" ],
                "ter_code"   : ter_code,
                #"value_unit" : unit,
                "db_row"     : r
            }
            
            path = {}
            total = ''
            for key in record:
                value = record[ key ]
                if key == "count":
                    new_entry[ "count" ] = value
                if key == "value_unit":
                    #new_entry[ "value_unit" ] = value_label
                    new_entry[ "value_unit" ] = value
                
                if "class" in key:
                    if value is None:
                        path[ key ] = ''
                    else:
                        path[ key ] = value
                
                if ter_code in key:
                    new_entry[ "total" ] = value
            
            new_entry[ "path" ] = path
            total = new_entry.get( "total" )
            
            if skip_empty and total is None or total == '':
                continue        # do not return empty values in response
            else:
                logging.debug( "new_entry: %s" % new_entry )
            
            entry_list_sorted.append( new_entry )
    
    logging.debug( "%d entries in list_sorted: \n%s" % ( len( entry_list_sorted ), entry_list_sorted ) )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "reordering entries took %s" % str_elapsed )
    
    return entry_list_sorted
"""


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
    logging.debug( "Python version: %s" % sys.version )

    configparser   = get_configparser()
    dv_host        = configparser.get( "config", "dataverse_root" )
    api_root       = configparser.get( "config", "api_root" )
    ristat_key     = configparser.get( "config", "ristatkey" )
    ristat_name    = configparser.get( "config", "ristatname" )
    ristatdocs     = configparser.get( "config", "hdl_documentation" )
    
    logging.info( "dv_host: %s" % dv_host )
    logging.info( "ristat_key: %s" % ristat_key )
    
    papers = []
    
    try:
        connection = Connection( dv_host, ristat_key )
    except:
        type_, value, tb = sys.exc_info()
        logging.error( "%s" % value )
        return Response( json.dumps( papers ), mimetype = "application/json; charset=utf-8" )
    
    
    dataverse = connection.get_dataverse( ristat_name )
    if not dataverse:
        logging.info( "ristat_key: %s" % ristat_key )
        logging.error( "COULD NOT GET A DATAVERSE CONNECTION" )
        return Response( json.dumps( papers ), mimetype = "application/json; charset=utf-8" )

    settings = DataFilter( request.args )
    
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
            url = "http://" + dv_host + "/api/datasets/" + str( datasetid ) + "/?&key=" + str( ristat_key )
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



# Topics - Get topics and process them as terms for GUI
@app.route( "/topics" )
def topics():
    logging.debug( "/topics" )
    logging.debug( "topics() request.args: %s" % str( request.args ) )
    
    # Pieter requirement, RUSREPS-216: 
    # - er per topic een json waarde is met key 'byear_counts', daar zat een array in met als keys de jaartallen en als waarde een count/totaal
    # - de topic_root geen decimalen heeft, dus '2' ipv '2.0', en '0' ipv '0.0'
    # for TOPIC_ROOT = 0, the DATATYPE values are integers [1...7]. 
    # Due to the xslx-to-csv conversion, the values become [1.0, 2.0 ...7.0]
    # Strip the unwanted ".0" here, but only for TOPIC_ROOT = 0
    
    language  = request.args.get( "language" )
    datatype  = request.args.get( "datatype" )
    base_year = request.args.get( "basisyear" )
    
    if not language:
        language = "ru"
    
    vocab_type = "topics"
    topics_dict = load_vocabulary( vocab_type, language, datatype, base_year )
    logging.debug( "topics_dict: %s" % type( topics_dict ) )
    logging.debug( "topics_dict: %s" % str( topics_dict ) )
    
    topics_array_out = []
    topics_array = topics_dict[ "data" ]
    
    for topic_dict in topics_array:
        # Pieter wants no decimals in topic_root value string
        topic_root = topic_dict[ "topic_root" ]
        topic_id   = topic_dict[ "topic_id" ]
        
        try:
            topic_root = str( int( float( topic_root ) ) )
            topic_dict[ "topic_root" ] = topic_root
        except:
            pass
        logging.debug( "topic: %s" % str( topic_dict ) )
        
        datatype = topic_dict[ "datatype" ]
        count_dict = topic_counts( language, datatype )
        
        if topic_root == "0":   # pseudo topic, datatype as integer
            datatype = str( int( float( datatype ) ) )
            topic_dict[ "datatype" ] = datatype
        
        topic_id = str( int( float( topic_id ) ) )
        topic_dict[ "topic_id" ] = topic_id 
        
        topic_dict_out = copy.deepcopy( topic_dict )
        topic_dict_out[ "byear_counts" ] = count_dict
        topics_array_out.append( topic_dict_out )
    
    topics_dict_out = { "data" : topics_array_out }
    logging.debug( "/topics return Response" )
    return Response( json.dumps( topics_dict_out ), mimetype = "application/json; charset=utf-8" )
    


# Years - Get available years for year selection at GUI step 2
@app.route( "/years" )
def years():
    logging.info( "/years" )
    
    logging.info( "request.args: %s" % str( request.args ) )
    settings = DataFilter( request.args )
    
    datatype = ''
    if "datatype" in settings.datafilter:
        datatype = settings.datafilter[ "datatype" ]
    
    classification = ''
    if "classification" in settings.datafilter:
        classification = settings.datafilter[ "classification" ]
    
    connection = get_connection()
    cursor = connection.cursor()
    
    # json_string = dictionary with record counts from table russianrepository for the given datatype
    json_string = load_years( cursor, datatype, classification )
    cursor.close()
    connection.close()
    
    logging.info( "json_string: %s" % str( json_string ) )
    
    logging.debug( "/years return Response" )
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



# Regions - Get regions to build the region selector at GUI step 3 
@app.route( "/regions" )
def regions():
    logging.debug( "/regions" )
    logging.debug( "regions() request.args: %s" % str( request.args ) )
    
    language  = request.args.get( "language" )
    datatype  = request.args.get( "datatype" )
    base_year = request.args.get( "basisyear" )
    
    vocab_type = "regions"
    data = load_vocabulary( vocab_type, language, datatype, base_year )
    
    logging.debug( "/regions return Response" )
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



# Histclasses - Get historical indicators for the indicator selection at GUI step 4
@app.route( "/histclasses" )
def histclasses():
    logging.info( "/histclasses" )
    logging.debug( "histclasses() request.args: %s" % str( request.args ) )
    
    language  = request.args.get( "language" )
    datatype  = request.args.get( "datatype" )
    base_year = request.args.get( "base_year" )
    
    vocab_type = "historical"
    data = load_vocabulary( vocab_type, language, datatype, base_year )
    
    logging.debug( "/histclasses return Response" )
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



# Classes - Get modern indicators for the indicator selection at GUI step 4
@app.route( "/classes" )
def classes():
    logging.info( "/classes" )
    logging.debug( "classes() request.args: %s" % str( request.args ) )
    
    language  = request.args.get( "language" )
    datatype  = request.args.get( "datatype" )
    base_year = request.args.get( "base_year" )
    
    vocab_type = "modern"
    data = load_vocabulary( vocab_type, language, datatype, base_year )
    logging.info( "data: %s" % data )
    
    logging.debug( "/classes return Response" )
    return Response( json.dumps( data ), mimetype = "application/json; charset=utf-8" )



# Indicators - Is this used by the GUI?
"""
@app.route( "/indicators", methods = [ "POST", "GET" ] )
def indicators():
    logging.debug( "indicators()" )
    qinput = simplejson.loads( request.data )
    
    connection = get_connection()
    cursor = connection.cursor()
    
    sql_query = "SELECT datatype, base_year, COUNT(*) FROM russianrepository GROUP BY base_year, datatype;"
    cursor.execute( sql_query )
    
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    logging.debug( "%d sql_names:" % len( sql_names ) )
    logging.debug( sql_names )
    
    sl_resp = cursor.fetchall()
    cursor.close()
    connection.close()
    
    eng_data = {}
    final_data = []
    
    for item in sl_resp:
        final_item = []
        for i, column_name in enumerate( sql_names ):
            value = item[ i ]
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, column_name, value ) )
                # ". " marks a trailing dot in histclass or class: skip
                pass
            
            if value in eng_data:
                value = value.encode( "utf-8" )
                value = eng_data[ value ]
            if column_name not in forbidden:
                final_item.append( value )
        
        final_data.append( final_item )
        logging.debug( str( final_data ) )
    
    entry_list = json_generator( qinput, sql_names, "data", final_data )
    
    language = request.args.get( "language" )
    download_key = request.args.get( "download_key" )
    json_string, cache_except = json_cache( entry_list, language, "data", download_key )
    
    logging.debug( "json_string before return Response:" )
    logging.debug( json_string )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )
"""


# Aggregation - Preview User selection post and gets data to build preview in GUI step 5 
@app.route( "/aggregation", methods = ["POST" ] )
def aggregation():
    logging.debug( "" )
    logging.info( "/aggregation" )
    time0 = time()      # seconds since the epoch
    logging.debug( "aggregation start: %s" % datetime.datetime.now() )
    
    qinput = simplejson.loads( request.data )
    logging.debug( "qinput: %s" % str( qinput ) )
    
    method = qinput.get( "Method" )
    redundant = True
    if method == "new":
        redundant = False
    logging.debug( "method: %s, redundant: %s" % ( method, redundant ) )
    
    language = qinput.get( "language" )
    classification = qinput.get( "classification" )
    datatype = qinput.get( "datatype" )
    logging.debug( "language: %s, classification: %s, datatype: %s" % ( language, classification, datatype ) )
    
    path = qinput.get( "path" )
    add_subclasses, path_stripped, key_set = strip_subclasses( path )
    
    logging.debug( "(%d) path          : %s" % ( len( path ),          path ) )
    logging.debug( "(%d) path_stripped : %s" % ( len( path_stripped ), path_stripped ) )
    
    # put some additional info in the download key
    base_year = qinput.get( "base_year" )
    if base_year is None or base_year == "":
        base_year = "0000"
    else:
        base_year = str( base_year )
    
    group_tercodes = False  # default situation
    #group_tercodes = True   # group ter_codes with total values per unique path + unit_value
    
    entry_dict_ig = {}
    entry_dict_ntc_ig = {}
    entry_dict_none_ig = {}
    entry_list_path_ig = {}
    if group_tercodes:
        logging.debug( "grouping ter_codes with total values per unique path + unit_value" )
    
    params = {
        "language"       : language,
        "datatype"       : datatype,
        "classification" : classification,
        "add_subclasses" : add_subclasses,
        "key_set"        : key_set,
        "group_tercodes" : group_tercodes,
        "redundant"      : redundant
    }
    
    uuid4 = str( uuid.uuid4() )
    logging.debug( "uuid4: %s" % uuid4 )
    download_key = "%s-%s-%s-%s-%s" % ( language, classification[ 0 ], datatype, base_year, uuid4 )
    #download_key = "%s-%s-%s-%s=%s" % ( language, classification[ 0 ], datatype, base_year, uuid4 )
    logging.debug( "download_key: %s" % download_key )
    
    configparser = get_configparser()
    tmp_dir = configparser.get( "config", "tmppath" )
    download_dir = os.path.join( tmp_dir, "download", download_key )
    if not os.path.exists( download_dir ):
        os.makedirs( download_dir )
    
    # split input path in subgroups with the same numbr of keys
    #path_lists = group_levels( path )       # input path WITH subclasses parameter, NOT path_stripped
    path_lists_bylen, path_lists_bysub = group_levels( path )
    
    params[ "path_lists_bylen" ] = path_lists_bylen
    params[ "path_lists_bysub" ] = path_lists_bysub
    
    json_string = str( "{}" )
    cache_except = None
    
    """
    prefix = num    default query with explicit ter_code specification; 
                    historic:   value_total = True,  value_numerical = True
                    modern:     not used
    prefix = ntc    query without ter_code specification: => all ter_codes requested
                    historic:   value_total = False, value_numerical = True
                    modern:     value_total = True,  value_numerical = True
    prefix = none   hist + modern:  
                    query with only NANs in value response
                    historic:   value_total = False, value_numerical = False
                    modern:     value_total = False, value_numerical = False
    """
    
    if classification == "historical":
        # historical classification has base_year from qinput
        params[ "base_year" ] = base_year
        
        ter_codes = qinput.get( "ter_code", [] )
        logging.debug( "ter_codes: %s" % ter_codes )
        params[ "ter_codes" ] = ter_codes       # with ter_codes
        
        if redundant:   # old
            entry_list_sorted = aggregate_historic_redun( params )
        else:           # new, redundant = False
            entry_list_sorted = aggregate_historic( params )
    
    elif classification == "modern":
        # modern classification does not have base_year or ter_code from qinput
        
        entry_list_total = []
        
        # modern classification does not provide a base_year; 
        # loop over base_years, and accumulate results.
        base_years = [ "1795", "1858", "1897", "1959", "2002" ]
        #base_years = [ "1858" ]    # test single year
        
        for base_year in base_years:
            logging.info( "base_year: %s" % base_year )
            params[ "base_year" ] = base_year
            
            params_ntc  = copy.deepcopy( params )
            params_none = copy.deepcopy( params )
            
            entry_list_year = []
            
            for pd, path_dict in enumerate( path_lists, start = 1 ):
                show_path_dict( num_path_lists, pd, path_dict )
                
                path_list = path_dict[ "path_list" ]
                add_subclasses = path_dict[ "subclasses" ]
                
                params_ntc[  "path" ] = path_list
                params_none[ "path" ] = path_list
                
                # only for debugging
                #params_ntc[  "etype" ] = "ntc"
                #params_none[ "etype" ] = "none"
                
                show_params( "params -1- = entry_list_ntc", params_ntc )
                #old_query_ntc, eng_data_ntc = aggregate_year( params_ntc, add_subclasses, value_total = True, value_numerical = True )
                sql_query_ntc = make_query( "ntc", params_ntc, add_subclasses, value_total = True, value_numerical = True )
                eng_data_ntc = {}
                entry_list_ntc = execute_year( params_ntc, sql_query_ntc, eng_data_ntc )
                if entry_debug: 
                    show_entries( "params -1- = entry_list_ntc", entry_list_ntc )
                
                # TODO
                #if group_tercodes:
                #   entry_dict_ntc_ig = group_by_ident( entry_list_ntc )
                
                show_params( "params -2- = entry_list_none", params_none )
                #old_query_none, eng_data_none = aggregate_year( params_none, add_subclasses, value_total = False, value_numerical = False )
                sql_query_none = make_query( "none", params_none, add_subclasses, value_total = False, value_numerical = False )   # non-numbers
                eng_data_none = {}
                entry_list_none = execute_year( params_none, sql_query_none, eng_data_none )
                if entry_debug: 
                    show_entries( "params -2- = entry_list_none", entry_list_none )
                
                # TODO
                #if group_tercodes:
                #   entry_dict_none_ig = group_by_ident( entry_list_none )
                
                #if group_tercodes:
                #   entry_list_path_ig = add_unique_items_grouped( ...
                # TODO: use entry_list_path_ig
                
                # entry_list_year = entry_list_ntc + entry_list_none
                logging.info( "add_unique_nones()" )
                entry_list_path = add_unique_items( language, "entry_list_none", entry_list_ntc, entry_list_none )
                logging.info( "entry_list_year: %d items" % len( entry_list_path ) )
                
                entry_list_year.extend( entry_list_path )           # different path_dict, so no duplicates 
                
            entry_list_total.extend( entry_list_year )              # different base_year, so no duplicates
            #entry_list_total = extend_nodups( entry_list_total, entry_list_year )  # avoid duplicates
            logging.info( "entry_list_total: %d items" % len( entry_list_total ) )
        
        entry_list_sorted = sort_entries( datatype, entry_list_total )
    # end classification
    
    json_string, cache_except = json_cache( entry_list_sorted, params, download_key )
    
    if cache_except is not None:
        logging.error( "caching of aggregation data failed" )
        logging.error( "length of json string: %d" % len( json_string ) )
    else:
        logging.debug( "aggregated json_string: \n%s" % json_string )
    
    collect_docs( params, download_dir, download_key )  # collect doc files in download dir

    logging.debug( "stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "aggregation took %s" % str_elapsed )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



def aggregate_historic_redun( params ):
    logging.info( "aggregate_historic_redun()" )
    
    redundant      = params[ "redundant" ]
    language       = params[ "language" ]
    datatype       = params[ "datatype" ]
    base_year      = params[ "base_year" ]
    group_tercodes = params[ "group_tercodes" ]
    path_lists     = params[ "path_lists_bylen" ]
    
    #num_path_lists = params[ "num_path_lists" ]
    num_path_lists = len( path_lists )
    
    entry_list_sorted = []
    entry_list_total  = []              # old: redundant = True
    record_dict_total = {}              # new: redundant = False
    
    # loop over the equal length path subgroups
    for pd, path_dict in enumerate( path_lists, start = 1 ):
        show_path_dict( num_path_lists, pd, path_dict )
        path_list      = path_dict[ "path_list" ]
        nkeys          = path_dict[ "nkeys" ]
        add_subclasses = path_dict[ "subclasses" ]
        
        params[ "path_list" ] = path_list
        
        prefix = "num"
        logging.debug( "-1- = entry_list_total" )
        #show_params( "params -1- = entry_list_total", params )
        
        sql_query = make_query( prefix, params, add_subclasses, value_total = True, value_numerical = True )
        sql_names, sql_resp = execute_only( sql_query )
        eng_data = {}
        entry_list = collect_fields( params, eng_data, sql_names, sql_resp )
        
        if entry_debug: 
            show_entries( "params -1- = entry_list", entry_list )
        if group_tercodes:
            entry_dict_ig = group_by_ident( entry_list )
        
        prefix = "ntc"
        entry_list_ntc = []
        if datatype == "1.02":      # not needed for 1.02 (and much data)
            logging.info( "SKIPPING -2- = entry_list_ntc" )
        else:                       # not needed for 1.02 (and much data)
            logging.debug( "-2- = entry_list_%s" % prefix )
            logging.debug( "path_list: %s" % params[ "path_list" ] )
            #show_params( "prefix=ntc -2- = entry_list_ntc", params )
            
            sql_query_ntc = make_query( prefix, params, add_subclasses, value_total = False, value_numerical = True )
            sql_names_ntc, sql_resp_ntc = execute_only( sql_query_ntc )
            eng_data_ntc = {}
            entry_list_ntc = collect_fields( params, eng_data_ntc, sql_names_ntc, sql_resp_ntc )
            
            if entry_debug: 
                show_entries( "prefix=ntc -2- = entry_list_ntc", entry_list_ntc )
            if group_tercodes:
                entry_dict_ntc_ig = group_by_ident( entry_list_ntc )
        
        prefix = "none"
        logging.debug( "-3- = entry_list_none" )
        #show_params( "prefix=none -3- = entry_list_none", params )
        
        sql_query_none = make_query( prefix, params, add_subclasses, value_total = False, value_numerical = False )   # non-numbers
        sql_names_none, sql_resp_none = execute_only( sql_query_none )
        eng_data_none = {}
        entry_list_none = collect_fields( params, eng_data_none, sql_names_none, sql_resp_none )
        
        if entry_debug: 
            show_entries( "params -3- = entry_list_none", entry_list_none )
        if group_tercodes:
            entry_dict_none_ig = group_by_ident( entry_list_none )
        
        # merge the the 3 lists (num, ntc, none)
        # entry_list_path = entry_list + entry_list_ntc
        logging.debug( "add_unique_ntcs()" )
        entry_list_path = add_unique_items( language, "entry_list_ntc", entry_list, entry_list_ntc )
        
        if group_tercodes:
            entry_list_path_ig = add_unique_items_grouped( language, "entry_dict_ntc", entry_dict_ig, entry_dict_ntc_ig )
        
        # TODO: use entry_list_path_ig
        
        # entry_list_collect = entry_list_path + entry_list_none
        logging.debug( "add_unique_nones()" )
        entry_list_collect = add_unique_items( language, "entry_list_none", entry_list_path, entry_list_none )
        logging.debug( "entry_list_collect: %d items" % len( entry_list_collect ) )
        
        # entry_list_total = entry_list_total + entry_list_collect
        entry_list_total.extend( entry_list_collect )      # different path_dict, so no duplicates
        #entry_list_total = extend_nodups( entry_list_total, entry_list_collect )   # avoid duplicates
        logging.debug( "entry_list_total: %d items" % len( entry_list_total ) )

    # sort the entries by path + value_unit
    entry_list_sorted = sort_entries( datatype, entry_list_total )
    logging.debug( "entry_list_sorted: %d items" % len( entry_list_sorted ) )
    #show_entries( "all", entry_list_sorted )

    return entry_list_sorted



def aggregate_historic( params ):
    logging.info( "aggregate_historic()" )
    
    datatype  = params[ "datatype" ]
    base_year = params[ "base_year" ]
    
    
    path_lists_bysub = params[ "path_lists_bysub" ]
    
    #num_path_lists = params[ "num_path_lists" ]
    
    #record_dict_total = SortedDict()    # json: dicts => tuples
    record_dict_total = {}
    
    
    # loop over the 2 yes/no subclassess path subgroups
    path_lists = params[ "path_lists_bysub" ]
    for pd, path_dict in enumerate( path_lists, start = 1 ):
        num_path_lists = len( path_lists )
        show_path_dict( num_path_lists, pd, path_dict )
        
        path_list      = path_dict[ "path_list" ]
        nkeys          = path_dict[ "nkeys" ]
        add_subclasses = path_dict[ "subclasses" ]
        
        params[ "path_list" ] = path_list
        params[ "num_path_lists" ] = num_path_lists
        
        prefix = "num"
        logging.debug( "-1- = entry_list_total" )
        #show_params( "params -1- = entry_list_total", params )
        
        sql_query = make_query( prefix, params, add_subclasses, value_total = True, value_numerical = True )
        sql_names, sql_resp = execute_only( sql_query, dict_cursor = True )
        nrecords = collect_records( record_dict_total, prefix, path_dict, params, sql_names, sql_resp )
        
    # loop over the equal length path subgroups
    path_lists = params[ "path_lists_bylen" ]
    for pd, path_dict in enumerate( path_lists, start = 1 ):
        path_list      = path_dict[ "path_list" ]
        nkeys          = path_dict[ "nkeys" ]
        add_subclasses = path_dict[ "subclasses" ]
        params[ "path_list" ] = path_list
        
        num_path_lists = len( path_list )
        show_path_dict( num_path_lists, pd, path_dict )
        
        prefix = "ntc"
        entry_list_ntc = []
        if datatype == "1.02":      # not needed for 1.02 (and much data)
            logging.info( "SKIPPING -2- = entry_list_ntc" )
        else:                       # not needed for 1.02 (and much data)
            logging.debug( "-2- = entry_list_%s" % prefix )
            #show_params( "prefix=ntc -2- = entry_list_ntc", params )
            
            sql_query_ntc = make_query( prefix, params, add_subclasses, value_total = False, value_numerical = True )
            sql_names_ntc, sql_resp_ntc = execute_only( sql_query_ntc, dict_cursor = True )
            nrecords = collect_records( record_dict_total, prefix, path_dict, params, sql_names_ntc, sql_resp_ntc )
        
        prefix = "none"
        logging.debug( "-3- = entry_list_none" )
        #show_params( "prefix=none -3- = entry_list_none", params )
        
        sql_query_none = make_query( prefix, params, add_subclasses, value_total = False, value_numerical = False )   # non-numbers
        sql_names_none, sql_resp_none = execute_only( sql_query_none, dict_cursor = True )
        nrecords = collect_records( record_dict_total, prefix, path_dict, params, sql_names_none, sql_resp_none )
    
    add_missing( record_dict_total, params )
    entry_list_sorted = sort_records( record_dict_total, params )
    #show_entries( "all", entry_list_sorted )

    return entry_list_sorted



def aggregate_modern_redun( params ):
    logging.info( "aggregate_modern_redun()" )
    entry_list_sorted = []

    return entry_list_sorted



def aggregate_modern( params ):
    logging.info( "aggregate_modern()" )
    entry_list_sorted = []

    return entry_list_sorted



def make_identifier( path, value_unit ):
    
    # ordered dict, sorted by keys
    ident_dict = collections.OrderedDict( sorted( path.items(), key = lambda t: t [ 0 ] ) )
    ident_dict[ "value_unit" ] = value_unit

    # identifier must be immutable
    #identifier = frozenset( ident_dict.items() )
    identifier = json.dumps( ident_dict.items(), encoding = "utf-8" )
    
    return identifier



def group_by_ident( entry_list ):
    logging.info( "group_by_ident()" )
    
    table_dict = {}
    
    for entry in entry_list:
        path = entry.get( "path" )
        value_unit = entry.get( "value_unit" )
        
        # ordered dict, sorted by keys
        ident_dict = collections.OrderedDict( sorted( path.items(), key = lambda t: t [ 0 ] ) )
        ident_dict[ "value_unit" ] = value_unit
        
        # identifier must be immutable
        #identifier = frozenset( ident_dict.items() )
        identifier = json.dumps( ident_dict.items(), encoding = "utf-8" )
        logging.debug( "identifier %s " % str( identifier ) )
        
        try:
            line_dict = table_dict[ identifier ]
            ter_codes = line_dict[ "ter_codes" ]
        except:
            line_dict = {}
            line_dict[ "datatype" ] = entry.get( "datatype" )
            line_dict[ "base_year" ] = entry.get( "base_year" )
            line_dict[ "path" ] = path
            line_dict[ "value_unit" ] = value_unit
            ter_codes = {}
        
        ter_code = entry.get( "ter_code" )
        total = entry.get( "total" )
        ter_codes[ ter_code ] = total
        line_dict[ "ter_codes" ] = ter_codes
        table_dict[ identifier ] = line_dict
    
    logging.info( "%d entries in dict" % len( table_dict ) )
    for key, value in table_dict.iteritems():
        logging.debug( "key: %s, \nvalue: %s" % ( key, str( value )) )
    
    return table_dict



def make_query( prefix, params, subclasses, value_total, value_numerical ):
    logging.debug( "make_query() %s " % prefix )
    """
    The msg can be one of three words, that 'encodes' how the params dict is made. 
    -1- "total" 
    -2- "ntc"   
    -3- "none"  
    """
    
    language       = params[ "language" ] 
    datatype       = params[ "datatype" ]
    classification = params[ "classification" ]
    base_year      = params[ "base_year" ]
    path_list      = params[ "path_list" ]
    ter_codes      = params[ "ter_codes" ]
    
    logging.debug( "language:        %s" % language )
    logging.debug( "datatype:        %s" % datatype )
    logging.debug( "classification:  %s" % classification )
    logging.debug( "base_year:       %s" % base_year )
    logging.debug( "path_list:       %s" % str( path_list ) )
    logging.debug( "subclasses:      %s" % subclasses )
    logging.debug( "ter_codes:       %s" % str( ter_codes ) )
    logging.debug( "value_total:     %s" % value_total )
    logging.debug( "value_numerical: %s" % value_numerical )
    
    path_keys = []
    for pdict in path_list:
        for k, v in pdict.iteritems():
            if k not in path_keys:
                path_keys.append( k )
                #logging.debug( "key: %s, value: %s" % ( k, v ) )
    
    path_keys.sort()
    
    # SELECT
    query  = "SELECT COUNT(*) AS datarecords"
    
    query += ", COUNT(*) - COUNT(value) AS data_active"
    
    if value_total:
        query += ", SUM(CAST(value AS DOUBLE PRECISION)) AS total"
    
    query += ", datatype, base_year, value_unit, ter_code"
    
    # paths
    for key in path_keys:
        query += ", %s" % key
    
    if subclasses:
        cls = "class"
        if classification == "historical":
            cls = "hist" + cls
        for k in range( 5, 10 ):
            query += ", %s%d" % ( cls, k )
    
    # FROM
    query += " FROM russianrepo_%s" % language
    
    # WHERE datatype AND base_year
    query += " WHERE datatype = '%s'" % datatype
    query += " AND base_year = '%s'" % base_year

    # AND value
    if value_numerical:
        query += " AND value <> ''"             # suppress empty values
        query += " AND value <> '.'"            # suppress a 'lone' "optional point", used in the table to flag missing data
        # plus an optional single . for floating point values, and plus an optional leading sign
        query += " AND value ~ '^[-+]?\d*\.?\d*$'"
    else:
        query += " AND (value = '' OR value = ' ' OR value = '.' OR value = '. ' OR value = NULL)"
    
    # AND path_dicts
    query += " AND ("
    for pd, path_dict in enumerate( path_list ):
        
        if prefix == "num":     # now number of keys may vary
            path_keys = []
            for k, v in path_dict.iteritems():
                if k not in path_keys:
                    path_keys.append( k )
                    #logging.debug( "key: %s, value: %s" % ( k, v ) )
            path_keys.sort()
        
        query += " ( "
        
        for pk, key in enumerate( path_keys ):
            val = path_dict[ key ]
            val = val.replace( "'", "''" )  # escape single quote by repeating it [also needs cursor.mogrify()]
            
            # suppress consecutive trailing dots?
            if prefix == "num":
                suppress = False
            else:
                suppress = True     # we only collect unique path strings (ignore value contents)
            
            if pk == 0:
                query += "(%s = '%s')" % ( key, val )
            else:
                if pk > 1 and suppress: # consecutive trailing dots
                    key_prev = path_keys[ pk - 1 ]
                    query += "( %s = '%s' OR (%s <> '. ' AND %s = '. ') )" % ( key, val, key_prev, key )
                else:
                    query += "(%s = '%s' OR %s = '. ')" % ( key, val, key )
            
            if pk + 1 < len( path_keys ):
                query += " AND "
        
        query += " )"
        if pd + 1 < len( path_list ):
            query += " OR"
    
    query += " )"
    
    # AND ter_codes IN / NOT IN
    if prefix == "num":
        l =  len( ter_codes )
        if l > 0:
            query += " AND ter_code IN ("
            
            for t, ter_code in enumerate( ter_codes ):
                query += " '%s'" % ter_code
                
                if t + 1 < l:
                    query += ", "
            
            query += ")"
    elif prefix == "ntc":
        l =  len( ter_codes )
        if l > 0:
            query += " AND ter_code NOT IN ("
            
            for t, ter_code in enumerate( ter_codes ):
                query += " '%s'" % ter_code
                
                if t + 1 < l:
                    query += ", "
            
            query += ")"
    
    # GROUP BY
    query += " GROUP BY datatype, base_year, "
    query += ", ".join( path_keys )
    
    if subclasses:
        cls = "class"
        if classification == "historical":
            cls = "hist" + cls
        for k in range( 5, 10 ):
            query += ", %s%d" % ( cls, k )
    
    query += ", value_unit"
    query += ", ter_code"
    
    # ORDER BY
    query += " ORDER BY datatype, base_year, "
    query += ", ".join( path_keys )
    
    if subclasses:
        cls = "class"
        if classification == "historical":
            cls = "hist" + cls
        for k in range( 5, 10 ):
            query += ", %s%d" % ( cls, k )
    
    query += ", value_unit"
    
    if prefix == "num":
        query += ", ter_code"
    
    query += ";"
    
    logging.debug( "make_query() %s" % query )
    
    return query



def show_path_dict( num_path_lists, pd, path_dict ):
    logging.debug( "show_path_dict()" )
    
    nkeys = path_dict[ "nkeys" ]
    path_list = path_dict[ "path_list" ]
    
    """
    add_subclasses = path_dict[ "subclasses" ]
    logging.debug( "path_list %d-of-%d, nkeys: %s, subclasses: %s, levels: %d" % 
        ( pd+1, len( path_lists ), nkeys, add_subclasses, len( path_list ) ) )
    """
    
    logging.debug( "path subgroup %d-of-%d, %d different paths of length %d" % ( pd, num_path_lists, len( path_list ), nkeys ) )

    
    for key, value in path_dict.iteritems():
        if key == "path_list":
            path_list = value
            logging.debug( "path_list: %s" % path_list )
            for p, path in enumerate( path_list ):
                logging.debug( "path_dict %d-of-%d, path: %s" % ( p+1, len( path_list ), path ) )
        else:
            logging.debug( "key: %s, value: %s" % ( key, value ) )



def show_params( info, params ):
    logging.info( "show_params() %s" % info )
    for key, value in params.iteritems():
        logging.info( "key: %s, value: %s" % ( key, value ) )



def show_entries( info, entries ):
    logging.info( "show_entries() %s" % info )
    logging.info( "%d items" % len( entries ) )
    nentries = len( entries )
    for e, entry in enumerate( entries ):
        logging.info( "%d-of-%d: %s" % ( e+1, nentries, str( entry ) ) )



def extend_nodups( tot_list, add_list ):
    for entry in add_list:
        if entry not in tot_list:
            tot_list.append( entry )

    return tot_list



# Filecatalog - Create filecatalog download link
@app.route( "/filecatalogdata", methods = [ "POST", "GET" ] )
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
    
    handle_names = [                # per 2018.04.23
        "hdl_errhs_population",     # ERRHS_1   39 files
        "hdl_errhs_labour",         # ERRHS_2    5 files
        "hdl_errhs_industry",       # ERRHS_3    0 files
        "hdl_errhs_agriculture",    # ERRHS_4   10 files
        "hdl_errhs_services",       # ERRHS_5    0 files
        "hdl_errhs_capital",        # ERRHS_6    0 files
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

    return send_file( zip_pathname, attachment_filename = zip_filename, as_attachment = True )



@app.route( "/download" )
def download():
    logging.info( "/download %s" % request.args )
    time0 = time()      # seconds since the epoch
    logging.debug( "download start: %s" % datetime.datetime.now() )
    
    configparser = get_configparser()
    dv_host    = configparser.get( "config", "dataverse_root" )
    ristat_key = configparser.get( "config", "ristatkey" )
    
    logging.debug( "dv_host: %s" % dv_host )
    logging.debug( "ristat_key: %s" % ristat_key )
    
    id_ = request.args.get( "id" )
    if id_:
        logging.debug( "id_: %s" % id_ )
        url = "https://%s/api/access/datafile/%s?key=%s&show_entity_ids=true&q=authorName:*" % ( dv_host, id_, ristat_key )
        
        logging.debug( "url: %s" % url )
        
        f = urllib2.urlopen( url )
        data = f.read()
        filetype = "application/pdf"
        
        if request.args.get( "filetype" ) == "excel":
            filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        logging.debug( "filetype: %s" % filetype )
        return Response( data, mimetype = filetype )
    
    key = request.args.get( "key" )
    if not key:
        return str( { "msg" : "Argument 'key' not found" } )
    
    logging.info( "download() key: %s" % key )
    zipping = True
    logging.debug( "download() zip: %s" % zipping )
    
    tmp_dir    = configparser.get( "config", "tmppath" )
    time_limit = int( configparser.get( "config", "time_limit" ) )
    top_download_dir = os.path.join( tmp_dir, "download" )
    logging.debug( "top_download_dir: %s" % top_download_dir )
    cleanup_downloads( top_download_dir, time_limit )       # remove too old downloads
    download_dir = os.path.join( top_download_dir, key )    # current download dir
    
    datafilter = {}
    datafilter[ "key" ] = key
    lex_lands, vocab_regs_terms, sheet_header, topic_name, params = preprocessor( use_gridfs, datafilter )
    
    xlsx_name = "%s.xlsx" % key
    
    redundant = params.get( "redundant", True )
    if redundant:
        pathname, msg = aggregate_dataset_fields( key, download_dir, xlsx_name, lex_lands, vocab_regs_terms, sheet_header, topic_name, params )
    else:
        pathname, msg = aggregate_dataset_records( key, download_dir, xlsx_name, lex_lands, vocab_regs_terms, sheet_header, topic_name, params )

    
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
        logging.info( "zip_filename: %s" % zip_filename )
        
        memory_file = BytesIO()
        with zipfile.ZipFile( memory_file, 'w' ) as zf:
            for root, sdirs, files in os.walk( download_dir ):
                for fname in files:
                    logging.info( "fname: %s" % fname )
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
        
        logging.debug( "stop: %s" % datetime.datetime.now() )
        str_elapsed = format_secs( time() - time0 )
        logging.info( "creating download took %s" % str_elapsed )
        
        return send_file( memory_file, attachment_filename = zip_filename, as_attachment = True )
    
    """
    clientcache = MongoClient()
    db_cache = clientcache.get_database( "datacache" )
    
    if use_gridfs:
        fs_cache = gridfs.GridFS( db_cache )
        result_str = fs_cache.get( str( key ) ).read()
        result = json.loads( result_str )
    else:
        result = db_cache.data.find( { "key": str( request.args.get( "key" ) ) } )
    
    for item in result:
        del item[ "key" ]
        del item[ "_id" ]
    
    dataset = json.dumps( item, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
        
    logging.debug( "stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "creating download took %s" % str_elapsed )
    
    return Response( dataset, mimetype = "application/json; charset=utf-8" )
    """



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
