# -*- coding: utf-8 -*-

"""
TODO
The functions aggregate_historic_items_redun() and aggregate_modern_items_redun() return a sorted list of dicts. 
For compatbility with the sequel functions json_cache_items() and the functions in preprocessor.py, 
the new functions aggregate_historic_items() and aggregate_modern_items() also return the same sorted list of dicts.
It would be better to keep the record_dicts of the new functions, and completely overhaul the 
messy sequel functons. 

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
FL-30-Apr-2019 downloads adapted
FL-13-May-2019 cleanup, reorganize
FL-14-May-2019 filecatalogue download Excel conversion spurious '.0'
FL-21-Jan-2020 VALUE_NA, VALUE_DOT, VALUE_NONE
FL-13-Feb-2020 Separate handling for VALUE_DOT; VALUE_NONE => VALUE_MIX
FL-26-Jun-2020 Separate directories for dataverse and downloads for frontend
FL-28-Jun-2020 use backend proxy name if present, else backend root name

def loadjson( json_dataurl ):                                   # called by documentation()
def topic_counts( language, datatype ):                         # called by topics()
def load_years( cursor, datatype, classification ):             # called by years()
def load_vocabulary( vocab_type, language, datatype, base_year ):       # called by several functions
def load_vocab( config_parser, vocab_fname, vocab, pos_rus, pos_eng ):  # called by process_csv()
def translate_item( item, eng_data ):                           # called by load_vocabulary()
def translate_vocabulary( vocab_filter, classification = None ):# was called by load_vocabulary()
def zap_empty_classes( item ):                                  # called by load_vocabulary()
def aggregate_historic_items( params )                          # called by aggregation()
def aggregate_modern_items( params )                            # called by aggregation()
def strip_subclasses( path ):                                   # called by aggregation()
def group_levels( path_list ):                                  # called by aggregation()
def json_cache_items( entry_list, params, download_key ):       # called by aggregation() and indicators()
def collect_docs( params, download_dir, download_key ):         # called by aggregation() and filecatalogdata()
def show_record_dict( dict_name, record_dict, sort = False ):   # called by collect_records()
def collect_records( record_dict_total, prefix, path_dict, params, eng_data, sql_names, sql_resp ): # called by aggregate_*_items()
def add_missing_valstr( record_dict_total, params ):            # called by aggregate_historic_items()
def sort_records( record_dict_total, base_year = None ):        # called by aggregate_*_items()
def process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx ):  # called by filecatalogdata()
def cleanup_downloads( download_dir, time_limit ):              # called by download()

@app.route( '/' )                                               def test():
@app.route( "/documentation" )                                  def documentation():
@app.route( "/topics" )                                         def topics():
@app.route( "/years" )                                          def years():
@app.route( "/regions" )                                        def regions():
@app.route( "/histclasses" )                                    def histclasses():
@app.route( "/classes" )                                        def classes():
#@app.route( "/indicators", methods = [ "POST", "GET" ] )        def indicators():  # no longer used?
@app.route( "/aggregation", methods = ["POST" ] )               def aggregation():
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

import codecs
import collections                      # OrderedDict()
import csv
import gridfs                           # mongodb
import json
import logging
import os
os.environ[ "MPLCONFIGDIR" ] = "/tmp"   # matplotlib (used by pandas) needs tmp a dir
import pandas as pd                     # csv, excel
import psycopg2
import psycopg2.extras
import random                           # download key
import re
import shutil                           # downloads
import simplejson
import uuid                             # download key
import urllib
import urllib2
import zipfile                          # downloads

from backports import configparser
from configutils import DataFilter
from copy import deepcopy
from datetime import date, datetime
from io import BytesIO                  # downloads
from flask import Flask, jsonify, Response, request, send_from_directory, send_file, session
from jsonmerge import merge
from pymongo import MongoClient
from socket import gethostname          # json hash
#from sortedcontainers import SortedDict    # json: dicts => tuples
from sys import exc_info
from time import time, localtime

from dataverse import Connection
from excelmaster import aggregate_dataset_fields, aggregate_dataset_records, preprocessor
from services_helpers import get_connection, format_secs, get_configparser, make_query, execute_only, show_path_dict
from services_deprec import aggregate_historic_items_redun, aggregate_modern_items_redun

sys.path.insert( 0, os.path.abspath( os.path.join( os.path.dirname( "__file__" ), "./" ) ) )

use_gridfs = True

vocab_debug = False

VALUE_NA_EN = "na"
VALUE_NA_RU = "нет данных"

VALUE_DOT_EN = "missing in source"
VALUE_DOT_RU = "пропущена в источнике"

VALUE_MIX_EN = "cannot aggregate at this level"
VALUE_MIX_RU = "агрегация на этом уровне невозможна"


def loadjson( json_dataurl ):
    logging.debug( "loadjson() %s" % json_dataurl )

    req = urllib2.Request( json_dataurl )
    opener = urllib2.build_opener()
    f = opener.open( req )
    dataframe = simplejson.load( f )
    return dataframe
# loadjson()


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
# topic_counts()


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
    
    logging.debug( sql )
    
    cursor.execute( sql )
    resp = cursor.fetchall()
    result = collections.OrderedDict()
        
    for val in resp:
        if val[ 0 ]:
            result[ val[ 0 ] ] = val[ 1 ]
    for year in years:
        if int( year ) not in result:
            result[ int( year ) ] = 0
    logging.debug( "result: %s" % result )
    
    json_string = json.dumps( result, encoding = "utf-8" )

    return json_string
# load_years()


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
    
    logging.debug( "vocab_name: %s" % vocab_name )
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
# load_vocabulary()


def load_vocab( config_parser, vocab_fname, vocab, pos_rus, pos_eng ):
    # load_vocab() from autoupdate.py
    global Nexcept
    logging.info( "load_vocab() vocab_fname: %s" % vocab_fname )
    # if pos_extar is not None, it is needed to make the keys and/or values unique
    handle_name = "hdl_vocabularies"
    dv_dir = config_parser.get( "config", "dv_dir" )
    
    vocab_dir = os.path.join( dv_dir, "dataverse_dst", "vocab/csv", handle_name )
    logging.info( "vocab_dir: %s" % vocab_dir )
    vocab_path = os.path.join( vocab_dir, vocab_fname )
    logging.info( "vocab_path: %s" % vocab_path )
    
    #vocab_file = open( vocab_path, "r" )
    vocab_file = codecs.open( vocab_path, "r", encoding = "utf-8" )
    
    nline = 0
    for csv_line in iter( vocab_file ):
        csv_line.strip()        # zap '\n'
        #logging.debug( csv_line )
        if nline == 0:
            pass        # skip header
        else:
            parts = csv_line.split( '|' )
            rus = parts[ pos_rus ].strip()
            eng = parts[ pos_eng ].strip()
            
            
            if vocab_fname == "ERRHS_Vocabulary_regions.csv":
                # The regions (territorium) vocab is special: 
                # vocab = dict(), not bidict()
                # the original rus terms contain a lot of noise, but the codes 
                # and eng translations are unique and OK. For both forward and 
                # inverse lookup the code is the key, and either rus or eng the value. 
                terr = parts[ 0 ].strip()
                terr_d = { "terr" : terr }
                terr_s = json.dumps( terr_d )
                rus_eng_d = { "rus" : rus, "eng" : eng }
                logging.debug( "terr: %s, rus_eng: %s %s" % ( terr, rus_eng_d[ "rus" ], rus_eng_d[ "eng" ] ) )
                rus_eng_s = json.dumps( rus_eng_d )
                try:
                    vocab[ terr_s ] = rus_eng_s
                except:
                    Nexcept += 1
                    type_, value, tb = sys.exc_info()
                    msg = "%s: %s %s" % ( type_, vocab_fname, value )
                    logging.error( msg )
                    #sys.stderr.write( "%s\n" % msg )
            elif vocab_fname == "ERRHS_Vocabulary_units.csv":
                if pos_rus == pos_eng:              # a bit of a hack
                    # vocab is a dict(), not bidict(), 
                    # used for decimals, either from rus or from eng
                    decimals_str = parts[ 2 ]
                    decimals = int( float( decimals_str.strip() ) )     # strip(): sometimes a spurious space
                    eng_d = decimals                # using value for decimals
                    if pos_rus == 0:                # rus => decimals
                        logging.debug( "rus: %s, decimals: %d" % ( rus, decimals ) )
                        vocab[ rus ] = decimals     # using key for rus
                    else:                           # eng => decimals
                        logging.debug( "eng: %s, decimals: %d" % ( eng, decimals ) )
                        vocab[ eng ] = decimals     # using key for eng
                else:
                    # vocab is a bidict(), not dict()
                    # used for normal bidict translation
                    logging.debug( "rus: %s, eng: %s" % ( rus, eng ) )
                    rus_s = json.dumps( { "rus" : rus } )
                    eng_s = json.dumps( { "eng" : eng } )
                    vocab[ rus_s ] = eng_s
            else:
                if vocab_fname == "ERRHS_Vocabulary_histclasses.csv":
                    byear = parts[ 2 ].strip()
                    dtype = parts[ 3 ].strip()
                    rus_d = { "rus" : rus, "byear" : byear, "dtype" : dtype }
                    eng_d = { "eng" : eng, "byear" : byear, "dtype" : dtype }
                
                elif vocab_fname == "ERRHS_Vocabulary_modclasses.csv":
                    dtype = parts[ 2 ][ 4: ].strip()
                    rus_d = { "rus" : rus, "dtype " : dtype  }
                    eng_d = { "eng" : eng, "dtype " : dtype  }
                else:
                    continue
                
                logging.debug( "rus: %s, eng: %s" % ( rus_d[ "rus" ], eng_d[ "eng" ] ) )
            
                rus_s = json.dumps( rus_d )
                eng_s = json.dumps( eng_d )
                
                """
                # test
                rus_d = json.loads( rus_s )
                eng_d = json.loads( eng_s )
                logging.debug( "%s rus_d: %s, %s eng_d: %s" % ( type( rus_d, ), rus_d, type( eng_d ), eng_d ) )
                logging.debug( "rus: %s, eng: %s" % ( rus_d[ "rus" ], eng_d[ "eng" ] ) )
                """
                
                try:
                    vocab[ rus_s ] = eng_s
                except:
                    Nexcept += 1
                    type_, value, tb = sys.exc_info()
                    msg = "%s: %s %s" % ( type_, vocab_fname, value )
                    logging.error( msg )
                    #sys.stderr.write( "%s\n" % msg )
                
        nline += 1
    
    vocab_file.close()

    return vocab
# load_vocab()


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
# translate_item()


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
# translate_vocabulary()


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
# zap_empty_classes()


def aggregate_historic_items( params ):
    logging.info( "aggregate_historic_items()" )
    
    datatype  = params[ "datatype" ]
    base_year = params[ "base_year" ]
    
    #record_dict_total = SortedDict()    # json: dicts => tuples
    record_dict_total = {}
    
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
        logging.debug( "-1- = entry_list_%s" % prefix )
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
        logging.debug( "-3- = entry_list_%s" % prefix )
        #show_params( "prefix=none -3- = entry_list_none", params )
        
        sql_query_none = make_query( prefix, params, add_subclasses, value_total = False, value_numerical = False )   # non-numbers
        sql_names_none, sql_resp_none = execute_only( sql_query_none, dict_cursor = True )
        nrecords = collect_records( record_dict_total, prefix, path_dict, params, sql_names_none, sql_resp_none )
    
    add_missing_valstr( record_dict_total, params )
    
    # input: data as dict, output: data as list, for compatibility with old list approach
    entry_list_sorted = sort_records( record_dict_total )
    #show_entries( "all", entry_list_sorted )

    return entry_list_sorted
# aggregate_historic_items()


def aggregate_modern_items( params ):
    logging.info( "aggregate_modern_items()" )
    
    language       = params[ "language" ]
    datatype       = params[ "datatype" ]
    path_lists     = params[ "path_lists_bylen" ]
    
    num_path_lists = len( path_lists )
    
    #record_dict_total = SortedDict()    # json: dicts => tuples
    #record_dict_total = {}
    entry_list_sorted = []
    
    # modern classification does not provide a base_year; 
    # loop over base_years, and accumulate results.
    base_years = [ "1795", "1858", "1897", "1959", "2002" ]
    #base_years = [ "1795" ]    # test single year
    
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
    
    for base_year in base_years:
        logging.info( "base_year: %s" % base_year )
        params[ "base_year" ] = base_year
        
        record_dict_year = {}
        
        for pd, path_dict in enumerate( path_lists, start = 1 ):
            #show_path_dict( num_path_lists, pd, path_dict )
            
            path_list = path_dict[ "path_list" ]
            add_subclasses = path_dict[ "subclasses" ]
            
            params[  "path_list" ] = path_list
            
            prefix = "ntc"
            logging.debug( "-1- = entry_list_%s" % prefix )
            #show_params( "params -1- = entry_list_ntc", params )
            
            sql_query_ntc = make_query( prefix, params, add_subclasses, value_total = True, value_numerical = True )
            sql_names_ntc, sql_resp_ntc = execute_only( sql_query_ntc, dict_cursor = True )
            nrecords = collect_records( record_dict_year, prefix, path_dict, params, sql_names_ntc, sql_resp_ntc )
            
            prefix = "none"
            logging.debug( "-2- = entry_list_%s" % prefix )
            #show_params( "params -2- = entry_list_none", params )
            
            sql_query_none = make_query( prefix, params, add_subclasses, value_total = False, value_numerical = False )   # non-numbers
            sql_names_none, sql_resp_none = execute_only( sql_query_none, dict_cursor = True )
            nrecords = collect_records( record_dict_year, prefix, path_dict, params, sql_names_none, sql_resp_none )
    
        #add_missing_valstr( record_dict_year, params )
    
        # input: data as dict, output: data as list, for compatibility with old list approach
        entry_list_year = sort_records( record_dict_year, base_year )           # sort per year
        entry_list_sorted.extend( entry_list_year )
    
    #show_entries( "all", entry_list_sorted )

    return entry_list_sorted
# aggregate_modern_items()


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
        stripped_entry = deepcopy( old_entry )
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
# strip_subclasses()


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
# group_levels()


def json_cache_items( entry_list, params, download_key ):
    # cache entry_list in mongodb with download_key as key
    
    time0 = time()      # seconds since the epoch
    logging.debug( "json_cache_items() start: %s" % datetime.now() )
    
    logging.debug( "json_cache_items() # entries in entry_list: %d" %  len( entry_list ) )
    logging.debug( "json_cache_items() params: %s" % str( params ) )
    
    config_parser = get_configparser()
    root = config_parser.get( "config", "root" )
    
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
    
    logging.debug( "stop: %s" % datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "json_cache_items() caching took %s" % str_elapsed )
    
    return json_string, exc_value
# json_cache_items()


def collect_docs( qinput, download_dir, download_key ):
    # collect the accompanying docs in the download dir
    time0 = time()      # seconds since the epoch
    logging.debug( "collect_docs() start: %s" % datetime.now() )
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
    
    config_parser = get_configparser()
    dv_dir = config_parser.get( "config", "dv_dir" )
    
    doc_dir = os.path.join( dv_dir, "dataverse_dst", "doc", "hdl_documentation" )
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

    logging.debug( "stop: %s" % datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "collect_docs() caching took %s" % str_elapsed )
# collect_docs()


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
# show_record_dict()


def collect_records( records_dict, sql_prefix, path_dict, params, sql_names, sql_resp ):
    logging.debug( "collect_records() sql_prefix: %s" % sql_prefix )
    
    logging.debug( "records_dict: %d records" % len( records_dict ) )
    # sql_names & sql_resp from execute_only()
    logging.debug( "sql_resp: %d records" % len( sql_resp ) )
    if len( sql_resp ) == 0:
        return

    time0 = time()      # seconds since the epoch
    logging.debug( "collect_records() start: %s" % datetime.now() )
    
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
    value_na  = ""
    value_dot = ""
    value_mix = ""
    if language.upper() == "EN":
        value_na  = VALUE_NA_EN
        value_dot = VALUE_DOT_EN
        value_mix = VALUE_MIX_EN
    elif language.upper() == "RU":
        value_na  = VALUE_NA_RU
        value_dot = VALUE_DOT_RU
        value_mix = VALUE_MIX_RU
    
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
        
        # historical: only add requested ter_codes
        # modern: add all ter_codes
        if( classification == "historical" and ter_code in ter_codes_req ) or classification == "modern":
            total = rec_dict.get( "total" )
            logging.debug( "new ter_code: %s, total: %s" % ( ter_code, total ) )
            
            new_float = None
            new_isfloat = False
            if sql_prefix in [ "num", "ntc" ]:
                try:
                    new_float = float( total )
                    new_isfloat = True
                except:
                    pass
            
            elif sql_prefix == "none":
                total = value_dot
            else:
                total = value_na
            
            if ter_code not in ter_code_dict.keys():
                # (only) new ter_code
                if new_isfloat:
                    ter_code_dict[ ter_code ] = new_float
                else:
                    ter_code_dict[ ter_code ] = total
            else:   # combine old & new
                old_total = ter_code_dict[ ter_code ]
                old_isfloat = False
                try:
                    old_float = float( old_total )
                    old_isfloat = True
                    logging.debug( "old_total: %f" % old_total )
                except:
                    pass
                
                combined = ""
                if old_isfloat and new_isfloat:
                    combined = old_float + new_float
                elif old_total == value_dot and new_total == value_dot:
                    combined = value_dot
                elif old_total == value_mix and new_total == value_mix:
                    combined = value_mix
                
                elif old_isfloat and total == value_mix:
                    combined = old_float
                elif new_isfloat and old_total == value_mix:
                    ter_code_dict[ ter_code ] = new_float
                else:
                    combined = value_mix
                
                ter_code_dict[ ter_code ] = combined
                logging.debug( "combined ter_code: %s, total: %s" % ( ter_code, combined ) )

    
    nrecords = len( records_dict.keys() )
    logging.debug( "paths in records_dict %d" % nrecords )
    
    show_record_dict( sql_prefix, records_dict )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "collect_records() took %s" % str_elapsed )
    
    return nrecords
# collect_records()


def add_missing_valstr( records_dict, params ):
    time0 = time()      # seconds since the epoch
    logging.debug( "add_missing_valstr() start: %s" % datetime.now() )

    # add value strings for empty fields
    language = params.get( "language" )
    value_na = ""
    value_dot = ""
    if language.upper() == "EN":
        value_na = VALUE_NA_EN
        value_dot = VALUE_DOT_EN
    elif language.upper() == "RU":
        value_na = VALUE_NA_RU
        value_dot = VALUE_DOT_RU

    ter_codes_req = params.get( "ter_codes" )      # requested ter_codes (only historical)

    for path_unit_str in records_dict:
        ter_code_dict = records_dict[ path_unit_str ]
        for ter_code in ter_codes_req:
            try:
                ter_code_dict[ ter_code ]
            except:
                ter_code_dict[ ter_code ] = value_na
    
    logging.debug( "add_missing_valstr() final # of paths in records_dict %d" % len( records_dict.keys() ) )
    
    #show_record_dict( "all", records_dict )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "add_missing_valstr() took %s" % str_elapsed )
    
    return records_dict
# add_missing_valstr()


def sort_records( records_dict, base_year = None ):
    # input: dict (+ base_year)
    # output: list
    time0 = time()      # seconds since the epoch
    logging.debug( "sort_records() start: %s" % datetime.now() )
    logging.debug( "sort_records() sorting %d records" % len( records_dict ) )
    
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
            "path"       : path,
            "value_unit" : value_unit,
            "ter_codes"  : ter_code_list
        }
        
        if base_year is not None:   # inject for modern (differentiate between the years)
            record_dict[ "base_year" ] = base_year
        
        entry_list.append( record_dict )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "sort_records() took %s" % str_elapsed )
    
    return entry_list
# sort_records()


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
        
        """
        TODO This fix now works, but it is too slow, especially for many files. 
        So make it part of the nightly update!
        # rounding of data in value column is specified in ERRHS_Vocabulary_units.csv
        # Equality of pos_rus anf pos_eng is a hack, used to flag decimals from either rus or eng
        config_parser = get_configparser()
        vocab_units = dict()
        if csv_filename.endswith( "-ru.csv" ):
            vocab_units = load_vocab( config_parser, "ERRHS_Vocabulary_units.csv", vocab_units, 0, 0 )
        elif csv_filename.endswith( "-en.csv" ):
            vocab_units = load_vocab( config_parser, "ERRHS_Vocabulary_units.csv", vocab_units, 1, 1 )
        else:
            logging.error( "csv_filename does not end with either '-ru.csv' or '-en.csv'" )
        logging.debug( str( vocab_units ) )
        
        # spurious '.0' added to integer values; re-round column VALUE (uppercase in csv)
        for row in df1.index:
            unit = df1[ "VALUE_UNIT" ][ row ]
            decimals = int( vocab_units[ unit ] )
            
            if decimals == 0:
                try:
                    val = df1[ "VALUE" ][ row ]
                    df1[ "VALUE" ][ row ] = str( long( round( float( val ), decimals ) ) )
                except:
                    pass
            else:
                break
        """
        
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
# process_csv()


def cleanup_downloads( download_dir, time_limit ):
    # remove too old downloads
    logging.debug( "cleanup_downloads() time_limit: %d, download_dir: %s" % ( time_limit, download_dir ) )
    
    dt_now = datetime.now()
    
    f_deleted = 0
    d_deleted = 0
    file_list = os.listdir( download_dir )
    
    for file_name in file_list:
        file_path = os.path.abspath( os.path.join( download_dir, file_name ) )
        
        if os.path.isdir( file_path ):
            dir_name = file_name
            dir_path = file_path
            mtime = os.path.getmtime( dir_path )
            dt_file = datetime.fromtimestamp( mtime )
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
            dt_file = datetime.fromtimestamp( mtime )
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
# cleanup_downloads()

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
# / test()


# Documentation - Get documentation needed to show files in help pop-up at GUI step 1
@app.route( "/documentation" )
def documentation():
    logging.debug( "/documentation" )
    logging.debug( "request.args: %s" % request.args )

    language = request.args.get( "lang", "en" )
    logging.debug( "language: %s" % language )

    config_parser = get_configparser()
    dv_dir = config_parser.get( "config", "dv_dir" )

    download_dir = os.path.join( dv_dir, "dataverse_dst/doc" )
    download_fname = "doclist-" + language + ".json"
    download_path = os.path.join( download_dir, download_fname )
    
    logging.debug( "download_dir: %s" % download_dir )
    logging.debug( "download_fname: %s" % download_fname )
    logging.debug( "download_path: %s" % download_path )
    
    file_json = codecs.open( download_path, 'r', "utf-8" )
    docs_json = file_json.read()
    file_json.close()

    logging.debug( docs_json )
    return Response( docs_json, mimetype = "application/json; charset=utf-8" )
# /documentation documentation()



def documentation_dv():
    logging.debug( "/documentation_dv" )
    #logging.debug( "Python version: %s" % sys.version )

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

    config_parser = get_configparser()
    dv_host       = config_parser.get( "config", "dataverse_root" )
    root          = config_parser.get( "config", "root" )
    ristat_key    = config_parser.get( "config", "ristatkey" )
    ristat_name   = config_parser.get( "config", "ristatname" )
    ristatdocs    = config_parser.get( "config", "hdl_documentation" )
    
    logging.info( "dv_host: %s" % dv_host )
    logging.debug( "ristat_key: %s" % ristat_key )
    logging.info( "ristat_name: %s" % ristat_name )
    
    scheme   = "http"
    use_root = root
    try:
        proxy = config_parser.get( "config", "proxy" )
        if proxy:       # not empty?
            scheme   = "https"
            use_root = proxy
    except:
        pass
    
    api_root = "%s://%s" % ( scheme, use_root )
    logging.info( "api_root: %s" % api_root )
    
    papers = []
    
    try:
        connection = Connection( dv_host, ristat_key )
        logging.debug( "connection succeeded" )
    except:
        logging.error( "connection failed" )
        #type_, value, tb = sys.exc_info()
        #logging.error( "%s" % value )
        etype = sys.exc_info()[ 0:1 ]
        value = sys.exc_info()[ 1:2 ]
        logging.error( "etype: %s, value: %s" % ( etype, value ) )
        return Response( json.dumps( papers ), mimetype = "application/json; charset=utf-8" )
    
    dataverse = connection.get_dataverse( ristat_name )
    if not dataverse:
        logging.info( "ristat_key: %s" % ristat_key )
        logging.error( "COULD NOT GET A DATAVERSE CONNECTION" )
        return Response( json.dumps( papers ), mimetype = "application/json; charset=utf-8" )
    
    logging.debug( "get_dataverse succeeded" )
    logging.debug( "title: %s" % dataverse.title )
    
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
# /documentation documentation_dv()


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
        
        topic_dict_out = deepcopy( topic_dict )
        topic_dict_out[ "byear_counts" ] = count_dict
        topics_array_out.append( topic_dict_out )
    
    topics_dict_out = { "data" : topics_array_out }
    logging.debug( "/topics return Response" )
    return Response( json.dumps( topics_dict_out ), mimetype = "application/json; charset=utf-8" )
# /topics topics()


# Years - Get available years for year selection at GUI step 2
@app.route( "/years" )
def years():
    time0 = time()      # seconds since the epoch
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
    
    logging.debug( "json_string: %s" % str( json_string ) )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "/years took %s" % str_elapsed )
    logging.debug( "/years return Response" )
    return Response( json_string, mimetype = "application/json; charset=utf-8" )
# /years years()


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
# /regions regions()


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
# /histclasses histclasses()


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
# /classes classes()


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
    json_string, cache_except = json_cache_items( entry_list, language, "data", download_key )
    
    logging.debug( "json_string before return Response:" )
    logging.debug( json_string )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )
# /indicators indicators()
"""


# Aggregation - Preview User selection post and gets data to build preview in GUI step 5 
@app.route( "/aggregation", methods = ["POST" ] )
def aggregation():
    logging.debug( "" )
    logging.info( "/aggregation" )
    time0 = time()      # seconds since the epoch
    logging.debug( "aggregation start: %s" % datetime.now() )
    
    qinput = simplejson.loads( request.data )
    logging.debug( "qinput: %s" % str( qinput ) )

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
    
    method = qinput.get( "Method" )
    redundant = False
    if method == "old":
        redundant = True
    logging.info( "method: %s, redundant: %s" % ( method, redundant ) )
    
    params = {
        "language"       : language,
        "datatype"       : datatype,
        "classification" : classification,
        "add_subclasses" : add_subclasses,
        "key_set"        : key_set,
        "redundant"      : redundant        # also used in download
    }

    # Split input path in subgroups with the same numbr of keys
    # input path WITH subclasses parameter, NOT path_stripped
    path_lists_bylen, path_lists_bysub = group_levels( path )
    
    params[ "path_lists_bylen" ] = path_lists_bylen
    params[ "path_lists_bysub" ] = path_lists_bysub

    
    if classification == "historical":
        # historical classification has base_year from qinput
        params[ "base_year" ] = base_year
        
        ter_codes = qinput.get( "ter_code", [] )
        logging.debug( "ter_codes: %s" % ter_codes )
        params[ "ter_codes" ] = ter_codes       # with ter_codes
        
        if redundant:   # old
            entry_list_sorted = aggregate_historic_items_redun( params )
        else:           # new, redundant = False
            entry_list_sorted = aggregate_historic_items( params )
    
    elif classification == "modern":
        # modern classification does not have base_year or ter_codes from qinput
        if redundant:   # old
            entry_list_sorted = aggregate_modern_items_redun( params )
        else:           # new, redundant = False
            entry_list_sorted = aggregate_modern_items( params )
    # end classification
    
    # download key
    uuid4 = str( uuid.uuid4() )
    logging.debug( "uuid4: %s" % uuid4 )
    download_key = "%s-%s-%s-%s-%s" % ( language, classification[ 0 ], datatype, base_year, uuid4 )
    #download_key = "%s-%s-%s-%s=%s" % ( language, classification[ 0 ], datatype, base_year, uuid4 )
    logging.debug( "download_key: %s" % download_key )
    
    #json_string = str( "{}" )
    #cache_except = None
    json_string, cache_except = json_cache_items( entry_list_sorted, params, download_key )
    #json_string, cache_except = json_cache_records( entry_list_sorted, params, download_key )  # TODO
    
    if cache_except is not None:
        logging.error( "caching of aggregation data failed" )
        logging.error( "length of json string: %d" % len( json_string ) )
    else:
        logging.debug( "aggregated json_string: \n%s" % json_string )
    
    # download dir
    config_parser = get_configparser()
    tmp_dir = config_parser.get( "config", "tmppath" )
    download_dir = os.path.join( tmp_dir, "download", download_key )
    if not os.path.exists( download_dir ):
        os.makedirs( download_dir )
    
    # collect doc files in download dir
    collect_docs( params, download_dir, download_key )

    logging.debug( "stop: %s" % datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "aggregation took %s" % str_elapsed )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )
# /aggregation aggregation()


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
    
    to_xlsx  = True     # convert csv to excel; add copyright sheet
    also_csv = False    # also copy csv to download?
    
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
    
    handle_names = [                # per 2019.05.13
        "hdl_errhs_population",     # ERRHS_1   40 files
        "hdl_errhs_labour",         # ERRHS_2   12 files
        "hdl_errhs_industry",       # ERRHS_3    0 files
        "hdl_errhs_agriculture",    # ERRHS_4   10 files
        "hdl_errhs_services",       # ERRHS_5    0 files
        "hdl_errhs_capital",        # ERRHS_6    0 files
        "hdl_errhs_land"            # ERRHS_7    2 files
    ]
    
    config_parser = get_configparser()
    try:
        config_fname = config_parser.config.get( "config_fname" )
        logging.debug( "config_fname: %s" % config_fname )
    except:
        type_, value, tb = sys.exc_info()
        msg = "%s: %s %s" % ( type_, filecatalogdata, value )
        logging.error( msg )
        #sys.stderr.write( "%s\n" % msg )
    
    root       = config_parser.get( "config", "root" )
    dv_dir     = config_parser.get( "config", "dv_dir" )
    tmp_dir    = config_parser.get( "config", "tmppath" )
    time_limit = int( config_parser.get( "config", "time_limit" ) )
    
    scheme   = "http"
    use_root = root
    try:
        proxy = config_parser.get( "config", "proxy" )
        if proxy:       # not empty?
            logging.info( "proxy: %s" % proxy )
            scheme   = "https"
            use_root = proxy
    except:
        pass
    
    api_root = "%s://%s" % ( scheme, use_root )
    logging.info( "api_root: %s" % api_root )
    
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
            csv_subdir = "csv-ru"
            extra = "-ru"
        
        # check if filecat xlsx has been pre-computed
        xlsx_filename = "ERRHS_%s_data_%s%s.xlsx" % ( datatype, base_year, extra )
        logging.debug( "xlsx_filename: %s" % xlsx_filename )
        fcat_subdir = "fcat-" + language
        xlsx_dir = os.path.join( dv_dir, "dataverse_dst", fcat_subdir, handle_name )
        xlsx_pathname = os.path.join( xlsx_dir, xlsx_filename )
        
        if os.path.isfile( xlsx_pathname ):
            logging.debug( "copy existing filecat xlsx: %s" % xlsx_pathname )
            shutil.copy2( xlsx_pathname, download_dir )
        else:
            csv_dir = os.path.join( dv_dir, "dataverse_dst", csv_subdir, handle_name )
            csv_filename = "ERRHS_%s_data_%s%s.csv" % ( datatype, base_year, extra )
            logging.debug( "csv_filename: %s" % csv_filename )
            
            csv_pathname = os.path.join( csv_dir, csv_filename )
            logging.debug( "csv_pathname: %s" % csv_pathname )
            
            # convert csv to excel; add copyright sheet
            process_csv( csv_dir, csv_filename, download_dir, language, to_xlsx )
            
            if also_csv:   # also copy csv for download 
                shutil.copy2( csv_pathname, download_dir )
            else:
                logging.debug( "skipped for download: %s" % csv_filename )
    
    # zip download dir
    zip_filename = "%s.zip" % download_key
    logging.debug( "zip_filename: %s" % zip_filename )
    zip_dirname = os.path.join( top_download_dir, download_key )
    logging.debug( "zip_dirname: %s" % zip_dirname )
    shutil.make_archive( zip_dirname, "zip", zip_dirname )
    
    #hostname = gethostname()
    json_hash = { "url_zip" : api_root + "/service/filecatalogget?zip=" + zip_filename }
    json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    
    logging.debug( json_string )
    logging.debug( "/filecatalogdata before Response()" )
    return Response( json_string, mimetype = "application/json; charset=utf-8" )
# /filecatalogdata filecatalogdata()


# Filecatalog - Get filecatalog download zip
@app.route( "/filecatalogget", methods = [ "POST", "GET" ] )
def filecatalogget():
    logging.debug( "/filecatalogget" )
    logging.debug( "request.args: %s" % str( request.args ) )
    zip_filename = request.args.get( "zip" )
    
    if zip_filename is None:
        json_hash = { "zip_filename" : "undefined" }
        json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
        logging.debug( json_string )
        logging.debug( "/filecatalogget before Response()" )
        return Response( json_string, mimetype = "application/json; charset=utf-8" )
        
    logging.debug( "zip_filename: %s" % zip_filename )

    config_parser = get_configparser()
    tmp_dir = config_parser.get( "config", "tmppath" )
    top_download_dir = os.path.join( tmp_dir, "download" )
    zip_pathname = os.path.join( top_download_dir, zip_filename )
    logging.debug( "zip_pathname: %s" % zip_pathname )

    return send_file( zip_pathname, attachment_filename = zip_filename, as_attachment = True )
# /filecatalogget filecatalogget()


@app.route( "/download" )
def download():
    logging.info( "/download %s" % request.args )
    time0 = time()      # seconds since the epoch
    logging.debug( "download start: %s" % datetime.now() )
    
    config_parser = get_configparser()
    dv_host       = config_parser.get( "config", "dataverse_root" )
    ristat_key    = config_parser.get( "config", "ristatkey" )
    
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
    
    tmp_dir    = config_parser.get( "config", "tmppath" )
    time_limit = int( config_parser.get( "config", "time_limit" ) )
    top_download_dir = os.path.join( tmp_dir, "download" )
    logging.debug( "top_download_dir: %s" % top_download_dir )
    cleanup_downloads( top_download_dir, time_limit )       # remove too old downloads
    download_dir = os.path.join( top_download_dir, key )    # current download dir
    
    datafilter = {}
    datafilter[ "key" ] = key
    params, topic_name, sheet_header, lex_lands, vocab_regs_terms = preprocessor( use_gridfs, datafilter )
    
    xlsx_name = "%s.xlsx" % key
    
    redundant = params.get( "redundant", True )
    if redundant:
        pathname, msg = aggregate_dataset_fields( key, download_dir, xlsx_name, params, topic_name, sheet_header, lex_lands, vocab_regs_terms )
    else:
        pathname, msg = aggregate_dataset_records( key, download_dir, xlsx_name, params, topic_name, sheet_header, lex_lands, vocab_regs_terms )

    
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
        
        logging.debug( "stop: %s" % datetime.now() )
        str_elapsed = format_secs( time() - time0 )
        logging.info( "creating download took %s" % str_elapsed )
        
        return send_file( memory_file, attachment_filename = zip_filename, as_attachment = True )
# /download_dir download()


# Get cron autoupdate log to inspect for errors
@app.route( "/logfile" )
def getupdatelog():
    logging.debug( "/logfile" )
    
    config_parser = get_configparser()
    etl_dir = config_parser.get( "config", "etlpath" )
    
    log_filename = "autoupdate.log"
    log_pathname = os.path.join( etl_dir, log_filename )
        
    return send_file( log_pathname, attachment_filename = log_filename, as_attachment = True )
# /logfile getupdatelog()


if __name__ == "__main__":
    app.run()

# [eof]
