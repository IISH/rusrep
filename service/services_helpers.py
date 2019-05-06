# -*- coding: utf-8 -*-

"""
Functions needed by services.py and/or services_obs.py
FL-06-May-2019 Created

def get_configparser():
def get_connection():
def make_query( prefix, params, add_subclasses, value_total = True, value_numerical = True ):
def execute_only( sql_query, dict_cursor = False ):

def show_path_dict( num_path_lists, pd, path_dict ):
def show_params( info, params ):
def show_entries( info, entries ):

def format_secs( seconds ):
"""

# future-0.17.1 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

import sys
reload( sys )
sys.setdefaultencoding( "utf8" )

import ConfigParser
import json
import logging
import os
import psycopg2
import psycopg2.extras

from datetime import date, datetime
from time import time, localtime



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
# get_configparser()


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
# get_connection()


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
    
    try:
        ter_codes = params[ "ter_codes" ]   # historical
    except:
        ter_codes = []                      # modern, ter_codes not specified
    
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
# make_query()


def execute_only( sql_query, dict_cursor = False ):
    logging.debug( "execute_only () dict_cursor: %s" % dict_cursor )
    # only sql execute part, response handling separate collect_fields()

    time0 = time()      # seconds since the epoch
    logging.debug( "execute_only() start: %s" % datetime.now() )

    connection = get_connection()
    
    if dict_cursor:
        cursor = connection.cursor( cursor_factory = psycopg2.extras.RealDictCursor )
    else:
        cursor = connection.cursor()
    
    sql_query = cursor.mogrify( sql_query )     # needed if single quote has been escaped by repeating it
    cursor.execute( sql_query )
    
    logging.debug( "query execute_only stop: %s" % datetime.now() )
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
# execute_only()


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
# show_path_dict()


def show_params( info, params ):
    logging.info( "show_params() %s" % info )
    for key, value in params.iteritems():
        logging.info( "key: %s, value: %s" % ( key, value ) )
# show_params()


def show_entries( info, entries ):
    logging.info( "show_entries() %s" % info )
    logging.info( "%d items" % len( entries ) )
    nentries = len( entries )
    for e, entry in enumerate( entries ):
        logging.info( "%d-of-%d: %s" % ( e+1, nentries, str( entry ) ) )
# show_entries()


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
# format_secs()

# [eof]
