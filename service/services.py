# -*- coding: utf-8 -*-

# VT-07-Jul-2016 latest change by VT
# FL-12-Dec-2016 use datatype in function documentation()
# FL-20-Jan-2017 utf8 encoding
# FL-03-Mar-2017 

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
import json
import logging
import os
import pandas as pd
import random
import re
import simplejson
import tables
import urllib
import urllib2
import psycopg2
import psycopg2.extras

from flask import Flask, Response, request
from pymongo import MongoClient
from StringIO import StringIO
from sys import exc_info
from rdflib import Graph, Literal, term

#import csv
#import getopt
#import glob
#import numpy as np
#import pprint
#import requests
#import unittest
#import xlwt

#from rdflib import BNode, Namespace, plugin
#from rdflib.serializer import Serializer
#from rdflib.namespace import DC, FOAF
#from twisted.web import http

sys.path.insert( 0, os.path.abspath( os.path.join( os.path.dirname( "__file__" ), './' ) ) )

from ristatcore.configutils import Configuration, DataFilter
#from ristatcore.configutils import Utils

from dataverse import Connection
from excelmaster import aggregate_dataset, preprocessor


forbidden = [ "classification", "action", "language", "path" ]


def connect():
    logging.debug( "connect()" )
    configparser = ConfigParser.RawConfigParser()
    
    RUSREP_CONFIG_PATH = os.environ[ "RUSREP_CONFIG_PATH" ]
    logging.info( "RUSREP_CONFIG_PATH: %s" % RUSREP_CONFIG_PATH )
    
    configpath = RUSREP_CONFIG_PATH
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
    
    #( row_count, dataset ) = load_regions( cursor, year, datatype, region, debug )
    return cursor



def classcollector( keywords ):
    logging.debug( "classcollector()" )
    logging.debug( keywords )
    
    classdict  = {}
    normaldict = {}
    for item in keywords:
        classmatch = re.search( r'class', item )
        if classmatch:
            classdict[ item ] = keywords[ item ]
        else:
            normaldict[ item ] = keywords[ item ]
    return ( classdict, normaldict )



def json_generator( cursor, json_dataname, data ):
    logging.debug( "json_generator() cursor: %s, json_dataname: %s" % ( cursor, json_dataname ) )
    logging.debug( "data: %s" % data )
    
    configparser = ConfigParser.RawConfigParser()
    
    RUSREP_CONFIG_PATH = os.environ[ "RUSREP_CONFIG_PATH" ]
    logging.info( "RUSREP_CONFIG_PATH: %s" % RUSREP_CONFIG_PATH )
    
    configpath = RUSREP_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    configparser.read( configpath )
    
    lang = 'en'
    try:
        qinput = json.loads( request.data )
        if 'language' in qinput:
            lang = qinput[ 'language' ]
    except:
        skip = 'yes'
    logging.debug( "language: %s" % lang )

    sql_names  = [ desc[ 0 ] for desc in cursor.description ]
    forbidden = { 'data_active', 0, 'datarecords', 1 }
    
    json_list = []
    json_hash = {}
    
    logging.debug( "%d values in data" % len( data ) )
    for value_str in data:
        data_keys    = {}
        extravalues = {}
        for i in range( len( value_str ) ):
            name  = sql_names[ i ]
            value = value_str[ i ]
            if value == ". ":
                #logging.debug( "i: %d, name: %s, value: %s" % ( i, thisname, value ) )
                # ". " marks a trailing dot in histclass or class: skip
                continue
            
            if name not in forbidden:
                data_keys[ name ] = value
            else:
                extravalues[ name ] = value

        # If aggregation check data output for 'NA' values
        if 'total' in data_keys:
            if extravalues[ 'data_active' ]:
                data_keys[ 'total' ] = 'NA'
        
        ( path, output ) = classcollector( data_keys )
        output[ 'path' ] = path
        json_list.append( output )
    
    # Cache
    clientcache = MongoClient()
    dbcache = clientcache.get_database( 'datacache' )

    json_hash[ json_dataname ] = json_list
    newkey = str( "%05.8f" % random.random() )
    json_hash[ 'url' ] = "%s/service/download?key=%s" % ( configparser.get( 'config', 'root' ), newkey )
    json_string = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    
    try:
        thisdata = json_hash
        del thisdata[ 'url' ]
        thisdata[ 'key' ] = newkey
        thisdata[ 'language' ] = lang
        result = dbcache.data.insert( thisdata )
    except:
        skip = 'something went wrong...'

    logging.debug( json_string )
    return json_string



def translated_vocabulary( newfilter ):
    logging.debug( "translated_vocabulary()" )
    logging.debug( newfilter )
    
    client = MongoClient()
    dbname = 'vocabulary'
    db = client.get_database( dbname )
    if newfilter:
        #vocab = db.data.find( { "YEAR": thisyear } )
        vocab = db.data.find( newfilter )
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
    
    #json_data = json_generator( cursor, 'years', result )
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



def load_topics( cursor ):
    logging.debug( "load_topics()" )
    data = {}
    sql = "select * from datasets.topics where 1=1"
    sql = sqlfilter( sql ) 

    # execute
    cursor.execute( sql )

    # retrieve the records from the database
    data = cursor.fetchall()
    json_data = json_generator( cursor, 'data', data )
    
    return json_data



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



def load_classes( cursor ):
    logging.debug( "load_classes()" )
    data = {}
    eng_data = {}
    total = 0
    classification = 'historical'
    if request.args.get( 'classification' ):
        classification = request.args.get( 'classification' )
    if request.args.get( 'language' ) == 'en':
        eng_data = translatedclasses( cursor, request.args )
        
    if request.args.get( 'overview' ):
        sql = "select distinct %s, year, datatype from datasets.classification where 1=1" % request.args.get( 'overview' ) 
        sql = sql + " AND %s <> '.'" % request.args.get( 'overview' )
        if request.args.get( 'year' ):
            sql = sql + " AND %s = '%s' " % ( 'year', request.args.get( 'year' ) )
        if request.args.get( 'datatype' ):
            sql = sql + " AND %s = '%s' " % ( 'datatype', request.args.get( 'datatype' ) )
    else:
        sql = "select * from datasets.classification where 1=1";
        sql = sqlconstructor( sql )

    # execute
    cursor.execute( sql )
    sql_names = [ desc[ 0 ] for desc in cursor.description ]

    # retrieve the records from the database
    datafilter = []
    data = cursor.fetchall()
    for dataline in data:
        datarow = {}
        active = ''
        for i in range( len( sql_names ) ):
            name = sql_names[ i ]
            if classification == 'historical':
                if name.find( "class", 0 ):
                    try:
                        nextvalue = dataline[ i+1 ]
                    except:
                        nextvalue = '.'
                    
                    if ( dataline[i] == '.' and nextvalue == '.' ):
                        skip = 'yes'
                    else:
                        toplevel = re.search( "(\d+)", name )
                        if name.find( "histclass10", 0 ):
                            value = dataline[ i ]
                            if value in eng_data:
                                value = eng_data[ value ][ 'class_eng' ]
                            datarow[ name ] = str( value ) 
                            if toplevel:
                                datarow[ "levels" ] = toplevel.group( 0 )
            
            if classification == 'modern':
                if name.find( "histclass", 0 ):
                    try:
                        nextvalue = dataline[ i+1 ]
                    except:
                        nextvalue = '.'
                    
                    if ( dataline[ i ] == '.' and nextvalue == '.' ):
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
        return json.dumps( datafilter, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )

    json_list = []
    json_hash = {}

    for value_str in data:
        data_keys = {}
        #sorted_keys = []
        for i in range( len( value_str ) ):
            name  = sql_names[ i ]
            value = value_str[ i ]
            if classification == 'historical':
                if not name.find( "class", 1 ):
                    data_keys[ name ] = value 
            else:
                data_keys[ name ] = value
        for i in range( 10, 1, -1 ):
            histclass = "histclass%s" % i
            mclass = "class%s" % i
        json_list.append( data_keys )
    #return str( json_list )
    
    json_data = json_generator( cursor, 'data', data )
    
    return json_data



def zap_empty_classes( item ):
    logging.debug( "zap_empty_classes()" )
    # trailing empty classes have value ". ", skip them; 
    # bridging empty classes have value '.', keep them; 
    #logging.debug( item )
    
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
    logging.debug( "load_vocabulary()" )
    
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

    if request.args.get( 'language' ) == 'en':
        thisyear = ''
        if request.args.get( 'base_year' ):
            if vocname == 'historical':     # FL why only for 'historical' ?
                vocab_filter = {}
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
        json_hash[ 'regions' ] = data
    elif vocname == 'modern':
        json_hash = data
    elif vocname == 'historical':
        json_hash = data
    else:
        json_hash[ 'data' ] = data
    json_data = json.dumps( json_hash, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    return json_data

    return json_hash



def load_histclasses( cursor ):
    logging.debug( "load_histclasses()" )
    data = {}
    sql = "select * from datasets.histclasses where 1=1"
    sql = sqlfilter( sql )

    # execute
    cursor.execute( sql )

    # retrieve the records from the database
    data = cursor.fetchall()
    json_data = json_generator( cursor, 'data', data )

    return json_data



def load_regions( cursor ):
    logging.debug( "load_regions()" )
    data = {}
    sql = "select * from datasets.regions where 1=1"
    sql = sqlfilter( sql )
    sql = sql + ';'
    # execute
    #return sql
    cursor.execute( sql )
    
    # retrieve the records from the database
    data = cursor.fetchall()
    json_data = json_generator( cursor, 'regions', data )
    return json_data



def load_data( cursor, year, datatype, region, debug ):
    logging.debug("load_data()")
    data = {}
    
    # execute our Query
    # Example SQL: cursor.execute("select * from russianrepository where year='1897' and datatype='3.01' limit 1000")
    #    for key, value in request.args.iteritems():
    #        extra = "%s<br>%s=%s<br>" % (extra, key, value)

    query = "select * from russianrepository WHERE 1 = 1 "
    query = sqlfilter( query )
    if debug:
        print( "DEBUG " + query + " <br>\n" )
    query += ' order by territory asc'
    
    # execute
    cursor.execute( query )
    
    # retrieve the records from the database
    records = cursor.fetchall()
    
    row_count = 0
    i = 0
    for row in records:
        i = i + 1
        data[ i ] = row
        #print row[ 0 ]
    json_data = json_generator( cursor, 'data', records )
    
    return json_data



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



class Histclass( tables.IsDescription ):
    histclass1 = tables.StringCol( 256, pos = 0 )
    histclass2 = tables.StringCol( 256, pos = 0 )
    histclass3 = tables.StringCol( 256, pos = 0 )



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


# ==============================================================================
app = Flask( __name__ )
logging.debug( __file__ )


@app.route( '/' )
def test():
    logging.debug( "test()" )
    description = 'Russian Repository API Service v.0.1<br>/service/regions<br>/service/topics<br>/service/data<br>/service/histclasses<br>/service/years<br>/service/maps (reserved)<br>'
    return description



@app.route( '/export' )
def export():
    logging.debug( "export()" )
    settings = Configuration()
    keys = [ "intro", "intro_rus", "datatype_intro", "datatype_intro_rus", "note", "note_rus", "downloadpage1", "downloadpage1_rus" "downloadclick", "downloadclick_rus", "warningblank", "warningblank_rus", "mapintro", "mapintro_rus" ]
    exportkeys = {}
    for ikey in keys:
        if ikey in settings.config:
            exportkeys[ ikey ] = settings.config[ ikey ]
    result = json.dumps( exportkeys, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    return Response( result, mimetype = 'application/json; charset=utf-8' )



@app.route( '/topics' )
def topics():
    logging.debug( "topics()" )
    cursor = connect()
    data = load_topics( cursor )
    return Response( data, mimetype = 'application/json; charset=utf-8' )



@app.route( '/vocab' )
def vocab():
    logging.debug( "vocab()" )
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
        return Response( jsondump, mimetype = 'application/json; charset=utf-8' )



@app.route( '/vocabulary' )
def getvocabulary():
    logging.debug( "getvocabulary()" )
    data = translated_vocabulary()
    json_string = json.dumps( data, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    return Response( json_string, mimetype = 'application/json; charset=utf-8' )



@app.route( '/aggregation', methods = ['POST', 'GET' ] )
def aggregation():
    logging.debug( "aggregation()" )
    
    python_vertuple = sys.version_info
    python_version = str( python_vertuple[ 0 ] ) + '.' + str( python_vertuple[ 1 ] ) + '.' + str( python_vertuple[ 2 ] )
    logging.info( "Python version: %s" % python_version  )
    
    thisyear = ''
    
    try:
        #qinput = json.loads( request.data )
        qinput = simplejson.loads( request.data )
        """
        from simplejson import JSONDecoder
        jd = JSONDecoder()
        qinput = dict(jd( request.data ))
        """
        """
        _globals = globals()
        logging.debug( "globals:" )
        for key in _globals:
            logging.debug( "key: %s, value: %s" % ( key, _globals[ key]  ) )
        
        logging.debug( "\nlocals:" )
        _locals = locals()
        for key in _locals:
            logging.debug( "key: %s, value: %s" % ( key, _locals[ key]  ) )
        
        logging.debug( "\ndir:" )
        _dir = dir()
        logging.debug( dir() )
        
        
        print( qinput )
        """
        logging.debug( "number of keys in request.data: %d" % len( qinput ) )
        for key in qinput:
            value = qinput[ key ]
            if key == "path":
                logging.debug( "path: %s" % u"\u2265" )
                for pdict in value:
                    logging.debug( str( pdict ) )
                    for pkey in pdict:
                        pvalue = pdict[ pkey ]
                        logging.debug( "key: %s, value: %s" % ( pkey, pvalue ) )
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
        # extra = "%s<br>%s=%s<br>" % (extra, key, value)
        if 'language' in qinput:
            if qinput[ 'language' ]== 'en':
                vocab_filter = {}
                base_year = qinput.get( "base_year" )
                if base_year:
                    vocab_filter[ "YEAR" ] = base_year
                datatype = qinput.get( "datatype" )
                if datatype:
                    vocab_filter[ "DATATYPE" ] = datatype
                
                eng_data = translated_vocabulary( vocab_filter )
                units = translated_vocabulary( { "vocabulary": "ERRHS_Vocabulary_units" } )
                logging.debug( "translated_vocabulary returned %d items" % len( units ) )
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
                if value in eng_data:
                    #value = eng_data[ value ][ 'class_rus' ]
                    value = eng_data[ value ]
                    logging.debug( "name: %s, value: %s" % ( name, value ) )
                
                #sql[ 'where' ] += "%s='%s' AND1 " % ( name, value )
                sql[ 'where' ] += "%s AND " % get_sql_query( name, value )
                sql[ 'condition' ] += "%s, " % name
                known_fields[ name ] = value
            
            elif name == 'path':
                full_path = qinput[ name ]
                top_sql = 'AND ('
                for path in full_path:
                    #tmp_sql = ' ('
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
                        
                        if value in eng_data:
                            logging.debug( "xkey: %s,     value: %s" % ( xkey, value ) )
                            value = eng_data[ value ]

                        """
                        p_inhabitants = value.find( "inhabitants" )
                        if p_inhabitants != -1:
                            logging.warning( "TRANSLATION ERROR: xkey: %s, value: %s" % ( xkey, value ) )
                            logging.warning( "value in eng_data: %s" % ( value in eng_data ) )
                            #del type
                            #logging.warning( "value type = %s" % type( value ) )
                        """
                        
                        #sql_local[ xkey ] = "%s='%s', " % ( xkey, value )
                        sql_local[ xkey ] = "(%s='%s' OR %s='. '), " % ( xkey, value, xkey )
                        
                        if not known_fields.has_key( xkey ):
                            known_fields[ xkey ] = value
                            sql[ 'condition' ] += "%s, " % xkey
                    
                    #tmp_sql += '1=1 ) '
                    #top_sql += tmp_sql + " OR "
                    
                    if sql_local:
                        sql[ 'internal' ] += ' ('
                        for key in sql_local:
                            sql_local[ key ] = sql_local[ key ][ :-2 ]
                            sql[ 'internal' ] += "%s AND " % sql_local[ key ]
                        
                        sql[ 'internal' ] = sql[ 'internal' ][ :-4 ]
                        sql[ 'internal' ] += ') OR'
    
    sql[ 'internal' ] = sql[ 'internal' ][ :-3 ]

    #select sum(cast(value as double precision)), value_unit from russianrepository where datatype = '1.02' and year='2002' and histclass2 = '' and histclass1='1' group by histclass1, histclass2, value_unit;
    # value may contain '.'& '. ' entries that cannot be SUMmed
    # => manually count with python, skipping '.'& '. ' entries
    #sql_query = "SELECT COUNT(*) AS datarecords, COUNT(*) - COUNT(value) AS data_active, SUM(CAST(value AS DOUBLE PRECISION)) AS total, value_unit, ter_code"
    sql_query  = "SELECT COUNT(*) AS datarecords" 
    sql_query += ", SUM(CAST(value AS DOUBLE PRECISION)) AS total"
    sql_query += ", COUNT(*) AS count"
    sql_query += ", COUNT(*) - COUNT(value) AS data_active"
    sql_query += ", value_unit, ter_code"

    if sql[ 'where' ]:
        logging.debug( "where: %s" % sql[ "where" ] )
        sql_query += ", %s" % sql[ 'condition' ]
        sql_query  = sql_query[ :-2 ]
        sql_query += " FROM russianrepository WHERE %s" % sql[ 'where' ]
        sql_query  = sql_query[ :-4 ]
        
    if sql[ 'internal' ]:
        logging.debug( "internal: %s" % sql[ "internal" ] )
        sql_query += " AND (%s) " % sql[ 'internal' ]
    
    sql_query += " AND value ~ '^\d+$'"      # regexp (~) to require that value only contains digits
    
    sql[ "group_by" ] = " GROUP BY value_unit, ter_code, "
    for field in known_fields:
        sql[ "group_by" ] += "%s," % field
    sql[ "group_by" ] = sql[ "group_by" ][ :-1 ]
    logging.debug( "group_by: %s" % sql[ "group_by" ] )
    sql_query += sql[ "group_by" ]

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
    for iclass in class_list:
        if sql[ "order_by" ] != " ORDER BY ":
            sql[ "order_by" ] += ", "
        sql[ "order_by" ] += "%s" % iclass
    logging.debug( "order_by: %s" % sql[ "order_by" ] )
    sql_query += " %s" % sql[ "order_by" ]

    logging.debug( "sql_query: %s" % sql_query )

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
                    #continue
                    pass
                
            #for value in item:
                if value in eng_data:
                    value = value.encode( 'UTF-8' )
                    value = eng_data[ value ]
                if thisname not in forbidden:
                    finalitem.append( value )
            
            finaldata.append( finalitem )
        
        json_data = json_generator( cursor, 'data', finaldata )
        
        logging.debug( "json_data before return Response:" )
        logging.debug( json_data )
        return Response( json_data, mimetype = 'application/json; charset=utf-8' )

    return str( '{}' )



@app.route( '/aggregate', methods = [ 'POST', 'GET' ] )
def aggr():
    logging.debug( "aggr()" )
    data = {}
    sqlfields = ''
    sqlkeys = ''
    eng_data = {}
    cursor = connect()
    total = 0
    
    try:
        qinput = json.loads( request.data )
    except:
        return '{}'

    forbidden = [ "classification", "action", "language", "path" ]
    if cursor:
        #     extra = "%s<br>%s=%s<br>" % (extra, key, value)
        if 'language' in qinput:
            if qinput[ 'language' ] == 'en':
                eng_data = translatedclasses( cursor, request.args )
        
        #return str( eng_data )
        for key in qinput:
            if key not in forbidden:
                value = qinput[ key ]
                if sqlfields:
                    sqlfields = "%s, %s" % ( sqlfields, key )
                    sqlkeys   = "%s, %s" % ( sqlkeys, key )
                else:
                    sqlfields = key
                    sqlkeys   = key
        
        sql = "select sum(cast(value as double precision)) as value, value_unit, territory, year, histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10, %s from russianrepository where 1=1" % sqlfields
        for name in qinput:
            if not name in forbidden:
                value = str( qinput[ name ] )
                if value in eng_data:
                    value = eng_data[ value ][ 'class_rus' ]
                if value[ 0 ] != "[":
                    sql+= " AND %s = '%s'" % ( name, qinput[ name ] )
                else:
                    orvalue = ''
                    for val in qinput[ name ]:
                        if val in eng_data:
                            val = eng_data[ val ][ 'class_rus' ]
                        orvalue += " '%s'," % ( val )
                    orvalue = orvalue[ :-1 ]
                    sql+= " AND %s IN (%s)" % ( name, orvalue )
            elif name == 'path':
                full_path = qinput[ name ]
                top_sql = 'AND ('
                for path in full_path:
                    tmp_sql = ' ('
                    for xkey in path:
                        value = path[ xkey ]
                        if value in eng_data:
                            value = str( eng_data[ value ][ 'class_rus' ] )
                        try:
                            tmp_sql += " %s = '%s' AND " % ( xkey, value.decode( 'utf-8' ) )
                        except:
                            tmp_sql += " %s = '%s' AND " % ( xkey, value )
                    tmp_sql += '1=1 ) '
                    top_sql += tmp_sql + " OR "
                top_sql = top_sql[ :-3 ]
                top_sql += ')'
                sql += top_sql

        #sql = sqlconstructor( sql )
        wheresql = "group by histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10, territory, year, %s, value_unit, value" % sqlkeys
        sql = "%s %s" % ( sql, wheresql )

        # execute
        cursor.execute( sql )
        sql_names = [ desc[ 0 ] for desc in cursor.description ]

        # retrieve the records from the database
        data = cursor.fetchall()
        result = []
        chain = {}
        regions = {}
        
        class inchain( object ):
            def __init__( self, name ):
                self.name = name

        hclasses = {}
        for row in data:
            line_item = {}
            data_item = {}
            for i in range( 0, len( sql_names ) ):
                if row[ i ] != None:
                    value = row[ i ]
                    data_item[ sql_names[ i ] ] = value

            if data_item[ 'value' ] != None:
                location = data_item[ 'territory' ]
                if location in eng_data:
                    location = eng_data[ location ][ 'class_eng' ]

                if location in regions:
                    regions[ location ] += data_item[ 'value' ]
                else:
                    regions[ location ] = data_item[ 'value' ]

            for i in range( 0, len( sql_names ) ):
                if row[ i ] != None:
                    value = row[ i ]
                    if value in eng_data:
                        value = eng_data[ value ][ 'class_eng' ]

                    if sql_names[ i ] == 'value':
                        if float( value ).is_integer():
                            value = int( value )
                    line_item[ sql_names[ i ] ] = value 
            try:
                total += float( line_item[ 'value' ] )
            except:
                itotal = 'NA'

            sorted_items = {}
            order = []
            for item in sorted( line_item ):
                order.append( item )
            for item in order:
                sorted_items[ item ] = line_item[ item ]
            x = collections.OrderedDict( sorted( sorted_items.items() ) )
            #return json.dumps( x )

            vocab = {}
            for i in range( 1,10 ):
                histkey = "histclass%s" % str( i )
                if histkey in x:
                    vocab[ histkey ] = x[ histkey ]
                    del x[ histkey ]
            
            x[ 'histclases' ] = vocab
            result.append( x )
        
        #json_data = json_generator( cursor, 'data', result )
        #result = hclasses
        final = {}
        final[ 'url' ] = 'http://data.sandbox.socialhistoryservices.org/service/download?id=1144&filetype=excel'
        final[ 'total' ] = total
        final[ 'regions' ] = regions
        final[ 'data' ] = result
        
        return Response( json.dumps( final ), mimetype = 'application/json; charset=utf-8' )



@app.route( '/download' )
def download():
    logging.debug( "download()" )
    clioinfra = Configuration()

    if request.args.get( 'id' ):
        host = "datasets.socialhistory.org"
        url = "https://%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, request.args.get('id'), clioinfra.config['ristatkey'])
        f = urllib2.urlopen( url )
        pdfdata = f.read()
        filetype = "application/pdf"
        if request.args.get( 'filetype' ) == 'excel':
            filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return Response( pdfdata, mimetype = filetype )
    
    if request.args.get( 'key' ):
        clientcache = MongoClient()
        datafilter = {}
        datafilter[ 'key' ] = request.args.get( 'key' )
        ( lexicon, regions ) = preprocessor( datafilter )
        full_path = "%s/%s.xlsx" % ( clioinfra.config[ 'tmppath' ],request.args.get( 'key' ) )
        filename = aggregate_dataset( full_path, lexicon, regions )
        with open( filename, 'rb' ) as f:
            datacontents = f.read()
        filetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        return Response( datacontents, mimetype = filetype )

        dbcache = clientcache.get_database( 'datacache' )
        result = dbcache.data.find( { "key": str( request.args.get( 'key' ) ) } )
        for item in result:
            del item[ 'key' ]
            del item[ '_id' ]
            dataset = json.dumps( item, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
            return Response( dataset, mimetype = 'application/json; charset=utf-8' )
    else:
        return 'Not found'



@app.route( '/documentation' )
def documentation():
    logging.debug( "documentation()" )
    cursor = connect()
    clioinfra = Configuration()
    host = "datasets.socialhistory.org"
    connection = Connection( host, clioinfra.config[ 'ristatkey' ] )
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
            for files in dataframe[ 'data' ][ 'latestVersion' ][ 'files' ]:
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
    
    return Response( json.dumps( papers ), mimetype = 'application/json; charset=utf-8' )



@app.route( '/histclasses' )
def histclasses():
    logging.debug( "histclasses()" )
    #cursor = connect()
    #data = load_histclasses( cursor )
    data = load_vocabulary( 'historical' )
    return Response( data, mimetype = 'application/json; charset=utf-8' )



@app.route( '/classes' )
def classes():
    logging.debug( "classes()" )
    #cursor = connect()
    #data = load_classes( cursor )
    data = load_vocabulary( 'modern' )
    
    return Response( data, mimetype = 'application/json; charset=utf-8' )



@app.route( '/years' )
def years():
    logging.debug( "years()" )
    cursor = connect()
    settings = DataFilter( request.args )
    datatype = ''
    if 'datatype' in settings.datafilter:
        datatype = settings.datafilter[ 'datatype' ]
    data = load_years( cursor, datatype )
    
    return Response( data, mimetype = 'application/json; charset=utf-8' )



@app.route( '/regions' )
def regions():
    logging.debug( "regions()" )
    cursor = connect()
    #data = load_regions( cursor )
    data = load_vocabulary( "ERRHS_Vocabulary_regions" )
    
    return Response( data, mimetype = 'application/json; charset=utf-8' )



@app.route( '/data' )
def data():
    logging.debug( "data()" )
    cursor = connect()
    year = 0
    datatype = '1.01'
    region = 0
    debug = 0
    data = load_data( cursor, year, datatype, region, debug )
    
    return Response( data, mimetype = 'application/json; charset=utf-8' )



@app.route( '/translate' )
def translate():
    logging.debug( "translate()" )
    cursor = connect()
    if cursor:
        data = {}
        sql = "select * from datasets.classmaps where 1=1";
        sql = sqlfilter( sql )

        # execute
        cursor.execute( sql )

        # retrieve the records from the database
        data = cursor.fetchall()
        json_data = json_generator( cursor, 'data', data )
        
        return Response( json_data, mimetype = 'application/json; charset=utf-8' )



@app.route( '/filter', methods = [ 'POST', 'GET' ] )
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
        return Response( json_data, mimetype = 'application/json; charset=utf-8' )
    else:
        return ''



# http://bl.ocks.org/mbostock/raw/4090846/us.json
@app.route( '/maps' )
def maps():
    logging.debug( "maps()" )
    donors_choose_url = "http://bl.ocks.org/mbostock/raw/4090846/us.json"
    response = urllib2.urlopen( donors_choose_url )
    json_response = json.load( response )
    return Response( json_response, mimetype = 'application/json; charset=utf-8' )


"""
@app.route('/')                 def test():
@app.route('/export')           def export():
@app.route('/topics')           def topics():
@app.route('/vocab')            def vocab():
@app.route('/vocabulary')       def getvocabulary():
@app.route('/aggregation'       def aggregation():
@app.route('/aggregate'         def aggr():
@app.route('/download')         def download():
@app.route('/documentation')    def documentation():
@app.route('/histclasses')      def histclasses():
@app.route('/classes')          def classes():
@app.route('/years')            def years():
@app.route('/regions')          def regions():
@app.route('/data')             def data():
@app.route('/translate')        def translate():
@app.route('/filter'            def login(settings=''):     # FL filter -> login ?
@app.route('/maps')             def maps():
"""

if __name__ == '__main__':
    app.run()
