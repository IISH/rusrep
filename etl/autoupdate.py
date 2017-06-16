#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This script retrieves RiStat files from Dataverse and updates MongoDB. 
- vocabulary *.tab files are stored locally and/or in MongoDB
- gets vocabulary data from PostgreSQL and stores it MongoDB
- transforms binary xlsx spreadsheet files to csv files

Notice: dpe/rusrep/etl contains a xlsx2csv.py copy; 
better use the curent version from PyPI

VT-07-Jul-2016 latest change by VT
FL-03-Mar-2017 Py2/Py3 compatibility: using pandas instead of xlsx2csv to create csv files
FL-03-Mar-2017 Py2/Py3 compatibility: using future-0.16.0
FL-27-Mar-2017 Also download documentation files
FL-17-May-2017 postgresql datasets.topics counts
FL-14-Jun-2017 latest change

def loadjson( apiurl ):
def empty_dir( dst_dir ):
def documents_by_handle( configparser, handle_name, dst_dir, dv_format = "", copy_local = False, to_csv = False, remove_xlsx = True ):
def update_documentation( configparser, copy_local, remove_xlsx = False ):
def update_vocabularies( configparser, mongo_client, dv_format, copy_local = False, to_csv = False, remove_xlsx = False):
def retrieve_handle_docs( configparser, handle_name, dv_format = "", copy_local = False, to_csv = False, remove_xlsx = False ):
def row_count( configparser ):
def clear_postgres( configparser ):
def store_handle_docs( configparser, handle_name ):
def test_csv_file( path_name ):
def filter_csv( csv_dir, in_filename ):
def update_handle_docs( configparser, mongo_client ):
def clear_mongo( mongo_client ):
def topic_counts( configparser ):
def translate_csv( configparser, handle_name ):
def format_secs( seconds ):
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

import ConfigParser
import datetime
import getpass
import json
import logging
import os
import pandas as pd
import psycopg2
import psycopg2.extras
import re
import sys
import urllib
import urllib2
import shutil
import simplejson
import StringIO

from bidict import bidict
from pymongo import MongoClient
from time import ctime, time
from vocab import vocabulary, classupdate


sys.path.insert( 0, os.path.abspath( os.path.join(os.path.dirname( "__file__" ), './' ) ) )
sys.path.insert( 0, os.path.abspath( os.path.join(os.path.dirname( "__file__" ), '../' ) ) )
sys.path.insert( 0, os.path.abspath( os.path.join(os.path.dirname( "__file__" ), '../service' ) ) )
#print( sys.path )
#print( "pwd:", os.getcwd() )

from dataverse import Connection

from service.configutils import DataFilter

# column comment_source of postgresql table russianrepository of db ristat
COMMENT_LENGTH_MAX_DB = 4096
# primary key for russianrepository table
pkey = None


def loadjson( url ):
    logging.debug( "loadjson() %s" % url )

    req = urllib2.Request( url )
    opener = urllib2.build_opener()
    f = opener.open( req )
    dataframe = simplejson.load( f )
    return dataframe



def empty_dir( dst_dir ):
    logging.info( "empty_dir() %s" % dst_dir )
    logging.info( "removing previous downloads" )
    
    for root, sdirs, files in os.walk( dst_dir ):
        #logging.info( "root:  %s" % str( root ) )
        #logging.info( "sdirs: %s" % str( sdirs ) )
        #logging.info( "files: %s" % str( files ) )
        
        if files is not None:
            files.sort()
        for fname in files:
            file_path = os.path.join( root, fname )
            # only delete files we recognize
            if fname.startswith( "ERRHS_" ):
                mtime = os.path.getmtime( file_path )
                timestamp = ctime( mtime )
                logging.info( "removing file: (created: %s) %s" % ( timestamp, file_path ) )
                try:
                    os.unlink( file_path )
                except:
                    type, value, tb = sys.exc_info()
                    logging.error( "%s" % value )
        
        if sdirs is not None:
            sdirs.sort()
        for dname in sdirs:
            # only delete dirs we recognize
            if dname.startswith( "hdl_" ):
                dir_path = os.path.join( root, dname )
                mtime = os.path.getmtime( dir_path )
                timestamp = ctime( mtime )
                logging.info( "removing dir: (created: %s) %s" % ( timestamp, dir_path ) )
                try:
                    shutil.rmtree( dir_path )
                except:
                    type, value, tb = sys.exc_info()
                    logging.error( "%s" % value )
        
        mtime = os.path.getmtime( root )
        timestamp = ctime( mtime )
        logging.info( "removing root: (created: %s) %s" % ( timestamp, root ) )
        try:
            shutil.rmtree( root )
        except:
            type, value, tb = sys.exc_info()
            logging.error( "%s" % value )



def documents_by_handle( configparser, handle_name, dst_dir, dv_format = "", copy_local = False, to_csv = False, remove_xlsx = True ):
    logging.info( "documents_by_handle() copy_local: %s, to_csv: %s" % ( copy_local, to_csv ) )
    logging.debug( "handle_name: %s" % handle_name )
    logging.info( "dst_dir: %s, dv_format: %s, copy_local: %s, to_csv: %s" % ( dst_dir, dv_format, copy_local, to_csv ) )
    
    host = "datasets.socialhistory.org"
    ristat_key = configparser.get( "config", "ristatkey" )
    logging.debug( "host: %s" % host )
    logging.debug( "ristat_key: %s" % ristat_key )
    
    connection = Connection( host, ristat_key )
    dataverse  = connection.get_dataverse( 'RISTAT' )
    
    logging.debug( "title: %s" % dataverse.title )
    #datasets = dataverse.get_datasets()
    
    settings = DataFilter( '' )
    papers = []
    ids = {}
    
    kwargs_xlsx2csv = { 'delimiter' : '|', 'lineterminator' : '\n' }
    
    sep = str(u'|').encode('utf-8')
    kwargs_pandas   = { 'sep' : sep, 'line_terminator' : '\n' }
    
    tmp_dir = configparser.get( "config", "tmppath" )
    if copy_local:
        download_dir = os.path.join( tmp_dir, "dataverse", dst_dir, handle_name )
        logging.info( "downloading dataverse files to: %s" % download_dir )
        if os.path.exists( download_dir ):
            empty_dir( download_dir )           # remove previous files
    
    csv_dir = ""
    if dst_dir == "xlsx":
        csv_dir = os.path.join( tmp_dir, "dataverse", "csv", handle_name )
    elif dst_dir == "vocab/xlsx":
        csv_dir = os.path.join( tmp_dir, "dataverse", "vocab/csv", handle_name )
    
    if os.path.exists( csv_dir ):
        empty_dir( csv_dir )                # remove previous files
    
    for item in dataverse.get_contents():
        # item dict keys: protocol, authority, persistentUrl, identifier, type, id
        handle = str( item[ 'protocol' ] ) + ':' + str( item[ 'authority' ] ) + "/" + str( item[ 'identifier' ] )
        logging.debug( "handle: %s" % handle )
        clio_handle = configparser.get( "config", handle_name )
        logging.debug( "clio_handle: %s" % clio_handle )
        
        if handle == clio_handle:
            logging.info( "handle_name: %s, using handle: %s" % ( handle_name, handle ) )
            datasetid = item[ 'id' ]
            url  = "https://" + str( host ) + "/api/datasets/" + str( datasetid )
            #url += "/?&key=" + str( ristat_key )
            url += "?key=" + str( ristat_key )
            
            dataframe = loadjson( url )
            
            files = dataframe[ 'data' ][ 'latestVersion' ][ 'files' ]
            logging.info( "number of files: %d" % len( files ) )
            for dv_file in files:
                logging.debug( str( dv_file ) )
                datasetVersionId = str( dv_file[ "datasetVersionId" ] )
                version          = str( dv_file[ "version" ] )
                label            = str( dv_file[ "label" ] )
                datafile         =  dv_file[ "datafile" ]
                paperitem = {}
                paperitem[ 'id' ]   = str( datafile[ 'id' ] )
                originalFormatLabel = str( datafile[ 'originalFormatLabel' ] )
                name = str( datafile[ 'name' ] )
                basename, ext = os.path.splitext( name )
                logging.debug( "basename: %s, ext: %s, originalFormatLabel: %s" % ( basename, ext, originalFormatLabel ) )
                if dv_format == "original" and ext == ".tab" and originalFormatLabel == "MS Excel (XLSX)":
                    name = basename + ".xlsx"
                    logging.debug( "tab => xlsx: %s" % name )
                paperitem[ 'name' ] = name
                ids[ paperitem[ 'id'] ] = name
                paperitem[ 'handle' ] = handle
                paperitem[ 'url' ] = "http://data.sandbox.socialhistoryservices.org/service/download?id=%s" % paperitem[ 'id' ]
                url  = "https://%s/api/access/datafile/%s" % ( host, paperitem[ 'id' ] )
                url += "?&key=%s&show_entity_ids=true&q=authorName:*" % str( ristat_key )
                if not dv_format == "":
                    url += "&format=original"
                logging.debug( url )
                
                if copy_local:
                    if not os.path.exists( download_dir ):
                        os.makedirs( download_dir )
                    
                    filename = paperitem[ 'name' ]
                    filepath = "%s/%s" % ( download_dir, filename )
                    logging.debug( "filepath: %s" % filepath )
                    
                    # read dataverse document from url, write contents to filepath
                    filein = urllib.urlopen( url )
                    fileout = open( filepath, 'wb' )
                    fileout.write( filein.read() )
                    fileout.flush()
                    os.fsync( fileout )
                    fileout.close()
                    
                    if to_csv:
                        if not os.path.exists( csv_dir ):
                            os.makedirs( csv_dir )
                        
                        root, ext = os.path.splitext( filename )
                        logging.info( "%s %s" % ( handle_name, root ) )
                        csv_path  = "%s/%s.csv" % ( csv_dir, root )
                        logging.debug( "csv_path:  %s" % csv_path )
                        
                        #Xlsx2csv( filepath, **kwargs_xlsx2csv ).convert( csv_path )
                        data_xls = pd.read_excel( filepath, index_col = False )
                        data_xls.to_csv( csv_path, encoding = 'utf-8', index = False, **kwargs_pandas )
                        
                        if remove_xlsx and ext == ".xlsx":
                            os.remove( filepath )   # keep the csv, remove the xlsx
                
                # FL-09-Jan-2017 should we not filter before downloading?
                try:
                    if 'lang' in settings.datafilter:
                        varpat = r"(_%s)" % ( settings.datafilter[ 'lang' ] )
                        pattern = re.compile( varpat, re.IGNORECASE )
                        found = pattern.findall( paperitem[ 'name' ] )
                        if found:
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
    
    return ( papers, ids )



def update_documentation( configparser, copy_local, remove_xlsx = False ):
    logging.info( "%s update_documentation()" % __file__ )

    handle_name = "hdl_documentation"
    logging.info( "retrieving documents from dataverse for handle name %s ..." % handle_name )
    dst_dir = "doc"
    dv_format = ""
    ( docs, ids ) = documents_by_handle( configparser, handle_name, dst_dir, dv_format, copy_local, remove_xlsx )
    ndoc =  len( docs )
    logging.info( "%d documents retrieved from dataverse" % ndoc )
    if ndoc == 0:
        logging.info( "no documents, nothing to do." )
        return



def update_vocabularies( configparser, mongo_client, dv_format, copy_local = False, to_csv = False, remove_xlsx = False):
    logging.info( "%s update_vocabularies()" % __file__ )
    """
    update_vocabularies():
    -1- retrieves ERRHS_Vocabulary_*.tab files from dataverse
    -2- with copy_local=True stores them locally
    -3- stores the new data in MogoDB db = "vocabulary", collection = 'data'
    """
    
    handle_name = "hdl_vocabularies"
    logging.info( "retrieving documents from dataverse for handle name %s ..." % handle_name )
    
    if dv_format == "original":
        dst_dir   = "vocab/xlsx"
        csv_dir   = "vocab/csv"
        ascii_dir = csv_dir
    else:
        dst_dir   = "vocab/tab"
        ascii_dir = dst_dir
    
    ( docs, ids ) = documents_by_handle( configparser, handle_name, dst_dir, dv_format, copy_local, to_csv, remove_xlsx )
    ndoc =  len( docs )
    logging.info( "%d documents retrieved from dataverse" % ndoc )
    if ndoc == 0:
        logging.info( "no documents, nothing to do." )
        return
    
    logging.debug( "keys in ids:" )
    for key in ids:
        logging.debug( "key: %s, value: %s" % ( key, ids[ key ] ) )
        
    logging.debug( "docs:" )
    for doc in docs:
        logging.debug( doc )
    
    # parameters to retrieve the vocabulary files
    host   = configparser.get( "config", "dataverse_root" )
    apikey = configparser.get( "config", "ristatkey" )
    dbname = configparser.get( "config", "vocabulary" )
    logging.debug( "host:   %s" % host )
    logging.debug( "apikey: %s" % apikey )
    logging.debug( "dbname: %s" % dbname )
    
    vocab_json = [ {} ]
    # the vocabulary files may already have been downloadeded by documents_by_handle();
    # with ".tab" extension vocabulary() retrieves them again from dataverse, 
    # with ".csv" extension vocabulary() retrieves them locally, 
    # and --together with some filtering-- 
    # appends them to a bigvocabulary
    tmp_dir = configparser.get( "config", "tmppath" )
    abs_ascii_dir = os.path.join( tmp_dir, "dataverse", ascii_dir, handle_name )
    bigvocabulary = vocabulary( host, apikey, ids, abs_ascii_dir )    # type: <class 'pandas.core.frame.DataFrame'>
    #print bigvocabulary.to_json( orient = 'records' )
    vocab_json = json.loads( bigvocabulary.to_json( orient = 'records' ) )  # type: <type 'list'>
    
    """
    vocab_json0 = vocab_json[ 0 ]
    for key in vocab_json0:
        logging.info( "key: %s, value: %s" % ( key, vocab_json0[ key ] ) )
    
    # there are 7 keys per dictionary entry, e.g vocab_json0:
    key: basisyear,  value: None
    key: EN,         value: Male
    key: vocabulary, value: ERRHS_Vocabulary_modclasses
    key: DATATYPE,   value: None
    key: YEAR,       value: None
    key: RUS,        value: мужчины
    key: ID,         value: MOD_1.01_1
    """
    
    logging.info( "processing %d vocabulary items..." % len( vocab_json ) )
    for item in vocab_json:
        #logging.debug( item )
        if 'YEAR' in item:
            item[ 'YEAR' ] = re.sub( r'\.0', '', str( item[ 'YEAR' ] ) )
        if 'basisyear' in item:
            item[ 'basisyear' ] = re.sub( r'\.0', '', str( item[ 'basisyear' ] ) )
    
    dbname_vocab = configparser.get( "config", "vocabulary" )
    db_vocab = mongo_client.get_database( dbname_vocab )
    logging.info( "inserting vocabulary in mongodb '%s'" % dbname_vocab )
    result = db_vocab.data.insert( vocab_json )



def retrieve_handle_docs( configparser, handle_name, dv_format = "", copy_local = False, to_csv = False, remove_xlsx = False ):
    logging.info( "" )
    logging.info( "retrieve_handle_docs() copy_local: %s" % copy_local )

    logging.info( "retrieving documents from dataverse for handle name %s ..." % handle_name )
    dst_dir = "xlsx"
    ( docs, ids ) = documents_by_handle( configparser, handle_name, dst_dir, dv_format, copy_local, to_csv, remove_xlsx )
    ndoc =  len( docs )
    if ndoc == 0:
        logging.info( "no documents retrieved." )
        return
    else:
        logging.info( "%d documents for handle %s retrieved from dataverse" % ( ndoc, handle_name ) )
    
    logging.debug( "keys in ids:" )
    for key in ids:
        logging.debug( "key: %s, value: %s" % ( key, ids[ key ] ) )
        
    logging.debug( "docs:" )
    for doc in docs:
        logging.debug( doc )



def row_count( configparser ):
    logging.debug( "row_count()" )

    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    logging.debug( "using configparser: %s" % configpath )

    configparser = ConfigParser.RawConfigParser()
    configparser.read( configpath )
    
    host     = configparser.get( 'config', 'dbhost' )
    dbname   = configparser.get( 'config', 'dbname' )
    dbtable  = configparser.get( 'config', 'dbtable' )
    user     = configparser.get( 'config', 'dblogin' )
    password = configparser.get( 'config', 'dbpassword' )
    
    connection_string = "host = '%s' dbname = '%s' user = '%s' password = '%s'" % ( host, dbname, user, password )
    logging.debug( "connection_string: %s" % connection_string )
    connection = psycopg2.connect( connection_string )
    
    cursor = connection.cursor()
    sql = "SELECT COUNT(*) FROM %s;" % dbtable
    logging.info( sql )
    cursor.execute( sql )
    data = cursor.fetchall()
    count = data[0][0]
    logging.info( "row count: %d" % count )
    
    connection.commit()
    cursor.close()
    connection.close()



def clear_postgres( configparser ):
    logging.info( "clear_postgres()" )

    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    logging.info( "using configparser: %s" % configpath )

    configparser = ConfigParser.RawConfigParser()
    configparser.read( configpath )
    
    host     = configparser.get( 'config', 'dbhost' )
    dbname   = configparser.get( 'config', 'dbname' )
    dbtable  = configparser.get( 'config', 'dbtable' )
    user     = configparser.get( 'config', 'dblogin' )
    password = configparser.get( 'config', 'dbpassword' )
    
    connection_string = "host = '%s' dbname = '%s' user = '%s' password = '%s'" % ( host, dbname, user, password )
    logging.info( "connection_string: %s" % connection_string )

    connection = psycopg2.connect( connection_string )
    cursor = connection.cursor()

    sql = "TRUNCATE TABLE %s;" % dbtable
    logging.info( sql )
    cursor.execute( sql )
    
    connection.commit()
    cursor.close()
    connection.close()



def store_handle_docs( configparser, handle_name ):
    logging.info( "" )
    logging.info( "store_handle_docs() %s" % handle_name )
    
    tmp_dir = configparser.get( "config", "tmppath" )
    csv_dir  = os.path.join( tmp_dir, "dataverse", "csv", handle_name )
    dir_list = []
    if os.path.isdir( csv_dir ):
        dir_list = os.listdir( csv_dir )
    
    logging.info( "using csv directory: %s" % csv_dir )

    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        print( "in %s" % __file__ )
        print( "configpath %s FILE DOES NOT EXIST" % configpath )
        print( "EXIT" )
        sys.exit( 1 )
    
    logging.info( "using configparser: %s" % configpath )

    configparser = ConfigParser.RawConfigParser()
    configparser.read( configpath )
    
    host     = configparser.get( 'config', 'dbhost' )
    dbname   = configparser.get( 'config', 'dbname' )
    dbtable  = configparser.get( 'config', 'dbtable' )
    user     = configparser.get( 'config', 'dblogin' )
    password = configparser.get( 'config', 'dbpassword' )
    
    connection_string = "host = '%s' dbname = '%s' user = '%s' password = '%s'" % ( host, dbname, user, password )
    logging.info( "connection_string: %s" % connection_string )

    connection = psycopg2.connect( connection_string )
    cursor = connection.cursor()

    for filename in dir_list:
        root, ext = os.path.splitext( filename )
        if root.startswith( "ERRHS_" ) and ext == ".csv":
            logging.info( "use: %s, to table: %s" % ( filename, dbtable ) )
            in_pathname = os.path.abspath( os.path.join( csv_dir, filename ) )
            logging.debug( in_pathname )
            #test_csv_file( pathname )
            
            #out_pathname = write_psv_file( csv_dir, filename )
            #psv_file = open( out_pathname, 'r' )
            #cursor.copy_from( psv_file, dbtable, sep ='|' )
            
            stringio_file = filter_csv( csv_dir, filename )
            cursor.copy_from( stringio_file, dbtable, sep = '|' )
            #cursor.copy_from( stringio_file, dbtable, sep = '|', null = "None" )
            
            #csv_strings.close()  # close object and discard memory buffer
            #csvfile.close()
            
            # debug strange record duplications
            connection.commit()
            row_count( configparser )
            
        else:
            logging.info( "skip: %s" % filename )

        #print( "break" )
        #break
    
    ndoc = len( dir_list )
    logging.info( "%d documents for handle %s stored in table %s" % ( ndoc, handle_name, dbtable ) )
    
    connection.commit()
    cursor.close()
    connection.close()



def test_csv_file( path_name ):
    csv_file = open( path_name, 'r' )
    nlines = 0
    
    for line in csv_file:
        cnt = line.count( '|' )
        fields = line.split( '|' )
        nfields = len( fields )
        print( "%d: %d" % ( nline, nfields ) )
        nlines += 1
        
    print( "%d" % nlines )



def filter_csv( csv_dir, in_filename ):
    global pkey
    logging.debug( "filter_csv()" )
    
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
    
    # some extra columns in postgres table
    pg_column_names = list( dv_column_names )
    # FL-13-Mar-2017: dropped column "indicator_id"
    #pg_column_names.insert( 0, "indicator_id" ) # pre-pended for postgres table
    pg_column_names.append( "indicator" )       # appended for for postgres table
    pg_column_names.append( "valuemark" )       # appended for for postgres table
    pg_column_names.append( "timestamp" )       # appended for for postgres table
    # FL-13-Mar-2017: added primary key (after "timestamp")
    pg_column_names.append( "pk" )              # appended for for postgres table
    
    ncolumns_dv = len( dv_column_names )
    logging.debug( "# of dv_columns: %d" % ncolumns_dv )
    
    in_pathname = os.path.abspath( os.path.join( csv_dir, in_filename ) )
    csv_file = open( in_pathname, 'r')
    
    """
    root, ext = os.path.splitext( in_filename )
    out_pathname = os.path.abspath( os.path.join( csv_dir, root + ".psv" ) )
    print( out_pathname )
    out_file = open( out_pathname, 'w' )
    """
    
    out_file = StringIO.StringIO()      # in-memory file
    
    nline = 0
    nskipped = 0
    comment_length_max = 0
    
    for line in csv_file:
        nline += 1
        #logging.info( "line %d: %s" % ( nline, line ) )
        line = line.strip( '\n' )       # remove trailing \n
        #logging.info( "%d in: %s" % ( nline, line ) )
        #print( "# new lines: %d" % line.count( '\n' ) )
        
        if len( line ) == line.count( '|' ):    # only separators, no data
            nskipped += 1
            continue
        
        fields = line.split( '|' )
        if nline == 1:
            line_header = line
            nfields_header = len( fields )              # nfields of header line
            
            # need a list as result, otherwise error in comparison below: 
            # TypeError: 'itertools.imap' object has no attribute '__getitem__'
            csv_header_names = list( map( str.lower, fields ) )
            logging.debug( "# of csv header fields: %d" % nfields_header )
            ndiff = nfields_header - ncolumns_dv
            #logging.info( "ndiff: %d" % ndiff )
            
            # header names must match predefined dv_column_names
            # (but after base_year there may be several empty columns in the input)
            skip_file = False
            if len( dv_column_names ) > len( csv_header_names ):
                msg = "skipping bad file %s" % in_filename
                logging.warning( msg ); print( msg )
                msg = "wrong header structure: \n%s" % line_header
                logging.warning( msg ); print( msg )
                skip_file = True
                break
            for i in range( ncolumns_dv ):
                if dv_column_names[ i ] != csv_header_names[ i ]:
                    msg = "skipping bad file %s" % in_filename
                    logging.warning( msg ); print( msg )
                    msg = "wrong header structure: \n%s" % line_header
                    logging.warning( msg ); print( msg )
                    skip_file = True
                    break
            if skip_file:
                break
                
            continue        # do not store header line
        else:
            # Keep the bridging '.' but replace trailing '.' by ". " (add a space)
            # So the (unordered) '. ' are easily recognizable in the RiStat requests
            nfields = len( fields )
            if nfields != nfields_header:
                msg = "skipping bad data line # %d" % nline
                logging.warning( msg ); print( msg )
                msg = "# of data fields (%d) does not match # of header fields (%d)" % ( nfields, nfields_header )
                logging.warning( msg ); print( msg )
                msg = "header: %s" % line_header
                logging.warning( msg ); print( msg )
                msg = "data: %s" % line
                logging.warning( msg ); print( msg )
                continue
            
            nzaphc = 0
            for i in reversed( range( nfields ) ):      # histclass fields
                #print( "%2d %s: %s" % ( i, csv_header_names[ i ], fields[ i ] ) )
                if csv_header_names[ i ].startswith( "histclass" ):     # historical
                    if fields[ i ] == ".":
                        fields[ i ] = ". "
                        nzaphc += 1
                    else:
                        break
            
            nzapc = 0
            for i in reversed( range( nfields ) ):  # class fields
                #print( "%2d %s: %s" % ( i, csv_header_names[ i ], fields[ i ] ) )
                if csv_header_names[ i ].startswith( "class" ):         # modern
                    if fields[ i ] == ".":
                        fields[ i ] = ". "
                        nzapc += 1
                    else:
                        break
            
            """
            # replace the value dot fields by an empty string, 
            # because the dots hamper COUNT, and we want them in the response.
            for i in range( nfields ):
                #print( "%2d %s: %s" % ( i, csv_header_names[ i ], fields[ i ] ) )
                if csv_header_names[ i ] == "value" :
                    if fields[ i ] == ".":
                        fields[ i ] = "None"
            """
            
            # check comment_source length
            comment_pos = csv_header_names.index( "comment_source" )
            comment = fields[ comment_pos ]
            comment_length = len( comment )
            comment_length_max = max( comment_length_max, comment_length )
            if comment_length > COMMENT_LENGTH_MAX_DB:
                fields[ comment_pos ] = ""      # because it is unicode we cannot just chop it
                msg = "too long comment in line:"
                logging.warning( msg );  print( msg )
                logging.warning( line ); print( line )
            
            # check missing datatype
            datatype_pos = csv_header_names.index( "datatype" )
            datatype = fields[ datatype_pos ]
            if len( datatype ) == 0 or datatype == '.':
                msg = "missing datatype in line:"
                logging.warning( msg );  print( msg )
                logging.warning( line ); print( line )
            else:   # chop spurious decimals of stupid spreadsheets
                fields[ datatype_pos ] = "%4.2f" % float( datatype )
            
        #print( "|".join( fields ) )
        if ndiff > 0:
            npop = ndiff
            for _ in range( npop ):
                fields.pop()            # skip trailing n fields
            #logging.debug( "# of fields popped: %d" % npop )
        elif ndiff < 0:
            napp = abs( ndiff ) - 1
            for _ in range( napp ):
                fields.append( "" )
            #logging.debug( "# of fields added: %d" % napp )
        #print( "# of fields: %d" % len( fields ) )
        
        # base_year, must be integer
        base_year_idx = None
        try:
            base_year_idx = csv_header_names.index( "base_year" )    # 38, 37, ...?
            try:
                dummy = int( fields[ base_year_idx ] )
            except:
                try:
                    fields[ base_year_idx ] = "0"
                except:
                    logging.info( "%d: in: %s" % ( nline, line ) )
                    logging.info( "%d out: %s" % ( nline, "|".join( fields ) ) )
                    print( "EXIT" )
                    sys.exit( 0 )
        except ValueError:
            pass
        
        #print( 1, "|".join( fields ) )
        fields.append( "0" )                # indicator field, not in csv file
        #print( 2, "|".join( fields ) )
        fields.append( "false" )            # valuemark field, not in csv file
        #print( 3, "|".join( fields ) )
        
        """
        # valuemark must be true or false, but is not in the input data
        valuemark_idx = base_year_idx + 2
        if fields[ valuemark_idx ] not in ( "true", "false" ):
            fields[ valuemark_idx ] = "false"
        """
        
        fields.append( "now()" )            # timestamp field, not in csv file
        
        # column indicator_id should become the primairy key
        # FL-13-Mar-2017: added primary key after "timestamp"
        if pkey is None: 
            pkey = 1
        fields.append( str( pkey ) )        # global
        pkey += 1
        
        # FL-13-Mar-2017: dropped column "indicator_id"
        #fields.insert( 0, "0" )             # prepend indicator_id field, not in csv file
        
        #print( "|".join( fields ) )
        
        table_line = "|".join( fields )
        logging.debug( "fields in table record: %d" % len( fields ))
        """
        if nline == 2:
            print( "%d fields" % len( fields ) )
            print( short_line )
        
            for i in range( len( fields ) ):
                print( "%2d %s: %s" % ( i, dv_column_names[ i ], fields[ i ] ) )
        """
        #logging.info( "%d: in: %s" % ( nline, line ) )
        #logging.info( "%d out: %s" % ( nline, table_line ) )
        
        out_file.write( "%s\n" % table_line )
    
    out_file.seek( 0)   # start of the stream
    #out_file.close()    # closed by caller!: closing discards memory buffer
    csv_file.close()
    
    logging.info( "lines written to csv file: %d (including header line)" % (nline - nskipped) )
    
    if comment_length_max > COMMENT_LENGTH_MAX_DB:
        logging.info( "WARNING: comment_length_max: %d, length available %d" % ( comment_length_max, COMMENT_LENGTH_MAX_DB ) )
    
    if nskipped != 0:
        logging.info( "empty lines (|-only) skipped: %d" % nskipped )
    
    #return out_pathname
    return out_file



def update_handle_docs( configparser, mongo_client ):
    logging.info( "" )
    logging.info( "update_handle_docs()" )
    
    configpath = RUSSIANREPO_CONFIG_PATH
    logging.info( "using configparser: %s" % configpath )
    # classupdate() uses postgresql access parameters from cpath contents
    classdata = classupdate( configpath )   # fetching historic and modern class data from postgresql table 
    
    dbname = configparser.get( "config", "vocabulary" )
    logging.info( "inserting historic and modern class data in mongodb '%s'" % dbname )
    
    db = mongo_client.get_database( dbname )
    result = db.data.insert( classdata )



def clear_mongo( mongo_client ):
    logging.info( "clear_mongo()" )
    
    dbname_vocab = configparser.get( "config", "vocabulary" )
    db_vocab = mongo_client.get_database( dbname_vocab )
    logging.info( "delete all documents from collection 'data' in mongodb db '%s'" % dbname_vocab )
    # drop the documents from collection 'data'; same as: db.drop_collection( coll_name )
    db_vocab.data.drop()
    
    logging.info( "clearing mongodb cache" )
    db_cache = mongo_client.get_database( 'datacache' )
    db_cache.data.drop()      # remove the collection 'data'



def topic_counts( configparser ):
    logging.info( "topic_counts()" )
    
    configpath = RUSSIANREPO_CONFIG_PATH
    if not os.path.isfile( configpath ):
        logging.error( "in %s" % __file__ )
        logging.error( "configpath %s FILE DOES NOT EXIST" % configpath )
        logging.error( "EXIT" )
        sys.exit( 1 )
    
    logging.info( "using configparser: %s" % configpath )

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
    
    main_topics = [ "1", "2", "3", "4", "5", "6", "7" ]
    for record in resp:
        datatype   = record.datatype
        topic_name = record.topic_name
        if datatype not in main_topics:
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
            logging.info( "datatype: %s , topic_name: %s, counts: %s" % ( datatype, topic_name, str( cnt_dict ) ) )
        else:
            #print( "skip main topic :", datatype, topic_name )
            pass
    
    connection.commit()
    cursor.close()
    connection.close()



def load_vocab( vocab_fname, vocab, pos_rus, pos_eng ):
    # if pos_extar is not None, it is needed to make the keys and/or values unique
    handle_name = "hdl_vocabularies"
    tmp_dir = configparser.get( "config", "tmppath" )
    vocab_dir = os.path.join( tmp_dir, "dataverse", "vocab/csv", handle_name )
    logging.info( "vocab_dir: %s" % vocab_dir )
    vocab_path = os.path.join( vocab_dir, vocab_fname )
    logging.info( "vocab_path: %s" % vocab_path )
    vocab_file = open( vocab_path, "r" )
    
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
            
            if vocab_fname == "ERRHS_Vocabulary_units.csv":
                logging.debug( "rus: %s, eng: %s" % ( rus, eng ) )
                rus_ = rus
                eng_ = eng
            
            elif vocab_fname == "ERRHS_Vocabulary_regions.csv":
                terr = parts[ 0 ].strip()
                rus_ = str( { "rus" : rus, "terr" : terr } )
                eng_ = str( { "eng" : eng, "terr" : terr } )
            
            elif vocab_fname == "ERRHS_Vocabulary_histclasses.csv":
                byear = parts[ 2 ].strip()
                dtype = parts[ 3 ].strip()
                rus_ = str( { "rus" : rus, "byear" : byear, "dtype" : dtype } )
                eng_ = str( { "eng" : eng, "byear" : byear, "dtype" : dtype } )
            
            elif vocab_fname == "ERRHS_Vocabulary_modclasses.csv":
                dtype = parts[ 2 ][ 4: ].strip()
                rus_ = str( { "rus" : rus, "dtype " : dtype  } )
                eng_ = str( { "eng" : eng, "dtype " : dtype  } )
            
            logging.debug( "rus_: %s, eng_: %s" % ( rus_, eng_ ) )
            
            try:
                vocab[ rus_ ] = eng_
            except:
                type, value, tb = sys.exc_info()
                logging.error( "EXCEPTION %s" % value )
                
        nline += 1
    
    vocab_file.close()

    return vocab



def translate_csv( configparser, handle_name ):
    logging.info( "" )
    logging.info( "translate_csv()" )
    logging.info( "translating csv documents for handle name %s ..." % handle_name )
    
    vocab_units = bidict()
    vocab_units = load_vocab( "ERRHS_Vocabulary_units.csv", vocab_units, 0, 1 )
    
    vocab_regions = bidict()
    vocab_regions = load_vocab( "ERRHS_Vocabulary_regions.csv", vocab_regions, 1, 2 )
    
    vocab_histclasses = bidict()
    vocab_histclasses = load_vocab( "ERRHS_Vocabulary_histclasses.csv", vocab_histclasses, 0, 1 )
    
    vocab_modclasses = bidict()
    vocab_modclasses = load_vocab( "ERRHS_Vocabulary_modclasses.csv", vocab_modclasses, 0, 1 )
    
    tmp_dir = configparser.get( "config", "tmppath" )
    csv_dir = os.path.join( tmp_dir, "dataverse", "csv", handle_name )
    logging.info( "csv_dir: %s" % csv_dir )
    
    eng_dir = os.path.join( tmp_dir, "dataverse", "csv-en", handle_name )
    logging.info( "eng_dir: %s" % eng_dir )
    
    if os.path.exists( eng_dir ):
        empty_dir( eng_dir )                # remove previous files
    if not os.path.exists( eng_dir ):
        os.makedirs( eng_dir )
    
    regions     = [ "TERRITORY" ]
    units       = [ "VALUE_UNIT" ]
    histclasses = [ "HISTCLASS1", "HISTCLASS2", "HISTCLASS3", "HISTCLASS4", "HISTCLASS5",  "HISTCLASS6", "HISTCLASS7", "HISTCLASS8", "HISTCLASS9", "HISTCLASS10", ]
    modclasses  = [ "CLASS1", "CLASS2", "CLASS3", "CLASS4", "CLASS5",  "CLASS6", "CLASS7", "CLASS8", "CLASS9", "CLASS10", ]
    translate_list = regions + units + histclasses + modclasses
    
    # read russian csv files
    dir_list = os.listdir( csv_dir )
    dir_list.sort()
    for csv_name in dir_list:
        #logging.info( csv_name )
        csv_path = os.path.join( csv_dir, csv_name )
        if not os.path.isfile( csv_path ):
            continue
        
        logging.info( csv_path )
        basename, ext = os.path.splitext( csv_name )
        eng_name = basename + "-en" +  ext
        #logging.info( eng_name )
        eng_path = os.path.join( eng_dir, eng_name )
        logging.info( eng_path )
        
        file_rus = open( csv_path, "r" )
        file_eng = open( eng_path, "w" )
        
        nline = 0
        for csv_line in iter( file_rus ):
            logging.debug( csv_line )
            eng_cols = []
            if nline == 0:
                header = csv_line.split( '|' )
                file_eng.write( csv_line )      # unchanged
            else:
                rus_cols = csv_line.split( '|' )
                ncolumns = len( rus_cols )
                for c in range( ncolumns ):
                    name = header[ c ]
                    rus_str = rus_cols[ c ]
                    
                    if name in units:
                        eng_str = vocab_units[ rus_str ]
                    elif name in regions:
                        eng_str = vocab_regions.get( rus_str )
                    elif name in histclasses:
                        eng_str = vocab_histclasses.get( rus_str )
                    elif name in modclasses:
                        eng_str = vocab_modclasses.get( rus_str )
                    else:
                        eng_str = rus_str
                    
                    if eng_str is None:
                        logging.debug( "not found: %s" % rus_str )
                        eng_str = "None"
                    
                    eng_cols.append( eng_str )
                
                eng_line = '|'.join( eng_cols )
                logging.debug( eng_line )
                file_eng.write( eng_line )
                
            nline += 1
        
        file_rus.close()
        file_eng.close()
        
        break



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



if __name__ == "__main__":
    DO_DOCUMENTATION = False     # documentation: dataverse  => local_disk
    DO_VOCABULARY    = False     # vocabulary: dataverse  => mongodb
    DO_RETRIEVE      = False     # ERRHS data: dataverse  => local_disk, xlsx -> csv
    DO_POSTGRES      = False     # ERRHS data: local_disk => postgresql, csv -> table
    DO_MONGODB       = False     # ERRHS data: postgresql => mongodb
    DO_TRANSLATE     = True     # translate Russian csv files to English variants
    
    #dv_format = ""
    dv_format = "original"  # does not work for ter_code (regions) vocab translations
    
    log_file = True
    
    log_level = logging.DEBUG
    #log_level = logging.INFO
    #log_level = logging.WARNING
    #log_level = logging.ERROR
    #log_level = logging.CRITICAL
    
    if log_file:
        mode = 'w'
        #mode = 'a'      # debugging
        logging_filename = "autoupdate.log"
        logging.basicConfig( filename = logging_filename, filemode = mode, level = log_level )
    else:
        logging.basicConfig( level = log_level )

    time0 = time()      # seconds since the epoch
    logging.info( "start: %s" % datetime.datetime.now() )
    logging.info( "user: %s" % getpass.getuser() )
    logging.info( __file__ )
    
    python_vertuple = sys.version_info
    python_version = str( python_vertuple[ 0 ] ) + '.' + str( python_vertuple[ 1 ] ) + '.' + str( python_vertuple[ 2 ] )
    logging.info( "Python version: %s" % python_version  )
    
    RUSSIANREPO_CONFIG_PATH = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]
    logging.info( "RUSSIANREPO_CONFIG_PATH: %s" % RUSSIANREPO_CONFIG_PATH )
    
    configparser = ConfigParser.RawConfigParser()
    configparser.read( RUSSIANREPO_CONFIG_PATH )
    mongo_client  = MongoClient()
    
    if DO_VOCABULARY or DO_MONGODB:
        clear_mongo( mongo_client )
    
    if DO_DOCUMENTATION:
        copy_local  = True      # for ziped downloads
        update_documentation( configparser, copy_local )
    
    if DO_VOCABULARY:
        # Downloaded vocabulary documents are not used to update the vocabularies, 
        # they are processed on the fly, and put in MongoDB
        copy_local = True      # to inspect
        if dv_format == "":
            to_csv = False      # we get .tab
        else:
            to_csv = True       # we get .xlsx
        update_vocabularies( configparser, mongo_client, dv_format, copy_local, to_csv )
    """
    handle_names = [ 
        "hdl_errhs_population",     # ERRHS_1   39 files
        "hdl_errhs_capital",        # 
        "hdl_errhs_industry",       # 
        "hdl_errhs_agriculture",    # ERRHS_4   10 files
        "hdl_errhs_labour",         # 
        "hdl_errhs_services",       # 
        "hdl_errhs_land"            # ERRHS_7   10 files
    ]
    """
    handle_names = [ "hdl_errhs_land" ]
    
    if DO_RETRIEVE:
        copy_local  = True
        to_csv      = True
        remove_xlsx = False
        for handle_name in handle_names:
            retrieve_handle_docs( configparser, handle_name, dv_format, copy_local, to_csv, remove_xlsx ) # dataverse  => local_disk
    
    if DO_POSTGRES:
        logging.StreamHandler().flush()
        row_count( configparser )
        clear_postgres( configparser )
        row_count( configparser )
        for handle_name in handle_names:
            store_handle_docs( configparser, handle_name )         # local_disk => postgresql
            logging.StreamHandler().flush()
            row_count( configparser )
    
        # done on-the-fly in services/topic_counts()
        #topic_counts( configparser )                               # postgresql datasets.topics counts
    
    if DO_MONGODB:
        update_handle_docs( configparser, mongo_client )           # postgresql => mongodb
    
    if DO_TRANSLATE:
        for handle_name in handle_names:
            translate_csv( configparser, handle_name )
    
    logging.info( "stop: %s" % datetime.datetime.now() )
    str_elapsed = format_secs( time() - time0 )
    logging.info( "processing took %s" % str_elapsed )
    
    # This should be called at application exit,
    # and no further use of the logging system should be made after this call.
    logging.shutdown()
    
# [eof]
