#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
VT-06-Jul-2016 latest change by VT
FL-09-Jan-2018 language parameter
FL-14-May-2019 pd.concat sort = True
"""

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

from six.moves import configparser, StringIO

import logging
import os
import pandas as pd
import psycopg2
import re
import urllib


def vocabulary( host, apikey, ids, abs_ascii_dir ):
    logging.info( "%s vocabulary()" % __file__ )
    logging.info( "vocabulary() abs_ascii_dir: %s" % abs_ascii_dir )
    
    lexicon = []
    len_totvocab = 0
    
    for thisid in ids:
        filename = ids[ thisid ]
        logging.info( "vocabulary() %s" % filename )
        basename, ext = os.path.splitext( filename )
        
        if ext == ".tab":
            filename = re.sub( '.tab', '', filename )
            url = "%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % ( host, thisid, apikey )
            f = urllib.urlopen( url )
            data = f.read()
            csvio = StringIO( str( data ) )
            dataframe = pd.read_csv( csvio, sep = '\t', dtype = 'unicode' )
        elif ext == ".xlsx":
            filename = basename + ".csv"
            pathname = os.path.join( abs_ascii_dir, filename )
            with open( pathname, 'r') as f:
                data = f.read()
                csvio = StringIO( str( data ) )
                dataframe = pd.read_csv( csvio, sep = '|', dtype = 'unicode' )
        
        # fetch columns from dataverse vocabulary file
        filter_columns = []
        mapping = {}
        for col in dataframe.columns:
            findval = re.search( r'RUS|EN|ID|TOPIC_ID|TOPIC_ROOT|DATATYPE|YEAR|basisyear', col )
            if findval:
                mapping[ col ] = findval.group( 0 )
                filter_columns.append( col )
        
        vocab = {}
        if filter_columns:
            logging.info( "filter_columns: %s" % filter_columns )
            vocab = dataframe[ filter_columns ]
            newcolumns = []
            for field in vocab:
                value = mapping[ field ]
                newcolumns.append( value )
            
            vocab.columns = newcolumns
            vocab = vocab.dropna()
            vocab[ 'vocabulary' ] = basename
            len_vocab = len( vocab )
            len_totvocab += len_vocab
            lexicon.append( vocab )
            
            logging.info( "id: %s, filename: %s, items: %d" % ( thisid, filename, len_vocab ) )
        else:
            logging.warning( "No filter_columns" )
    
    logging.info( "lexicon contains %d vocabularies containing %d items in total" % ( len( lexicon ), len_totvocab ) )
    # concatenate the vocabularies with pandas
    return pd.concat( lexicon, sort = True )



def classupdate( cpath, language ):
    logging.info( "%s classupdate()" % __file__ )
    config_parser = configparser.RawConfigParser()
    config_parser.read( cpath )
    
    dbtable_name = "dbtable" + '_' + language
    dbtable  = config_parser.get( "config", dbtable_name )
    dbhost   = config_parser.get( "config", "dbhost" )
    dbname   = config_parser.get( "config", "dbname" )
    user     = config_parser.get( "config", "dblogin" )
    password = config_parser.get( "config", "dbpassword" )
    
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % ( dbhost, dbname, user, password )
    logging.debug( "conn_string: %s" % conn_string )

    conn = psycopg2.connect( conn_string )
    cursor = conn.cursor()
    
    finalclasses = []
    
    # historic data
    logging.info( "fetching historic data from postgresql table %s..." % dbtable )
    historic_columns = "base_year, value_unit, value_label, datatype, histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10"
    sql = "SELECT DISTINCT %s FROM %s" % ( historic_columns, dbtable )
    logging.info( "sql: %s" % sql )

    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [ desc[ 0 ] for desc in cursor.description ]     # list of the selected column names
    
    if data:
        no_dtype = 0
        for valuestr in data:
            classes = {}
            for i in range( len( valuestr ) ):
                name  = sqlnames[ i ]
                value = valuestr[ i ]
                #logging.debug( "name: %s, value: %s" % ( name, value ) )
                if value:
                    try:
                        classes[ name ] = str( value )
                    except:
                        #print( __file__, "historic", value )
                        classes[ name ] = value

            flagvalue  = 0
            firstclass = 0
            for n in range( 10, 1, -1 ):
                name = "histclass%s" % n
                if name in classes:
                    if classes[ name ]:
                        flagvalue = 1
                        if not firstclass:
                            firstclass = n

                    if flagvalue == 0:
                        del classes[ name ]

            # Check comma and add between classes
            for n in range( 1,firstclass ):
                name = "histclass%s" % n
                if name not in classes:
                    classes[ name ] = '.'

            classes[ 'vocabulary' ] = 'historical'
            if 'datatype' in classes:
                finalclasses.append( classes )
            else:
                no_dtype += 1
        
        logging.info( "numbers of historic class records: %d, skipping %d without datatype" % ( len( data ), no_dtype ) )
    
    # modern data
    logging.info( "fetching modern data from postgresql table %s..." % dbtable )
    modern_columns = "base_year, value_unit, value_label, datatype, class1, class2, class3, class4, class5, class6, class7, class8, class9, class10"
    sql = "SELECT DISTINCT %s FROM %s" % ( modern_columns, dbtable )
    logging.info( "sql: %s" % sql )
    
    cursor.execute( sql )
    data = cursor.fetchall()
    sqlnames = [ desc[ 0 ] for desc in cursor.description ]     # list of the selected column names
    
    if data:
        no_dtype = 0
        for valuestr in data:
            classes = {}
            for i in range( len( valuestr ) ):
                name  = sqlnames[ i ]
                value = valuestr[ i ]
                if value:
                    try:
                        classes[ name ] = str( value )
                    except:
                        #print( __file__, "modern", value )
                        classes[ name ] = value

            flagvalue  = 0
            firstclass = 0
            for n in range( 10, 1, -1 ):
                name = "class%s" % n
                if name in classes:
                    if classes[ name ]:
                        flagvalue = 1
                        if not firstclass:
                            firstclass = n
                    if flagvalue == 0:
                        del classes[ name ]

            # Check comma and add between classes
            for n in range( 1, firstclass ):
                name = "class%s" % n
                if name not in classes:
                    classes[ name ] = '.'

            classes[ 'vocabulary' ] = 'modern'
            if 'datatype' in classes:
                finalclasses.append( classes )
            else:
                no_dtype += 1
        
        logging.info( "numbers of modern class records: %d, skipping %d without datatype" % ( len( data ), no_dtype ) )
        
    logging.info( "total number of class records: %d" % len( finalclasses ) )
    
    return finalclasses

# [eof]
