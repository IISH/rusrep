#!/usr/bin/python
# -*- coding: utf-8 -*-

# VT-07-Jul-2016 latest change by VT
# FL-28-Mar-2017 

import json
import logging
import openpyxl
import re

from icu import Locale, Collator
from openpyxl.cell import get_column_letter
from pymongo import MongoClient


def preprocessor( datafilter ):
    logging.debug( "preprocessor()" )
     
    dataset = []
    lexicon = {}
    lands   = {}
    year    = 0
    lang    = 'en'

    key = datafilter[ "key" ]
    if key:
        clientcache = MongoClient()
        dbcache = clientcache.get_database( 'datacache' )
        result = dbcache.data.find( { "key": key } )
        
        ter_codes = []      # actually used region codes
        
        for rowitem in result:
            logging.debug( "rowitem: " + str( rowitem ) )
            
            del rowitem[ 'key' ]
            del rowitem[ '_id' ]
            
            if 'language' in rowitem:
                lang = rowitem[ 'language' ]
                del rowitem['language']
            
            for item in rowitem[ 'data' ]:
                dataitem = item
                if 'path' in item:
                    classes = item[ 'path' ]
                    del item[ 'path' ]
                    clist = {}
                    for classname in classes:
                        dataitem[ classname] = classes[ classname ]
                if 'year' in item:
                    year = item[ 'year' ]
                if 'base_year' in item:
                    year = item[ 'base_year' ]
                
                itemlexicon = dataitem
                lands = {}
                if 'ter_code' in itemlexicon:
                    ter_code = itemlexicon[ 'ter_code' ]
                    ter_codes.append( ter_code )
                    lands[ itemlexicon[ 'ter_code' ] ] = itemlexicon[ 'total' ]
                    del itemlexicon[ 'ter_code' ]
                if 'total' in itemlexicon:
                    del itemlexicon[ 'total' ]
                
                lexkey = json.dumps( itemlexicon )
                itemlexicon[ 'lands' ] = lands
                if lexkey in lexicon:
                    currentlands = lexicon[ lexkey ]
                    for item in lands:
                        currentlands[ item ] = lands[ item ]
                    lexicon[ lexkey ] = currentlands
                else:
                    lexicon[ lexkey ] = lands
                
                dataset.append( dataitem )
        
        db = clientcache.get_database( 'vocabulary' )   # vocabulary
        
        # load regions
        regions_filter = {}
        regions_filter[ "vocabulary" ] = "ERRHS_Vocabulary_regions"
        if year:
            regions_filter[ 'basisyear' ] = str( year )
        vocab_regions = db.data.find( regions_filter )
        regions = {}
        
        logging.debug( str( ter_codes ) )
        for item in vocab_regions:
            #logging.debug( str( item ) )
            ID = item[ 'ID' ]
            if ID in ter_codes:
                if lang == 'en':
                    regions[ item[ 'ID' ] ] = item[ 'EN' ]
                else:
                    regions[ item[ 'ID' ] ] = item[ 'RUS' ]
        
        vocabulary = {}
        vocabulary[ 'regions' ] = regions
        
        # create header
        header = []
        
        topic = ""
        key_comps = key.split( '-' )
        if len( key_comps ) >= 2:
            topic = key_comps[ 1 ]
        
        # load terms
        terms_needed  = [ "base_year", "count", "datatype", "value_unit" ]
        terms_needed += [ "class1", "class2", "class3", "class4", "class5", "class6", "class7", "class8", "class9", "class10" ]
        terms_needed += [ "histclass1", "histclass2", "histclass3", "histclass4", "histclass5", "histclass6", "histclass7", "histclass8", "histclass9", "histclass10" ]
        
        terms = {}
        vocab_download = db.data.find( { "vocabulary": "ERRHS_Vocabulary_download" } )
        topic_name = ""
        for item in vocab_download:
            #logging.debug( str( item ) )
            ID = item[ 'ID' ]
            #logging.debug( "%s %s" % ( topic, ID ) )
            if ID == topic:
                if lang == 'en':
                    topic_name = item[ 'EN' ]
                else:
                    topic_name = item[ 'RUS' ]
            
            if ID in terms_needed:
                if lang == 'en':
                    terms[ item[ 'ID' ] ] = item[ 'EN' ]
                else:
                    terms[ item[ 'ID' ] ] = item[ 'RUS' ]
        
        
        #logging.debug( "topic: %s, topic_name: %s" % ( topic, topic_name ) )
        vocabulary[ 'terms' ] = terms
        
        if lang == 'en':
            if key[ 0 ] == 'h':
                classification = "HISTORICAL"
            elif key[ 0 ] == 'm':
                classification = "MODERN"
            else:
                classification = ""
            
            header.append( { "r" : 1, "c" : 0, "value" : "Electronic repository of Russian Historical Statistics - ristat.org" } )
            header.append( { "r" : 3, "c" : 0, "value" : "TOPIC:" } )
            header.append( { "r" : 3, "c" : 1, "value" : topic_name } )
            header.append( { "r" : 4, "c" : 0, "value" : "BENCHMARK-YEAR:" } )
            header.append( { "r" : 4, "c" : 1, "value" : year } )
            header.append( { "r" : 5, "c" : 0, "value" : "CLASSIFICATION:" } )
            header.append( { "r" : 5, "c" : 1, "value" : classification } )
        else:
            if key[ 0 ] == 'h':
                classification = "ИСТОРИЧЕСКАЯ"
            elif key[ 0 ] == 'm':
                classification = "СОВРЕМЕННАЯ"
            else:
                classification = ""
            
            header.append( { "r" : 1, "c" : 0, "value" : "Электронный архив Российской исторической статистики - ristat.org" } )
            header.append( { "r" : 3, "c" : 0, "value" : "ТЕМА:" } )
            header.append( { "r" : 3, "c" : 1, "value" : topic_name } )
            header.append( { "r" : 4, "c" : 0, "value" : "ГОД:" } )
            header.append( { "r" : 4, "c" : 1, "value" : year } )
            header.append( { "r" : 5, "c" : 0, "value" : "КЛАССИФИКАЦИЯ:" } )
            header.append( { "r" : 5, "c" : 1, "value" : classification } )
    
    
    logging.debug( str( lexicon ) )
    logging.debug( str( vocabulary ) )
    logging.debug( str( header ) )
    
    return ( lexicon, vocabulary, header )



def aggregate_dataset( fullpath, result, vocab, header ):
    logging.debug( "aggregate_dataset()" )
    
    wb = openpyxl.Workbook( encoding = 'utf-8' )
    ws = wb.get_active_sheet()
    ws.title = "Dataset"

    i = 9
    myloc = Locale( 'ru' )  # 'el' is the locale code for Greek
    col = Collator.createInstance( myloc )
    regions = {}
    regnames = []
    
    for ter_code in vocab[ 'regions' ]:
        tmpname = vocab[ 'regions' ][ ter_code ]
        ter_name = tmpname.decode( 'utf-8' )
        regions[ ter_name ] = ter_code
        regnames.append( ter_name ) 

    sorted_regions = sorted( regnames, cmp = col.compare )

    for itemchain in result:
        j = 0
        if i == 9:
            #ws.column_dimensions[ "C" ].width = 80
            #ws.column_dimensions[ "D" ].width = 20
            #ws.column_dimensions[ "O" ].width = 100
            #ws.column_dimensions[ "P" ].width = 100
    
            chain = json.loads( itemchain )
            terdata = result[ itemchain ]
            
            for name in sorted( chain ):
                c = ws.cell( row = i, column = j )
                col_name = name
                if col_name in vocab[ 'terms' ]:
                    col_name = vocab[ 'terms' ][ col_name ]
                c.value = col_name
                logging.debug( "%d: %s" % ( j, col_name ) )
                j += 1
            
            for ter_name in sorted_regions:
                ter_code = regions[ ter_name ]
                c = ws.cell( row = i, column = j )
                ter_name = ter_code
                if ter_code in vocab[ 'regions' ]:
                    ter_name = vocab[ 'regions' ][ ter_code ]
                c.value = ter_name
                logging.debug( "%d: %s" % ( j, ter_name ) )
                j += 1
            i += 1
        
        if itemchain:
            j = 0
            chain = json.loads( itemchain )
            terdata = result[ itemchain ]
            for name in sorted( chain ):
                c = ws.cell( row = i, column = j )
                c.value = chain[ name ] 
                j += 1
            
            # Sorting
            for ter_name in sorted_regions:
                ter_code = regions[ ter_name ]
                c = ws.cell( row = i, column = j )
                if ter_code in terdata:
                    ter_value = terdata[ ter_code ]
                    ter_value = re.sub( r'\.0', '', str( ter_value ) )
                else:
                    ter_value = 'NA'
                c.value = ter_value
                j += 1
            i += 1
    
    for line in header:
        c = ws.cell( row = line[ "r" ], column = line[ "c" ] )
        c.value = line[ "value" ]
    
    

    wb.save( fullpath )
    return fullpath

#datakey = '0.34172879'
#datakey = "0.67168331"
#fullpath = "/home/dpe/rusrep/service/test1.xlsx"
#lexicon = preprocessor(datakey)
#filename= create_excel_dataset(fullpath, lexicon)
#print filename
#for lexkey in lexicon:
#    print str(lexkey)
#    print str(lexicon[lexkey])

