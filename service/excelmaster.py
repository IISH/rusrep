#!/usr/bin/python
# -*- coding: utf-8 -*-

# VT-07-Jul-2016 Latest change by VT
# FL-15-May-2017 Latest change

import json
import logging
import openpyxl
import os
import re

from datetime import date
from icu import Locale, Collator
from openpyxl.cell import get_column_letter
from pymongo import MongoClient


def preprocessor( datafilter ):
    logging.debug( "preprocessor() datafilter: %s" % datafilter )
    
    dataset = []
    lex_lands = {}
    lands   = {}
    year    = 0
    lang    = ""

    key = datafilter[ "key" ]
    if key:
        topic = ""
        key_comps = key.split( '-' )
        logging.debug( "key_comps: %s" % str( key_comps ) )
        if len( key_comps ) >= 1:
            lang = key_comps[ 0 ]
        if len( key_comps ) >= 2:
            hist_mod = key_comps[ 1 ]
        if len( key_comps ) >= 3:
            topic = key_comps[ 2 ]
        
        clientcache = MongoClient()
        dbcache = clientcache.get_database( 'datacache' )
        logging.debug( "dbcache.data.find with key: %s" % key )
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
                if lexkey in lex_lands:
                    currentlands = lex_lands[ lexkey ]
                    for item in lands:
                        currentlands[ item ] = lands[ item ]
                    lex_lands[ lexkey ] = currentlands
                else:
                    lex_lands[ lexkey ] = lands
                
                dataset.append( dataitem )
        
        vocab_regs_terms = {}       # regions and terms
        vocab_regs_terms[ "ter_codes" ] = ter_codes
        
        db = clientcache.get_database( 'vocabulary' )   # vocabulary
        
        """
        # moved to aggregate_dataset()
        # load regions
        regions_filter = {}
        regions_filter[ "vocabulary" ] = "ERRHS_Vocabulary_regions"
        if hist_mod == 'h' and year:
            regions_filter[ 'basisyear' ] = str( year )
        vocab_regions = db.data.find( regions_filter )
        regions = {}
        
        logging.debug( "ter_codes: %s" % str( ter_codes ) )
        for item in vocab_regions:
            #logging.debug( str( item ) )
            ID = item[ 'ID' ]
            if ID in ter_codes:
                if lang == 'en':
                    regions[ item[ 'ID' ] ] = item[ 'EN' ]
                else:
                    regions[ item[ 'ID' ] ] = item[ 'RUS' ]
        
        vocab_regs_terms[ "regions" ] = regions
        """
        
        # create sheet_header
        sheet_header = []
        # load terms
        terms_needed  = [ "na", "base_year", "count", "datatype", "value_unit" ]
        if hist_mod == 'h':     # historical
            terms_needed += [ "histclass1", "histclass2", "histclass3", "histclass4", "histclass5", "histclass6", "histclass7", "histclass8", "histclass9", "histclass10" ]
        elif hist_mod == 'm':   # modern
            terms_needed += [ "class1", "class2", "class3", "class4", "class5", "class6", "class7", "class8", "class9", "class10" ]
        
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
        vocab_regs_terms[ "terms" ] = terms
        
        if lang == 'en':
            if hist_mod == 'h':
                classification = "HISTORICAL"
            elif hist_mod == 'm':
                classification = "MODERN"
            else:
                classification = ""
            
            sheet_header.append( { "r" : 1, "c" : 0, "value" : "Electronic repository of Russian Historical Statistics - ristat.org" } )
            sheet_header.append( { "r" : 3, "c" : 0, "value" : "TOPIC:" } )
            sheet_header.append( { "r" : 3, "c" : 1, "value" : topic_name } )
            sheet_header.append( { "r" : 4, "c" : 0, "value" : "BENCHMARK-YEAR:" } )
            sheet_header.append( { "r" : 4, "c" : 1, "value" : year } )
            sheet_header.append( { "r" : 5, "c" : 0, "value" : "CLASSIFICATION:" } )
            sheet_header.append( { "r" : 5, "c" : 1, "value" : classification } )
        else:
            if hist_mod == 'h':
                classification = "ИСТОРИЧЕСКАЯ"
            elif hist_mod == 'm':
                classification = "СОВРЕМЕННАЯ"
            else:
                classification = ""
            
            sheet_header.append( { "r" : 1, "c" : 0, "value" : "Электронный архив Российской исторической статистики - ristat.org" } )
            sheet_header.append( { "r" : 3, "c" : 0, "value" : "ТЕМА:" } )
            sheet_header.append( { "r" : 3, "c" : 1, "value" : topic_name } )
            sheet_header.append( { "r" : 4, "c" : 0, "value" : "ГОД:" } )
            sheet_header.append( { "r" : 4, "c" : 1, "value" : year } )
            sheet_header.append( { "r" : 5, "c" : 0, "value" : "КЛАССИФИКАЦИЯ:" } )
            sheet_header.append( { "r" : 5, "c" : 1, "value" : classification } )
    
    logging.debug( "preprocessor lex_lands: %s" % str( lex_lands ) )
    logging.debug( "preprocessor vocab_regs_terms: %s" % str( vocab_regs_terms ) )
    logging.debug( "preprocessor sheet_header: %s" % str( sheet_header ) )
    
    return ( lex_lands, vocab_regs_terms, sheet_header )



def aggregate_dataset( key, download_dir, xlsx_name, lex_lands, vocab_regs_terms, sheet_header ):
    logging.debug( "aggregate_dataset() key: %s" % key )
    
    xlsx_pathname = os.path.abspath( os.path.join( download_dir, xlsx_name ) )
    logging.debug( "full_path: %s" % xlsx_pathname )
    
    lang = ""
    hist_mod = ""
    base_year_str = ""
    key_comps = key.split( '-' )
    if len( key_comps ) >= 1:
        lang = key_comps[ 0 ]
    if len( key_comps ) >= 2:
        hist_mod = key_comps[ 1 ]
    if len( key_comps ) >= 3:
        base_year_str = key_comps[ 3 ]
        base_year = int( base_year_str )
    logging.debug( "lang: %s, hist_mod: %s, base_year_str: %s" % ( lang, hist_mod, base_year_str ) )
    
    logging.debug( "keys in vocab_regs_terms:" )
    for key in vocab_regs_terms:
        logging.debug( "key: %s" % key )
    
    #na = vocab_regs_terms[ "terms" ][ "na" ]
    #logging.debug( "na: %s" % na )
    
    nsheets = 0
    ter_code_list = []
    wb = openpyxl.Workbook( encoding = 'utf-8' )
    if hist_mod == 'h':
        nsheets = 1
        base_years = [ base_year ]
        ws_0 = wb.get_active_sheet()
        ws_0.title = "Dataset"
        
        ter_name = "ter_codes.txt"
        ter_path = os.path.join( download_dir, ter_name )
        with open( ter_path, "r" ) as f:
            ter_code_str = f.read()
            logging.debug( "ter_code_str: %s, %s" % ( ter_code_str, type( ter_code_str ) ) )
            ter_code_str = ter_code_str[ 1: -1 ]    # remove leading and trailing bracket
            ter_code_str = ter_code_str.replace( "'", "" )
            ter_code_list = ter_code_str.split( ',' )
            logging.debug( "ter_code_list: %s, %s" % ( str( ter_code_list ), type( ter_code_list ) ) )
        
        os.remove( ter_path )       # not in download
    
    elif hist_mod == 'm':
        nsheets = 5
        base_years = [ 1795, 1858, 1897, 1959, 2002 ]
        ws_0 = wb.get_active_sheet()
        ws_0.title = "1795"
        ws_1 = wb.create_sheet( 1, "1858" )
        ws_2 = wb.create_sheet( 2, "1897" )
        ws_3 = wb.create_sheet( 3, "1959" )
        ws_4 = wb.create_sheet( 4, "2002" )
    
    # loop over the data sheets, and select the correct year data
    for sheet_idx in range( nsheets ):
        base_year = base_years[ sheet_idx ]
        logging.debug( "work_sheet: %d, base_year: %s" % ( sheet_idx, base_year ) )
        if sheet_idx == 0:
            ws = ws_0
        elif sheet_idx == 1:
            ws = ws_1
        elif sheet_idx == 2:
            ws = ws_2
        elif sheet_idx == 3:
            ws = ws_3
        elif sheet_idx == 4:
            ws = ws_4
        
        
        clientcache = MongoClient()
        dbcache = clientcache.get_database( 'datacache' )
        logging.debug( "dbcache.data.find with key: %s" % key )
        result = dbcache.data.find( { "key": key } )
        db = clientcache.get_database( 'vocabulary' )   # vocabulary
        
        # load regions for base_year
        regions_filter = {}
        regions_filter[ "vocabulary" ] = "ERRHS_Vocabulary_regions"
        regions_filter[ 'basisyear' ] = str( base_year )
        vocab_regions = db.data.find( regions_filter )
        
        regions = {}
        ter_codes = vocab_regs_terms[ "ter_codes" ]
        logging.debug( "ter_codes: %s" % str( ter_codes ) )
        for item in vocab_regions:
            #logging.debug( str( item ) )
            ID = item[ 'ID' ]
            if ID in ter_codes:
                if lang == 'en':
                    regions[ item[ 'ID' ] ] = item[ 'EN' ]
                else:
                    regions[ item[ 'ID' ] ] = item[ 'RUS' ]
        
        logging.debug( "regions: %s" % str( regions ) )
        vocab_regs_terms[ "regions" ] = regions
        
        logging.debug( "keys in vocab_regs_terms:" )
        for key in vocab_regs_terms:
            logging.debug( "key: %s" % key )
        
        na = vocab_regs_terms[ "terms" ][ "na" ]
        logging.debug( "na: %s" % na )
        
        regions = {}        # key: reg_name, value: reg_code
        reg_names = []      # list of reg_name's
        for ter_code in vocab_regs_terms[ "regions" ]:
            tmpname = vocab_regs_terms[ "regions" ][ ter_code ]
            ter_name = tmpname.decode( 'utf-8' )
            regions[ ter_name ] = ter_code
            reg_names.append( ter_name ) 

        locale = Locale( 'ru' )  # 'el' is the locale code for Greek
        collator = Collator.createInstance( locale )
        sorted_regions = sorted( reg_names, cmp = collator.compare )
        logging.debug( "sorted_regions: %s" % str( sorted_regions ) )
        
        # sheet_header line here; lines above for legend
        i = 9
        logging.debug( "# of itemchains in lex_lands: %d" % len( lex_lands ) )
        for itemchain in lex_lands:
            j = 0
            
            # sheet_header
            if i == 9:
                chain = json.loads( itemchain )
                ter_data = lex_lands[ itemchain ]
                
                logging.debug( "# of names in chain: %d" % len( chain ) )
                logging.debug( "names in chain: %s" % str( chain ) )
                for name in sorted( chain ):
                    if name == "count":         # skip 'count' column in download
                        continue
                    c = ws.cell( row = i, column = j )
                    column_name = name
                    if column_name in vocab_regs_terms[ "terms" ]:
                        column_name = vocab_regs_terms[ "terms" ][ column_name ]
                    c.value = column_name
                    logging.debug( "column %d: %s" % ( j, column_name ) )
                    j += 1
                
                
                
                logging.debug( "# of ter_names in sorted_regions: %d" % len( sorted_regions ) )
                for ter_name in sorted_regions:
                    ter_code = regions[ ter_name ]
                    c = ws.cell( row = i, column = j )
                    ter_name = ter_code
                    if ter_code in vocab_regs_terms[ "regions" ]:
                        ter_name = vocab_regs_terms[ "regions" ][ ter_code ]
                    c.value = ter_name
                    logging.debug( "column %d: %s" % ( j, ter_name ) )
                    j += 1
                i += 1
            
            # data
            if itemchain:
                logging.debug( "itemchain" )
                j = 0
                chain = json.loads( itemchain )
                logging.debug( "# of items: %d" % len( chain ))
                if base_year != chain.get( "base_year" ):
                    logging.debug( "skip: %s, base_year not in chain" % base_year )
                    continue
                
                ter_data = lex_lands[ itemchain ]
                logging.debug( "ter_data: %s: " % str( ter_data ) )
                for name in sorted( chain ):
                    if name == "count":         # skip 'count' column in download
                        continue
                    c = ws.cell( row = i, column = j )
                    c.value = chain[ name ] 
                    logging.debug( "%d: %s" % ( j, name ) )
                    j += 1
                
                # Sorting
                for ter_name in sorted_regions:
                    ter_code = regions[ ter_name ]
                    logging.debug( "ter_name: %s, ter_code: %s: " % ( ter_name, ter_code ) )
                    if ter_code in ter_data:
                        ter_value = ter_data[ ter_code ]
                        ter_value = re.sub( r'\.0', '', str( ter_value ) )
                    else:
                        ter_value = na
                    
                    c = ws.cell( row = i, column = j )
                    c.value = ter_value
                    logging.debug( "%d: %s" % ( j, ter_value ) )
                    j += 1
                
                i += 1
        
    
    #logging.debug( "# of lines in sheet_header: %d" % len( sheet_header ) )
    if hist_mod == 'h':
        for line in sheet_header:
            c = ws.cell( row = line[ "r" ], column = line[ "c" ] )
            c.value = line[ "value" ]
            #logging.debug( "sheet_header r: %d, c: %d, value: %s" % ( line[ "r" ], line[ "c" ], line[ "value" ] ) )
    elif hist_mod == 'm':
        for ws in wb.worksheets:
            prev_value = ""
            for line in sheet_header:
                c = ws.cell( row = line[ "r" ], column = line[ "c" ] )
                if prev_value == "BENCHMARK-YEAR:":
                    c.value = ws.title
                else:
                    c.value = line[ "value" ]
                prev_value = c.value
                logging.debug( "sheet_header r: %d, c: %d, value: %s" % ( line[ "r" ], line[ "c" ], line[ "value" ] ) )
    
    # create copyright sheet; extract language id from filename
    comps1 = xlsx_pathname.split( '/' )
    comps2 = comps1[ -1 ].split( '-' )
    language = comps2[ 0 ]
    logging.debug( "language: %s" % language )
    
    if hist_mod == 'h':
        ws_cr = wb.create_sheet( 1, "Copyrights" )
    else:
        ws_cr = wb.create_sheet( 5, "Copyrights" )
    
    c = ws_cr.cell( row = 1, column = 0 )
    c.value = "Electronic Repository of Russian Historical Statistics / Электронный архив Российской исторической статистики"
    c = ws_cr.cell( row = 2, column = 0 )
    c.value = "2014-%d" % date.today().year
    
    if language == "en":
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
    elif language == "ru":
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

    wb.save( xlsx_pathname )
    
    return xlsx_pathname

# [eof]
