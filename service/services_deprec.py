# -*- coding: utf-8 -*-

"""
Cleanup of services.py

FL-06-May-2019 Created
FL-07-May-2019 Changed

Deprecated functions:
def aggregate_historic_items_redun( params ):   
def aggregate_modern_items_redun( params )
def add_unique_items( language, list_name, entry_list_collect, entry_list_none ):
def add_unique_items_grouped( language, dict_name, entry_dict_collect, entry_dict_none )
def collect_fields(  params, eng_data, sql_names, sql_resp ):
def sort_entries( datatype, entry_list ):
def json_generator( params, sql_names, json_dataname, data, qkey_set = None ):
def class_collector( keywords ):
def execute_year( params, sql_query, eng_data ):
def group_by_ident( entry_list ):
def extend_nodups( tot_list, add_list ):
"""

# future-0.17.1 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

import sys
reload( sys )
sys.setdefaultencoding( "utf8" )

import logging
import re

from copy import deepcopy
from datetime import date, datetime
from operator import itemgetter
from time import time, localtime

from services_helpers import get_connection, format_secs, make_query, execute_only, show_path_dict

entry_debug = False


def aggregate_historic_items_redun( params ):
    logging.info( "aggregate_historic_items_redun()" )
    
    language       = params[ "language" ]
    datatype       = params[ "datatype" ]
    base_year      = params[ "base_year" ]
    path_lists     = params[ "path_lists_bylen" ]
    
    num_path_lists = len( path_lists )
    
    entry_list_sorted = []
    entry_list_total  = []
    
    group_tercodes = False  # default situation
    #group_tercodes = True   # group ter_codes with total values per unique path + unit_value
    if group_tercodes:
        logging.debug( "grouping ter_codes with total values per unique path + unit_value" )
    
    entry_dict_ig = {}
    entry_dict_ntc_ig = {}
    entry_dict_none_ig = {}
    entry_list_path_ig = {}
    
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
    
    # loop over the equal length path subgroups
    for pd, path_dict in enumerate( path_lists, start = 1 ):
        show_path_dict( num_path_lists, pd, path_dict )
        
        path_list      = path_dict[ "path_list" ]
        nkeys          = path_dict[ "nkeys" ]
        add_subclasses = path_dict[ "subclasses" ]
        
        params[ "path_list" ] = path_list
        
        prefix = "num"
        logging.debug( "-1- = entry_list_%s" % prefix )
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
        logging.debug( "-3- = entry_list_%s" % prefix )
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
# aggregate_historic_items_redun()


def aggregate_modern_items_redun( params ):
    logging.info( "aggregate_modern_items_redun()" )
    
    language       = params[ "language" ]
    datatype       = params[ "datatype" ]
    #group_tercodes = params[ "group_tercodes" ]
    path_lists     = params[ "path_lists_bylen" ]
    
    num_path_lists = len( path_lists )
    
    entry_list_total = []
    
    # modern classification does not provide a base_year; 
    # loop over base_years, and accumulate results.
    base_years = [ "1795", "1858", "1897", "1959", "2002" ]
    #base_years = [ "1858" ]    # test single year
    
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
        
        entry_list_year = []
        
        for pd, path_dict in enumerate( path_lists, start = 1 ):
            #show_path_dict( num_path_lists, pd, path_dict )
            
            path_list = path_dict[ "path_list" ]
            add_subclasses = path_dict[ "subclasses" ]
            
            params[  "path_list" ] = path_list
            
            prefix = "ntc"
            logging.debug( "-1- = entry_list_%s" % prefix )
            #show_params( "params -1- = entry_list_ntc", params )
            
            sql_query_ntc = make_query( prefix, params, add_subclasses, value_total = True, value_numerical = True )
            sql_names_ntc, sql_resp_ntc = execute_only( sql_query_ntc )
            eng_data_ntc = {}
            #entry_list_ntc = execute_year( params, sql_query_ntc, eng_data_ntc )
            entry_list_ntc = collect_fields( params, eng_data_ntc, sql_names_ntc, sql_resp_ntc )
            
            if entry_debug: 
                show_entries( "params -1- = entry_list_ntc", entry_list_ntc )
            
            # TODO
            #if group_tercodes:
            #   entry_dict_ntc_ig = group_by_ident( entry_list_ntc )
            
            prefix = "none"
            logging.debug( "-2- = entry_list_%s" % prefix )
            #show_params( "params -2- = entry_list_none", params )
            
            sql_query_none = make_query( prefix, params, add_subclasses, value_total = False, value_numerical = False )   # non-numbers
            sql_names_none, sql_resp_none = execute_only( sql_query_none )
            eng_data_none = {}
            #entry_list_none = execute_year( params, sql_query_none, eng_data_none )
            entry_list_none = collect_fields( params, eng_data_none, sql_names_none, sql_resp_none )
            
            if entry_debug: 
                show_entries( "params -2- = entry_list_none", entry_list_none )
            
            # TODO
            #if group_tercodes:
            #   entry_dict_none_ig = group_by_ident( entry_list_none )
            
            #if group_tercodes:
            #   entry_list_path_ig = add_unique_items_grouped( ...
            # TODO: use entry_list_path_ig
            
            # merge the the 2 lists (ntc, none)
            # entry_list_year = entry_list_ntc + entry_list_none
            logging.info( "add_unique_nones()" )
            entry_list_path = add_unique_items( language, "entry_list_none", entry_list_ntc, entry_list_none )
            logging.info( "entry_list_year: %d items" % len( entry_list_path ) )
            
            entry_list_year.extend( entry_list_path )           # different path_dict, so no duplicates 
            
        entry_list_total.extend( entry_list_year )              # different base_year, so no duplicates
        #entry_list_total = extend_nodups( entry_list_total, entry_list_year )  # avoid duplicates
        logging.info( "entry_list_total: %d items" % len( entry_list_total ) )
    
    entry_list_sorted = sort_entries( datatype, entry_list_total )

    return entry_list_sorted
# aggregate_modern_items_redun()


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
        entry_new = deepcopy( entry_modify )
        
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
# add_unique_items()


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
# add_unique_items_grouped(


def collect_fields( params, eng_data, sql_names, sql_resp ):
    # sql_names & sql_resp from execute_only()
    
    time0 = time()      # seconds since the epoch
    logging.debug( "collect_fields() start: %s" % datetime.now() )
    
    forbidden = [ "classification", "action", "language", "path" ]
    
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
    
    params_ = deepcopy( params )       # params path sometimes disrupted by json_generator() ???
    key_set = params[ "key_set" ]
    entry_list = json_generator( params_, sql_names, "data", final_data, key_set )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "collect_fields() took %s" % str_elapsed )

    return entry_list
# collect_fields()


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
        itm = deepcopy( item )
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
            itm = deepcopy( item )
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
# sort_entries()


def json_generator( params, sql_names, json_dataname, data, qkey_set = None ):
    time0 = time()      # seconds since the epoch
    logging.debug( "generator() start: %s" % datetime.now() )
    
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
        entry_path_cpy = deepcopy( entry_path )
        
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
# json_generator()


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
# class_collector()


def execute_year( key_set, params, sql_query, eng_data = {} ):
    logging.info( "execute_year()" )
    
    time0 = time()      # seconds since the epoch
    logging.debug( "execute_year() start: %s" % datetime.now() )
    
    connection = get_connection()
    cursor = connection.cursor()
    sql_query = cursor.mogrify( sql_query )     # needed if single quote has been escaped by repeating it
    cursor.execute( sql_query )
    
    logging.debug( "query execute stop: %s" % datetime.now() )
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
    
    params_ = deepcopy( params )       # params path sometimes disrupted by json_generator() ???
    key_set = params[ "key_set" ]
    entry_list = json_generator( params_, sql_names, "data", final_data, key_set )
    
    str_elapsed = format_secs( time() - time0 )
    logging.info( "execute_year() took %s" % str_elapsed )
    
    return entry_list
# execute_year()


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
# group_by_ident()


def extend_nodups( tot_list, add_list ):
    for entry in add_list:
        if entry not in tot_list:
            tot_list.append( entry )

    return tot_list
# extend_nodups()

# [eof]