# FL-15-Nov-2017 Ceated
# FL-07-May-2017 Changed

"""

def sqlfilter( sql ):
def sqlconstructor( sql ):
def dataset_filter( data, sql_names, classification ):
def get_sql_where( name, value ):
def aggregate_year( params, add_subclasses, value_total = True, value_numerical = True ):
def aggregate_year( params, add_subclasses, value_total = True, value_numerical = True ):
def make_identifier( path, value_unit ):
def reorder_entries( params, entry_list_ntc, entry_list_none, entry_list = None)
def show_record_list( list_name, record_list ):
def remove_dups( entry_list_collect ):
def records2oldentries( records_dict, params ):
def merge_3records( record_dict_num, record_dict_ntc, record_dict_none ):
def merge_2records( record_dict_total, record_dict_path ):
def topic_counts( schema ):    # obsolete, remove
def load_topics( qinput ):     # obsolete, remove
def filecat_subtopic( qinput, cursor, datatype, base_year ):
def translate_classes( cursor, classinfo ):
def load_data( cursor, year, datatype, region, debug ):
def rdf_convertor( url ):

@app.route( "/export" )                                         def export():
@app.route( "/filecatalog", methods = [ 'POST', 'GET' ] )       def filecatalog():
@app.route( "/vocab" )                                          def vocab():
@app.route( "/translate" )                                      def translate():
@app.route( "/filter", methods = [ "POST", "GET" ] )            def login( settings = '' ):
@app.route( "/maps" )                                           def maps():
"""

#from __future__ import absolute_import      # VT

from StringIO import StringIO


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
# sqlfilter()


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
# sqlconstructor()


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
# dataset_filter()


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
# get_sql_where()


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
# aggregate_year()


def make_identifier( path, value_unit ):
    # ordered dict, sorted by keys
    ident_dict = collections.OrderedDict( sorted( path.items(), key = lambda t: t [ 0 ] ) )
    ident_dict[ "value_unit" ] = value_unit

    # identifier must be immutable
    #identifier = frozenset( ident_dict.items() )
    identifier = json.dumps( ident_dict.items(), encoding = "utf-8" )
    
    return identifier
# make_identifier()


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
# reorder_entries()


def show_record_list( list_name, record_list ):
    logging.info( "show_record_list()" )
    logging.info( "%s # of records: %d" % ( list_name, len( record_list ) ) )
    for r, record in enumerate( record_list ):
        path_unit_str = record[ "path_unit_str" ]
        ter_codes  = record[ "ter_codes" ]
        logging.info( "# %d path_unit: %s" % ( r, path_unit_str ) )
        logging.info( "# %d ter_codes: %s" % ( r, str( ter_codes ) ) )
# show_record_list()


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



def records2oldentries( records_dict, params ):
    # compatibility check with old response structure
    time0 = time()      # seconds since the epoch
    logging.info( "records2oldentries() start: %s" % datetime.now() )

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



def merge_3records( record_dict_num, record_dict_ntc, record_dict_none ):
    time0 = time()      # seconds since the epoch
    logging.info( "merge_3records() start: %s" % datetime.now() )
    
    show_record_dict( "record_dict_num", record_dict_num )
    show_record_dict( "record_dict_ntc", record_dict_ntc )
    show_record_dict( "record_dict_none", record_dict_none )
    
    record_dict = deepcopy( record_dict_num )
    
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



def merge_2records( record_dict, record_dict_path ):
    time0 = time()      # seconds since the epoch
    logging.info( "merge_2records() start: %s" % datetime.now() )
    
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



def translate_classes( cursor, classinfo ):
    logging.debug( "translate_classes()" )
    
    dictdata = {}
    
    sql = "SELECT * FROM datasets.classmaps"
    sqlclass = ''
    for classname in classinfo:
        if sqlclass:
            sqlclass = "%s, '%s'" % ( sqlclass, classinfo[ classname ] )
        else:
            sqlclass = "'%s'" % classinfo[ classname ]
    sql = "%s (%s)" % ( sql, sqlclass )

    sql = "SELECT * FROM datasets.regions"
    cursor.execute( sql )
    data = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    
    if data:
        for value_str in data:
            data_keys = {}
            for i in range( len( value_str ) ):
                name  = sql_names[ i ]
                value = value_str[ i ]
                if name == "region_name":
                    name = "class_rus"
                if name == "region_name_eng":
                    name = "class_eng"
                data_keys[ name ] = value

            dictdata[ data_keys[ "class_eng" ] ] = data_keys
            dictdata[ data_keys[ "class_rus" ] ] = data_keys
        
    sql = "SELECT * from datasets.valueunits";
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
            dictdata[ data_keys[ "class_rus" ] ] = data_keys
            dictdata[ data_keys[ "class_eng" ] ] = data_keys
    
    sql = "SELECT * from datasets.classmaps"
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
            dictdata[ data_keys[ "class_rus" ] ] = data_keys
            dictdata[ data_keys[ "class_eng" ] ] = data_keys

    return dictdata



def load_data( cursor, year, datatype, region, debug ):
    logging.debug("load_data()")
    data = {}

    query = "select * from russianrepository WHERE 1 = 1 "
    query = sqlfilter( query )
    if debug:
        print( "DEBUG " + query + " <br>\n" )
    query += " order by territory asc"
    
    cursor.execute( query )
    records = cursor.fetchall()
    
    row_count = 0
    i = 0
    for row in records:
        i = i + 1
        data[ i ] = row
    
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    json_list = json_generator( sql_names, "data", records )
    
    return json_list



def rdf_convertor( url ):
    logging.debug( "rdf_convertor()" )
    
    f = urllib.urlopen( url )
    data = f.read()
    csvio = StringIO( str( data ) )
    dataframe = pd.read_csv( csvio, sep = '\t', dtype = "unicode" )
    final_subset = dataframe
    columns = final_subset.columns
    
    configparser = get_configparser()
    rdf_prefix = configparser.get( "config", "rdf_prefix" )
    vocab_uri  = configparser.get( "config", "vocab_uri" )
    
    rdf = rdf_prefix
    g   = Graph()

    for ids in final_subset.index:
        item = final_subset.ix[ ids ]
        uri = term.URIRef( "%s%s" % ( vocab_uri, str( item[ "ID" ] ) ) )
        if uri:
            for col in columns:
                if col is not "ID":
                    if item[ col ]:
                        c = term.URIRef( col )
                        g.add( ( uri, c, Literal( str( item[ col ] ) ) ) )
                        rdf += "ristat:%s " % item[ "ID" ]
                        rdf += "ristat:%s ristat:%s." % ( col, item[ col ] )
                    rdf += "\n"
    return g

# some pieces of: 
def reorder_entries( params, entry_list_ntc, entry_list_none, entry_list = None ):
...
    #use_temp_file = True   # not finished, csv file: each line complete and sorted as db columns!
    use_temp_file = False
    
    csvwriter = None
    #delimiter = b'|'
    delimiter = b','
    quoting = csv.QUOTE_NONNUMERIC
    #quoting = csv.QUOTE_MINIMAL
    #quotechar = b'|'

    if use_temp_file:
        configparser = get_configparser()
        tmp_dir = configparser.get( "config", "tmppath" )
        csv_dir = os.path.join( tmp_dir, "download" )
        csv_name = "temp.csv"
        csv_path = os.path.join( csv_dir, csv_name )
        
        #if os.path.isfile( csv_path ):
        csv_file = open( csv_path, "wb" )
        csv_file.truncate()
        
        #csvwriter = csv.writer( csv_path, delimiter = delimiter, quotechar = quotechar, quoting = quoting )
        csvwriter = csv.writer( csv_file, delimiter = delimiter, quoting = quoting )
        
        """
        #csv_file.write( "%s\n" % csv_line )
        csv_line = [ "column_1", "column_2", "column_2" ]
        csvwriter.writerow( csv_line )
        
        csv_line = [ 'a b', 3.14, 'x y z ' ]
        csvwriter.writerow( csv_line )
        """
    
...
    """
    # no header with copy_from
    if use_temp_file:
        csv_header = []
        for column in range( 1, nlevels_use + 1 ):
            csv_header.append( "%s%d" % ( level_prefix, column ) )
        csv_header.append( "value_unit" )
        csv_header.append( "count" )
        for tc, ter_code in enumerate( ter_codes ):
            csv_header.append( "tc_%s" % ter_code )
        logging.info( csv_header )
        csvwriter.writerow( csv_header )
    """
...

...
    if use_temp_file:
        csv_file.close()
        #sql_copy = "COPY %s FROM '%s' DELIMITER '%s' CSV HEADER;" % ( table_name, csv_path, delimiter )
        #logging.debug( sql_copy )
        #cursor.execute( sql_copy )
        #cursor.copy_from( csv_file, table_name, columns = %s ) % columns
        logging.debug( "levels_str: %s" % levels_str )
        logging.debug( "ter_codes_str: %s" % ter_codes_str )
        levels_str.sort()
        ter_codes_str.sort()
        logging.debug( "levels_str: %s" % levels_str )
        logging.debug( "ter_codes_str: %s" % ter_codes_str )
        columns_sort = list( levels_str )
        columns_sort.append( "value_unit" )
        columns_sort.append( "count" )
        columns_sort.extend( ter_codes_str )
        logging.debug( columns_sort )
        csv_file = open( csv_path, "rb" )
        cursor.copy_from( csv_file, table_name, columns = columns_sort )
        csv_file.close()
...
...
        if use_temp_file:
            csvwriter.writerow( values )
        else:
		  ...
...

# ------------------------------------------------------------------------------
@app.route( "/export" )
def export():
    logging.debug( "/export" )
    
    configparser = get_configparser()
    keys = [ "intro", "intro_rus", "datatype_intro", "datatype_intro_rus", "note", "note_rus", "downloadpage1", "downloadpage1_rus" "downloadclick", "downloadclick_rus", "warningblank", "warningblank_rus", "mapintro", "mapintro_rus" ]
    exportkeys = {}
    for ikey in keys:
        if configparser.get( "config", ikey ) is not None:
            exportkeys[ ikey ] = configparser.get( "config", ikey )
    result = json.dumps( exportkeys, encoding = "utf8", ensure_ascii = False, sort_keys = True, indent = 4 )
    return Response( result, mimetype = "application/json; charset=utf-8" )



@app.route( "/topics_old" )
def topics_old():
    logging.debug( "/topics_old" )
    # uses a pre-fabricated postgres table: obsolete
    language = request.args.get( "language" )
    download_key = request.args.get( "download_key" )
    
    json_list = load_topics()
    json_string, cache_except = json_cache( json_list, language, "data", download_key )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



# Is this no longer in use?
# Its functionality seems not ok.
@app.route( "/filecatalog", methods = [ "POST", "GET" ] )
def filecatalog():
    logging.debug( "/filecatalog" )
    
    # e.g.: ?lang=en&subtopics=1_01_1795x1_02_1795
    subtopic_list = []
    logging.debug( "# of arguments %s" % len( request.args ) )
    for arg in request.args:
        logging.debug( "arg: %s, value: %s" % ( arg, request.args[ arg ] ) )
        if arg.startswith( "subtopics" ):
            subtopic_list.append( request.args[ arg ] )
    
    language = request.args.get( "lang" )
    download_key = request.args.get( "download_key" )
    
    logging.debug( "lang: %s" % language )
    logging.debug( "download_key: %s" % download_key )
    logging.debug( "subtopics: %s" % subtopic_list )
    
    json_list = []
    
    if subtopic_list is not None:
        connection = get_connection()
        cursor = connection.cursor()
        
        for subtopic in subtopic_list:
            logging.debug( "subtopic: %s" % subtopic )
            if len( subtopic ) == 9:    # e.g.: 1_01_1795
                base_year = subtopic[ 5: ]
                datatype  = subtopic[ :4 ]
                datatype  = datatype.replace( '_', '.' )
                logging.debug( "datatype: %s, base_year: %s" % ( datatype, base_year ) )
                json_list1 = filecat_subtopic( cursor, datatype, base_year )
                #json_list.append( json_list1 )
        
        cursor.close()
        connection.close()
    
    json_string, cache_except = json_cache( json_list, language, "data", download_key )
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



@app.route( "/vocab" )
def vocab():
    logging.debug( "/vocab" )
    
    configparser   = get_configparser()
    dataverse_root = configparser.get( "config", "dataverse_root" )
    ristatkey      = configparser.get( "config", "ristatkey" )
    
    url = "%s/api/access/datafile/586?&key=%s&show_entity_ids=true&q=authorName:*" % ( dataverse_root, ristatkey )
    g = rdf_convertor( url )
    showformat = "json"
    if request.args.get( "format" ):
        showformat = request.args.get( "format" )
    if showformat == "turtle":
        jsondump = g.serialize( format = "n3" )
        return Response( jsondump, mimetype = "application/x-turtle; charset=utf-8" )
    else:
        jsondump = g.serialize( format = "json-ld", indent = 4 )
        return Response( jsondump, mimetype = "application/json; charset=utf-8" )



@app.route( "/translate" )
def translate():
    logging.debug( "/translate" )
    
    connection = get_connection()
    cursor = connection.cursor()
    
    sql = "SELECT * FROM datasets.classmaps";
    cursor.execute( sql )
    data = cursor.fetchall()
    sql_names = [ desc[ 0 ] for desc in cursor.description ]
    cursor.close()
    connection.close()
    
    json_list = json_generator( sql_names, "data", data )
    
    language = request.args.get( "language" )
    download_key = request.args.get( "download_key" )
    json_string, cache_except = json_cache( json_list, language, "data", download_key )
    
    return Response( json_string, mimetype = "application/json; charset=utf-8" )



@app.route( "/filter", methods = [ "POST", "GET" ] )
def login( settings = '' ):
    logging.debug( "login()" )
    
    filter = {}
    try:
        qinput = request.json
    except:
        return "{}"
    
    try:
        if qinput[ "action" ] == "aggregate":
            sql = "SELECT histclass1, datatype, value_unit, value, ter_code FROM russianrepository"
    except:
        sql = "SELECT * FROM datasets.classification";
    
    try:
        classification = qinput[ "classification" ]
    except:
        classification = "historical"
    
    forbidden = [ "classification", "action", "language" ]
    n = 0
    for name in qinput:
        if not name in forbidden:
            n += 1
            if n == 1:
                sql += " WHERE"
            else:
                sql += " AND"
            sql += " %s = '%s'" % ( name, qinput[ name ] )
    
    if sql:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.execute( sql )
        data = cursor.fetchall()
        sql_names = [ desc[ 0 ] for desc in cursor.description ]
        cursor.close()
        connection.close()
        
        json_data = dataset_filter( data, sql_names, classification )
        return Response( json_data, mimetype = "application/json; charset=utf-8" )
    else:
        return "{}"



# http://bl.ocks.org/mbostock/raw/4090846/us.json
@app.route( "/maps" )
def maps():
    logging.debug( "/maps" )
    donors_choose_url = "http://bl.ocks.org/mbostock/raw/4090846/us.json"
    response = urllib2.urlopen( donors_choose_url )
    json_response = json.load( response )
    
    return Response( json_response, mimetype = "application/json; charset=utf-8" )


# [eof]
