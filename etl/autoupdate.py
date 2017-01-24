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
FL-24-Jan-2017
"""

from __future__ import absolute_import

import ConfigParser
import json
import logging
import os
import psycopg2
import psycopg2.extras
import re
import sys
import urllib
import urllib2
import simplejson
import StringIO

from pymongo import MongoClient
from vocab import vocabulary, classupdate
from xlsx2csv import Xlsx2csv

#import collections
#import ConfigParser
#import csv
#import getopt
#import glob
#import pprint
#import requests
#import tables
#import unittest
#import xlwt
#from flask import Flask, Response, request
#from twisted.web import http

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname("__file__"), './')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname("__file__"), '../')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname("__file__"), '../service')))
#print(sys.path)
#print("pwd:", os.getcwd())

from dataverse import Connection

#from cliocore.configutils import Configuration, Utils, DataFilter
from service.configutils import Configuration, DataFilter


def loadjson(apiurl):
    logging.debug("loadjson() %s" % apiurl)
    jsondataurl = apiurl

    req = urllib2.Request(jsondataurl)
    opener = urllib2.build_opener()
    f = opener.open(req)
    dataframe = simplejson.load(f)
    return dataframe


"""
def connect():
    logging.debug("connect()")
    cparser = ConfigParser.RawConfigParser()
    cpath = "/etc/apache2/rusrep.config"
    cparser.read(cpath)

    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (cparser.get('config', 'dbhost'), cparser.get('config', 'dbname'), cparser.get('config', 'dblogin'), cparser.get('config', 'dbpassword'))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    return cursor
"""


def alldatasets(clioinfra, copy_local):
    logging.info("%s alldatasets()" % __file__)
    #cursor = connect()
    
    host = clioinfra.config['dataverseroot']
    dom = re.match(r'https\:\/\/(.+)$', host)
    if dom:
        host = dom.group(1)
    connection = Connection(host, clioinfra.config['ristatkey'])
    dataverse = connection.get_dataverse('RISTAT')
    
    #settings = DataFilter('')
    papers = []
    kwargs = { 'delimiter' : '|' }

    for item in dataverse.get_contents():
        handle = str(item['protocol']) + ':' + str(item['authority']) + "/" + str(item['identifier'])
        
        #if handle not in [clioinfra.config['hdl_documentation'], clioinfra.config['hdl_vocabularies']]:
        if handle == clioinfra.config['hdl_population']:
            logging.info("use:  handle: %s" % handle)
            datasetid = item['id']
            url = "https://" + str(host) + "/api/datasets/" + str(datasetid) + "/?&key=" + str(clioinfra.config['ristatkey'])
            dataframe = loadjson(url)
            
            try:
                for files in dataframe['data']['latestVersion']['files']:
                    paperitem = {}
                    paperitem['id'] = str(files['datafile']['id'])
                    paperitem['name'] = str(files['datafile']['name'])
                    url = "https://%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, paperitem['id'], clioinfra.config['ristatkey'])
                    logging.info( url )
                    
                    if copy_local:
                        filepath = "%s/%s" % (clioinfra.config['tmppath'], paperitem['name'])
                        csvfile = "%s/%s.csv" % (clioinfra.config['tmppath'], paperitem['name'])
                        logging.info("filepath: %s" % filepath)
                        logging.info("csvfile: %s" % csvfile)
                        
                        f = urllib.urlopen(url)
                        fh = open(filepath, 'wb')
                        fh.write(f.read())
                        fh.close()
                        outfile = open(csvfile, 'w+')
                        Xlsx2csv(filepath, outfile, **kwargs)
            except:
                print("alldatasets()")
                type, value, tb = sys.exc_info()
                logging.error( "%s" % value )
            
        logging.info("skip: handle: %s" % handle)
    
    return papers



def documents_by_handle(clioinfra, handle_name, copy_local=False, to_csv=False, remove_xlsx=True):
    logging.info("%s documents_by_handle() copy_local: %s, to_csv: %s" % (__file__, copy_local, to_csv))
    logging.debug("handle_name: %s" % handle_name )
    
    #cursor = connect()
    
    host = "datasets.socialhistory.org"
    ristatkey = clioinfra.config['ristatkey']
    logging.debug("host: %s" % host)
    logging.debug("ristatkey: %s" % ristatkey)
    
    connection = Connection(host, ristatkey)
    dataverse = connection.get_dataverse('RISTAT')
    
    logging.debug("title: %s" % dataverse.title)
    #datasets = dataverse.get_datasets()
    
    settings = DataFilter('')
    papers = []
    ids = {}
    kwargs = {              # for xlsx files
        'delimiter' : '|', 
        'lineterminator' : '\n'
    }
    
    if copy_local:
        download_dir = os.path.join( clioinfra.config['tmppath'], handle_name)
        logging.info("downloading dataverse files to: %s" % download_dir )
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
    
    for item in dataverse.get_contents():
        # item dict keys: protocol, authority, persistentUrl, identifier, type, id
        handle = str(item['protocol']) + ':' + str(item['authority']) + "/" + str(item['identifier'])
        logging.debug("handle: %s" % handle)
        
        if handle in clioinfra.config[handle_name]:
            logging.debug("using handle: %s" % handle )
            datasetid = item['id']
            url = "https://" + str(host) + "/api/datasets/" + str(datasetid) + "/?&key=" + str(clioinfra.config['ristatkey'])
            dataframe = loadjson(url)
            
            for files in dataframe['data']['latestVersion']['files']:
                paperitem = {}
                paperitem['id'] = str(files['datafile']['id'])
                paperitem['name'] = str(files['datafile']['name'])
                ids[paperitem['id']] = paperitem['name']
                paperitem['handle'] = handle
                paperitem['url'] = "http://data.sandbox.socialhistoryservices.org/service/download?id=%s" % paperitem['id']
                url = "https://%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, paperitem['id'], clioinfra.config['ristatkey'])
                logging.debug( url )
                
                if copy_local:
                    filename = paperitem['name']
                    filepath = "%s/%s" % (download_dir, filename)
                    logging.debug("filepath: %s" % filepath)
                    
                    # read dataverse document from url, write contents to filepath
                    filein = urllib.urlopen(url)
                    fileout = open(filepath, 'wb')
                    fileout.write(filein.read())
                    fileout.close()
                    
                    if to_csv:
                        root, ext = os.path.splitext(filename)
                        logging.info(root)
                        csvpath  = "%s/%s.csv" % (download_dir, root)
                        logging.debug("csvpath:  %s" % csvpath)
                        #csvfile = open(csvpath, 'w+')
                        #xlsx2csv(filepath, csvfile, **kwargs)
                        Xlsx2csv(filepath, **kwargs).convert(csvpath)
                        if remove_xlsx and ext == ".xlsx":
                            os.remove(filepath) # keep the csv, remove the xlsx
                
                # FL-09-Jan-2017 should we not filter before downloading?
                try:
                    if 'lang' in settings.datafilter:
                        varpat = r"(_%s)" % (settings.datafilter['lang'])
                        pattern = re.compile(varpat, re.IGNORECASE)
                        found = pattern.findall(paperitem['name'])
                        if found:
                            papers.append(paperitem)
                    
                    if 'topic' in settings.datafilter:
                        varpat = r"(_%s_.+_\d+_+%s.|class|region)" % (settings.datafilter['topic'], settings.datafilter['lang'])
                        pattern = re.compile(varpat, re.IGNORECASE)
                        found = pattern.findall(paperitem['name'])
                        if found:
                            papers.append(paperitem)
                    else:
                        if 'lang' not in settings.datafilter:
                            papers.append(paperitem)
                except:
                    if 'lang' not in settings.datafilter:
                        papers.append(paperitem)

    return (papers, ids)



def update_vocabularies(clioinfra, mongo_client, copy_local=False, remove_xlsx=True):
    logging.info("%s update_vocabularies()" % __file__)
    """
    update_vocabularies():
    -1- retrieves ERRHS_Vocabulary_*.tab files from dataverse
    -2- with copy_local=True stores them locally
    -3- drops the MogoDB db = 'vocabulary', collection = 'data
    -4- stores the new data in MogoDB db = 'vocabulary', collection = 'data
    """
    
    handle_name = "hdl_vocabularies"
    logging.info("retrieving documents from dataverse for handle name %s ..." % handle_name )
    (docs, ids) = documents_by_handle(clioinfra, handle_name, copy_local, remove_xlsx)
    ndoc =  len(docs)
    logging.info("%d documents retrieved from dataverse" % ndoc)
    if ndoc == 0:
        logging.info("no documents, nothing to do.")
        return
    
    logging.debug("keys in ids:")
    for key in ids:
        logging.debug("key: %s, value: %s" % (key, ids[key]))
        
    logging.debug("docs:")
    for doc in docs:
        logging.debug(doc)
    
    
    # parameters to retrieve the vocabulary files
    host = clioinfra.config['dataverseroot']
    apikey = clioinfra.config['ristatkey']
    dbname = clioinfra.config['vocabulary']
    logging.debug("host: %s" % host)
    logging.debug("apikey: %s" % apikey)
    logging.debug("dbname: %s" % dbname)
    
    vocab_json = [{}]
    # the vocabulary files may already have been downloadeded by documents_by_handle();
    # vocabulary() retrieves them again, and --together with some filetring-- 
    # appends them to a bigvocabulary
    bigvocabulary = vocabulary(host, apikey, ids)       # type: <class 'pandas.core.frame.DataFrame'>
    #print bigvocabulary.to_json(orient='records')
    vocab_json = json.loads(bigvocabulary.to_json(orient='records'))  # type: <type 'list'>
    
    """
    vocab_json0 = vocab_json[0]
    for key in vocab_json0:
        logging.info("key: %s, value: %s" % (key, vocab_json0[key]))
    
    # there are 7 keys per dictionary entry, e.g vocab_json0:
    key: basisyear,  value: None
    key: EN,         value: Male
    key: vocabulary, value: ERRHS_Vocabulary_modclasses
    key: DATATYPE,   value: None
    key: YEAR,       value: None
    key: RUS,        value: мужчины
    key: ID,         value: MOD_1.01_1
    """
    
    logging.info("processing %d vocabulary items..." % len(vocab_json))
    for item in vocab_json:
        #logging.debug(item)
        if 'YEAR' in item:
            item['YEAR'] = re.sub(r'\.0', '', str(item['YEAR']))
        if 'basisyear' in item:
            item['basisyear'] = re.sub(r'\.0', '', str(item['basisyear']))
    
    db = mongo_client.get_database(dbname)
    logging.info("delete all documents from collection 'data' in mongodb db '%s'" % dbname)
    # drop the documents from collection 'data'; same as: db.drop_collection(coll_name)
    db.data.drop()
    
    logging.info("inserting vocabulary in mongodb '%s'" % dbname)
    result = db.data.insert(vocab_json)

    logging.info("clearing mongodb cache")
    db = mongo_client.get_database('datacache')
    db.data.drop()      # remove the collection 'data'



def retrieve_population(clioinfra, copy_local=False, to_csv=False, remove_xlsx=True):
    logging.info("retrieve_population() %s" % copy_local )

    handle_name = "hdl_population"
    logging.info("retrieving documents from dataverse for handle name %s ..." % handle_name )
    (docs, ids) = documents_by_handle(clioinfra, handle_name, copy_local, to_csv, remove_xlsx)
    ndoc =  len(docs)
    if ndoc == 0:
        logging.info("no documents retrieved.")
        return
    else:
        logging.info("%d documents for handle %s retrieved from dataverse" % (ndoc, handle_name))
    
    logging.debug("keys in ids:")
    for key in ids:
        logging.debug("key: %s, value: %s" % (key, ids[key]))
        
    logging.debug("docs:")
    for doc in docs:
        logging.debug(doc)



def store_population(clioinfra):
    logging.info("store_population()")
    
    handle_name = "hdl_population"
    csvdir = os.path.join( clioinfra.config['tmppath'], handle_name)
    if not os.path.isdir(csvdir):
        print("in %s" % __file__)
        print("csvdir %s DIRECTORY DOES NOT EXIST" % csvdir )
        print("EXIT" )
        sys.exit(1)
    logging.info("using csv directory: %s" % csvdir )

    configpath = RUSREP_CONFIG_PATH
    if not os.path.isfile(configpath):
        print("in %s" % __file__)
        print("configpath %s FILE DOES NOT EXIST" % configpath )
        print("EXIT" )
        sys.exit(1)
    logging.info("using configuration: %s" % configpath)

    configparser = ConfigParser.RawConfigParser()
    configparser.read(configpath)
    
    host     = configparser.get('config', 'dbhost')
    dbname   = configparser.get('config', 'dbname')
    user     = configparser.get('config', 'dblogin')
    password = configparser.get('config', 'dbpassword')
    table    = "russianrepository"
    
    connection_string = "host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password)
    logging.info("connection_string: %s" % connection_string)

    connection = psycopg2.connect(connection_string)
    cursor = connection.cursor()

    sql = "truncate %s;" % table
    logging.info(sql)
    cursor.execute(sql)

    dirlist = os.listdir(csvdir) 
    for filename in dirlist:
        root, ext = os.path.splitext(filename)
        if root.startswith("ERRHS_") and ext == ".csv":
            #table = root.lower()
            logging.info("use:  %s, to table: %s" % (filename, table))
            in_pathname = os.path.abspath(os.path.join(csvdir, filename))
            logging.debug(in_pathname)
            #test_csv_file(pathname)
            
            #out_pathname = write_psv_file(csvdir, filename)
            #psv_file = open(out_pathname, 'r')
            #cursor.copy_from(psv_file, table, sep='|')
            
            stringio_file = filter_csv(csvdir, filename)
            cursor.copy_from(stringio_file, table, sep='|')
            connection.commit()
            
            #csv_strings.close()  # close object and discard memory buffer
            #csvfile.close()
            
        else:
            logging.info("skip: %s" % filename)

        #print("break")
        #break
    
    ndoc = len(dirlist)
    logging.info("%d documents for handle %s stored in table %s" % (ndoc, handle_name, table))
    cursor.close()
    connection.close()



def test_csv_file(path_name):
    csv_file = open(path_name, 'r')
    nlines = 0
    
    for line in csv_file:
        cnt = line.count('|')
        fields = line.split('|')
        nfields = len(fields)
        print("%d: %d" % (nline, nfields))
        nlines += 1
        
    print("%d" % nlines)



def filter_csv(csvdir, in_filename):
    logging.debug("filter_csv()")
    
    column_names = [
        "indicator_id",
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
        "base_year", 
        "indicator", 
        "valuemark"
    ]
    ncolumns = len(column_names)
    logging.debug("# of columns: %d" % ncolumns)
    
    in_pathname = os.path.abspath(os.path.join(csvdir, in_filename))
    csv_file = open( in_pathname, 'r')
    
    """
    root, ext = os.path.splitext(in_filename)
    out_pathname = os.path.abspath(os.path.join(csvdir, root + ".psv"))
    print(out_pathname)
    out_file = open(out_pathname, 'w')
    """
    
    out_file = StringIO.StringIO()      # in-memory file
    
    nline = 0
    nskipped = 0
    for line in csv_file:
        nline += 1
        #logging.info("line %d: %s" % (nline, line))
        line = line.strip('\n')   # remove trailing \n
        #logging.info("%d in: %s" % (nline, line))
        #print("# new lines: %d" % line.count('\n'))
        
        if len(line) == line.count('|'):
            nskipped += 1
            continue
        
        fields = line.split('|')
        if nline == 1:
            nfields = len(fields)
            csvheader_names = map(str.lower, fields)
            logging.debug("# of fields: %d" % nfields)
            ndiff = nfields - ncolumns  # NB "indicator_id" is not in the fields
            #logging.info("ndiff: %d" % ndiff)
            continue        # do not store header line
        """
        # No, do not remove: the dots must be in db; filter on RiStat requests
        else:
            # remove dots from trailing '.' filler fields for histclass & class fields
            nzaphc = 0
            for i in reversed(range(nfields)):  # histclass fields
                #print("%2d %s: %s" % (i, csvheader_names[i], fields[i]))
                if csvheader_names[i].startswith( "histclass" ):
                    if fields[i] == ".":
                        fields[i] = ""
                        nzaphc += 1
                    else:
                        break
            
            nzapc = 0
            for i in reversed(range(nfields)):  # class fields
                #print("%2d %s: %s" % (i, csvheader_names[i], fields[i]))
                if csvheader_names[i].startswith( "class" ):
                    if fields[i] == ".":
                        fields[i] = ""
                        nzapc += 1
                    else:
                        break
            """
            """
            if nzaphc != 0 or nzapc != 0:
                print("nzaphc: %d, nzapc: %d" % (nzaphc, nzapc))
                #print(line)
                #print("|".join(fields))
                for i in range(len(fields)):
                    print("%2d %s: %s" % (i, csvheader_names[i], fields[i]))
                sys.exit(0)
            """
        #print("|".join(fields))
        if ndiff > 0:
            npop = 1 + ndiff
            for _ in range(npop):
                fields.pop()            # skip trailing n fields
            #logging.info("# of fields popped: %d" % npop)
        elif ndiff < 0:
            napp = abs(ndiff) - 1
            for _ in range(napp):
                fields.append("")
            #logging.info("# of fields added: %d" % napp)
        
        # column indicator_id should become the primairy key
        fields.insert(0, "0")    # prepend indicator_id, not in csv file
        #print("|".join(fields))
        
        """
        if nline == 2:
            for i in range(len(fields)):
                print("%2d %s: %s" % (i, column_names[i], fields[i]))
        """
        # i = 38: base_year, must be integer
        try:
            dummy = int(fields[38])
        except:
            fields[38] = "0"
        
        if fields[40] not in ("true", "false"):
            fields[40] = "false"
        
        table_line = "|".join(fields)
        """
        if nline == 2:
            print("%d fields" % len(fields))
            print(short_line)
        
            for i in range(len(fields)):
                print("%2d %s: %s" % (i, column_names[i], fields[i]))
        """
        #logging.info("%d out: %s" % (nline, table_line))
        out_file.write("%s\n" % table_line )
    
    out_file.seek(0)    # start of the stream
    #out_file.close()    # closed by caller!: closing discards memory buffer
    csv_file.close()
    
    if nskipped != 0:
        logging.info("%d empty lines (|-only) skipped" % nskipped)
    
    #return out_pathname
    return out_file



def update_population(clioinfra, mongo_client):
    logging.info("update_population()")
    
    configpath = RUSREP_CONFIG_PATH
    logging.info("using configuration: %s" % configpath)
    # classupdate() uses postgresql access parameters from cpath contents
    classdata = classupdate(configpath)     # fetching historic and modern class data from postgresql table 
    
    dbname = clioinfra.config['vocabulary']
    logging.info("inserting historic and modern class data in mongodb '%s'" % dbname)
    
    db = mongo_client.get_database(dbname)
    result = db.data.insert(classdata)



if __name__ == "__main__":
    #log_level = logging.DEBUG
    log_level = logging.INFO
    logging.basicConfig(level=log_level)
    logging.info(__file__)
    
    RUSREP_HOME = os.environ["RUSREP_HOME"]
    logging.info("RUSREP_HOME: %s" % RUSREP_HOME)
    
    CLIOINFRA_HOME = os.environ["CLIOINFRA_HOME"]
    logging.info("CLIOINFRA_HOME: %s" % CLIOINFRA_HOME)
    
    RUSREP_CONFIG_PATH = RUSREP_HOME + "/config/russianrep.config"
    logging.info("RUSREP_CONFIG_PATH: %s" % RUSREP_CONFIG_PATH )
    
    CLIOINFRA_CONFIG_PATH = CLIOINFRA_HOME + "/config/clioinfra.conf"
    logging.info("CLIOINFRA_CONFIG_PATH: %s" % CLIOINFRA_CONFIG_PATH )
    
    # service.configutils.Configuration() uses path to clioinfra.conf from service.__init__.py , 
    clioinfra    = Configuration()
    mongo_client = MongoClient()
    
    # Downloaded vocabulary documents are not used to update the vocabularies, 
    # they are processed on the fly, and put in MongoDB
    # Notice that the MongoDB table is dropped before re-filling, so 
    # update_vocabularies() must be the first function called in __main__. 
    copy_local = False
    update_vocabularies(clioinfra, mongo_client, copy_local)
    
    copy_local = False
    to_csv = True
    remove_xlsx = True
    retrieve_population(clioinfra, copy_local, to_csv, remove_xlsx) # dataverse  => local_disk
    store_population(clioinfra)                         # ? local_disk => postgresql
    update_population(clioinfra, mongo_client)          # ? postgresql => mongodb

# [eof]
