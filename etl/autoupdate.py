#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
This script updates the vocabulary files: 
retrieving them from Dataverse, writing to MongoDB
The documentation files in PostgreSQL are not used

VT-07-Jul-2016 latest change by VT
FL-09-Jan-2017
"""

from __future__ import absolute_import

import json
import logging
import os
import re
import sys
import urllib
import urllib2
import simplejson

from pymongo import MongoClient
from vocab import vocabulary, classupdate
from xlsx2csv import xlsx2csv

#import collections
#import ConfigParser
#import csv
#import getopt
#import glob
#import pprint
#import psycopg2
#import psycopg2.extras
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

def alldatasets():
    logging.debug("%s alldatasets()" % __file__)
    #cursor = connect()
    clioinfra = Configuration()
    host = clioinfra.config['dataverseroot']
    dom = re.match(r'https\:\/\/(.+)$', host)
    if dom:
        host = dom.group(1)
    connection = Connection(host, clioinfra.config['ristatkey'])
    dataverse = connection.get_dataverse('RISTAT')
    
    #settings = DataFilter('')
    #papers = []
    kwargs = { 'delimiter' : '|' }

    for item in dataverse.get_contents():
        handle = str(item['protocol']) + ':' + str(item['authority']) + "/" + str(item['identifier'])
        logging.debug("handle: %s" % handle)
        if handle not in [clioinfra.config['ristatdocs'], clioinfra.config['ristatvoc']]:
            datasetid = item['id']
            url = "https://" + str(host) + "/api/datasets/" + str(datasetid) + "/?&key=" + str(clioinfra.config['ristatkey'])
            dataframe = loadjson(url)
            for files in dataframe['data']['latestVersion']['files']:
                paperitem = {}
                paperitem['id'] = str(files['datafile']['id'])
                paperitem['name'] = str(files['datafile']['name'])
                url = "https://%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, paperitem['id'], clioinfra.config['ristatkey'])
                filepath = "%s/%s" % (clioinfra.config['tmppath'], paperitem['name'])
                csvfile = "%s/%s.csv" % (clioinfra.config['tmppath'], paperitem['name'])
                f = urllib.urlopen(url)    
                print filepath
                fh = open(filepath, 'wb')
                fh.write(f.read())
                fh.close()
                outfile = open(csvfile, 'w+')
                xlsx2csv(filepath, outfile, **kwargs)
    return ''


def download_docs(handle_name):
    logging.debug("%s download_docs()" % __file__)
    #cursor = connect()
    clioinfra = Configuration()
    
    host = "datasets.socialhistory.org"
    connection = Connection(host, clioinfra.config['ristatkey'])
    dataverse = connection.get_dataverse('RISTAT')
    
    logging.debug("title: %s" % dataverse.title)
    #datasets = dataverse.get_datasets()
    
    settings = DataFilter('')
    papers = []
    ids = {}
    
    logging.debug("handle_name: %s" % handle_name )
    tmppath = os.path.join( clioinfra.config['tmppath'], handle_name)
    logging.debug("downloading dataverse files to: %s" % tmppath )
    if not os.path.exists(tmppath):
        os.makedirs(tmppath)
    
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
                filepath = "%s/%s" % (tmppath, paperitem['name'])
                #filepath = "%s/%s" % (clioinfra.config['tmppath'], paperitem['name'])
                #csvfile = "%s/%s.csv" % (clioinfra.config['tmppath'], paperitem['name'])
                logging.debug( url )
                
                # read dataverse document from url, write contents to filepath
                f = urllib.urlopen(url)
                fh = open(filepath, 'wb')
                fh.write(f.read())
                fh.close()
                
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


def update_vocabularies():
    logging.info("%s update_vocabularies()" % __file__)
    #docs = alldatasets()
    
    handle_name = "hdl_vocabularies"
    logging.info("download documents from dataverse for handle name %s ..." % handle_name )
    (docs, ids) = download_docs(handle_name)
    ndoc =  len(docs)
    logging.info("%d documents downloaded from dataverse" % ndoc)
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
    clioinfra = Configuration()
    host = clioinfra.config['dataverseroot']
    apikey = clioinfra.config['ristatkey']
    dbname = clioinfra.config['vocabulary']
    logging.debug("host: %s" % host)
    logging.debug("apikey: %s" % apikey)
    logging.debug("dbname: %s" % dbname)
    
    client = MongoClient()
    db = client.get_database(dbname)
    logging.info("drop data in mongodb db %s" % dbname)
    db.data.drop()      # remove the data; same as: db.drop_collection(dbname)
    
    # the vocabulary files have already been downloadeded by download_docs();
    # vocabulary() downloads them again, and --together with some filetring-- 
    # appends them to a bigvocabulary
    bigvocabulary = vocabulary(host, apikey, ids)       # type: <class 'pandas.core.frame.DataFrame'>
    #print bigvocabulary.to_json(orient='records')
    data = json.loads(bigvocabulary.to_json(orient='records'))  # type: <type 'list'>
    
    logging.debug("processing %d vocabulary items..." % len(data))
    for item in data:
        #logging.debug(item)
        if 'YEAR' in item:
            item['YEAR'] = re.sub(r'\.0', '', str(item['YEAR']))
        if 'basisyear':
            item['basisyear'] = re.sub(r'\.0', '', str(item['basisyear']))
    
    logging.info("inserting data in mongodb")
    result = db.data.insert(data)

    logging.info("fetching classdata from postgresql")
    classdata = classupdate()
    
    logging.info("inserting classdata in mongodb")
    result = db.data.insert(classdata)
    
    logging.debug("clearing mongodb cache")
    db = client.get_database('datacache')
    db.data.drop()      # remove the data; same as: db.drop_collection(dbname)


def update_data():
    logging.info("update_data()")

    handle_name = "hdl_population"
    logging.info("download documents from dataverse for handle name %s ..." % handle_name )
    (docs, ids) = download_docs(handle_name)
    ndoc =  len(docs)
    logging.info("%d documents downloaded from dataverse" % ndoc)
    if ndoc == 0:
        logging.info("no documents, nothing to do.")
        return
    
    logging.debug("keys in ids:")
    for key in ids:
        logging.debug("key: %s, value: %s" % (key, ids[key]))
        
    logging.debug("docs:")
    for doc in docs:
        logging.debug(doc)
    
    
    


if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO)
    logging.info(__file__)
    
    update_vocabularies()
    #update_data()

# [eof]
