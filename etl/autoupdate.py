#!/usr/bin/python

# VT-07-Jul-2016 latest change by VT
# FL-19-Dec-2016

from __future__ import absolute_import

import logging
import os
import sys

from flask import Flask, Response, request
import requests
from twisted.web import http
import urllib
import json
import simplejson
import tables
import urllib2
import glob
import csv
import xlwt
import psycopg2
import psycopg2.extras
import pprint
import collections
import getopt
import ConfigParser
import re
import os
import sys
import unittest
from vocab import vocabulary, classupdate
from pymongo import MongoClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname("__file__"), './')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname("__file__"), '../')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname("__file__"), '../service')))
#print(sys.path)
#print("pwd:", os.getcwd())

from xlsx2csv import xlsx2csv

#from cliocore.configutils import Configuration, Utils, DataFilter
from service.configutils import Configuration, Utils, DataFilter

from dataverse import Connection


def loadjson(apiurl):
    logging.debug("loadjson() %s" % apiurl)
    jsondataurl = apiurl

    req = urllib2.Request(jsondataurl)
    opener = urllib2.build_opener()
    f = opener.open(req)
    dataframe = simplejson.load(f)
    return dataframe


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


def alldatasets():
    logging.debug("alldatasets()")
    cursor = connect()
    clioinfra = Configuration()
    host = clioinfra.config['dataverseroot']
    dom = re.match(r'https\:\/\/(.+)$', host)
    if dom:
        host = dom.group(1)
    connection = Connection(host, clioinfra.config['ristatkey'])
    dataverse = connection.get_dataverse('RISTAT')
    settings = DataFilter('')
    papers = []
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


def content():
    logging.debug("content()")
    cursor = connect()
    clioinfra = Configuration()
    #logging.debug(clioinfra.config)
    
    host = "datasets.socialhistory.org"
    connection = Connection(host, clioinfra.config['ristatkey'])
    dataverse = connection.get_dataverse('RISTAT')
    settings = DataFilter('')
    papers = []
    ids = {}
    
    for item in dataverse.get_contents():
        handle = str(item['protocol']) + ':' + str(item['authority']) + "/" + str(item['identifier'])
        logging.debug("handle: %s" % handle)
        #if handle not in [clioinfra.config['ristatdocs'], clioinfra.config['ristatvoc']]:
        if handle in clioinfra.config['ristatvoc']:
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
                filepath = "%s/%s" % (clioinfra.config['tmppath'], paperitem['name'])
                csvfile = "%s/%s.csv" % (clioinfra.config['tmppath'], paperitem['name'])
                print url
                f = urllib.urlopen(url)
                fh = open(filepath, 'wb')
                fh.write(f.read())
                fh.close()

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(__file__)
    
    
    #docs = alldatasets()

    # Update vocabularies
    logging.debug("Update vocabularies...")
    (docs, ids) = content()
    
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

    if ids:
        db = client.get_database(dbname)
        db.data.drop()      # remove the data; same as: db.drop_collection(dbname)
        
        bigvocabulary = vocabulary(host, apikey, ids)
        data = json.loads(bigvocabulary.to_json(orient='records'))
        
        logging.debug("processing %d items..." % len(data))
        for item in data:
            #logging.debug(item)
            if 'YEAR' in item:
                item['YEAR'] = re.sub(r'\.0', '', str(item['YEAR']))
            if 'basisyear':
                item['basisyear'] = re.sub(r'\.0', '', str(item['basisyear']))
        
        result = db.data.insert(data)
        #print bigvocabulary.to_json(orient='records')

    logging.debug("inserting data")
    classdata = classupdate()
    result = db.data.insert(classdata)
    
    logging.debug("clearing cache")
    db = client.get_database('datacache')
    db.data.drop()      # remove the data; same as: db.drop_collection(dbname)
    
# [eof]
