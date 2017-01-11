#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
VT-06-Jul-2016 latest change by VT
FL-11-Jan-2017
"""

import ConfigParser
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
import re
import urllib

from StringIO import StringIO

#import numpy as np
#import collections
#import getopt
#import pprint
#import vocab


def vocabulary(host, apikey, ids):
    logging.info("%s vocabulary()" % __file__)
    
    lexicon = []
    len_totvocab = 0
    
    for thisid in ids:
        filename = ids[thisid]
        filename = re.sub('.tab', '', filename)
        url = "%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, thisid, apikey)
        f = urllib.urlopen(url)
        data = f.read()
        csvio = StringIO(str(data))
        dataframe = pd.read_csv(csvio, sep='\t', dtype='unicode')
    
        filtercols = []
        mapping = {}
        for col in dataframe.columns:
            findval = re.search(r'RUS|EN|ID|DATATYPE|YEAR|basisyear', col)
            if findval:
                mapping[col] = findval.group(0)
                filtercols.append(col)

        vocab = {}
        if filtercols:
            vocab = dataframe[filtercols]
            newcolumns = []
            for field in vocab:
                value = mapping[field]
                newcolumns.append(value)
            
            vocab.columns = newcolumns
            vocab = vocab.dropna()
            vocab['vocabulary'] = filename
            len_vocab = len(vocab)
            len_totvocab += len_vocab
            lexicon.append(vocab)
        
        logging.info("id: %s, filename: %s, items: %d" % (thisid, filename, len_vocab))
        
    logging.info("lexicon contains %d vocabularies containing %d items in total" % (len(lexicon), len_totvocab))
    # concatenate the vocabularies with pandas
    return pd.concat(lexicon)


def classupdate(cpath):
    logging.info("%s classupdate()" % __file__)
    cparser = ConfigParser.RawConfigParser()
    cparser.read(cpath)
    
    host = cparser.get('config', 'dbhost')
    dbname = cparser.get('config', 'dbname')
    user = cparser.get('config', 'dblogin')
    password = cparser.get('config', 'dbpassword')
    table = "russianrepository"
    
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (host, dbname, user, password)
    logging.debug("conn_string: %s" % conn_string)

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    finalclasses = []
    
    # historic data
    logging.info("fetching historic data from postgresql table %s..." % table)
    historic_columns = "base_year, value_unit, value_label, datatype, histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10"
    sql = "SELECT DISTINCT %s FROM %s" % (historic_columns, table)
    logging.info("sql: %s" % sql)

    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [desc[0] for desc in cursor.description]     # list of the selected column names
    
    
    if data:
        no_dtype = 0
        for valuestr in data:
            classes = {}
            for i in range(len(valuestr)):
                name = sqlnames[i]
                value = valuestr[i]
                #logging.debug("name: %s, value: %s" % (name, value))
                if value:
                     classes[name] = str(value)

            flagvalue = 0
            firstclass = 0
            for n in range(10,1,-1):
                name = "histclass%s" % n
                if name in classes:
                    if classes[name]:
                        flagvalue = 1
                        if not firstclass:
                            firstclass = n

                    if flagvalue == 0:
                        del classes[name]

            # Check comma and add between classes
            for n in range(1,firstclass):
                name = "histclass%s" % n
                if name not in classes:
                    classes[name] = '.'

            classes['vocabulary'] = 'historical'
            if 'datatype' in classes:
                finalclasses.append(classes)
            else:
                no_dtype += 1
        
        logging.info("numbers of historic class records: %d, skipping %d without datatype" % (len(data), no_dtype))
    
    # modern data
    logging.info("fetching modern data from postgresql table %s..." % table)
    modern_columns = "base_year, value_unit, value_label, datatype, class1, class2, class3, class4, class5, class6, class7, class8, class9, class10"
    sql = "SELECT DISTINCT %s FROM %s" % (modern_columns, table)
    logging.info("sql: %s" % sql)
    
    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [desc[0] for desc in cursor.description]     # list of the selected column names
    
    if data:
        no_dtype = 0
        for valuestr in data:
            classes = {}
            for i in range(len(valuestr)):
                name = sqlnames[i]
                value = valuestr[i]
                if value:
                    classes[name] = str(value)

            flagvalue = 0
            firstclass = 0
            for n in range(10,1,-1):
                name = "class%s" % n
                if name in classes:
                    if classes[name]:
                        flagvalue = 1
                        if not firstclass:
                            firstclass = n
                    if flagvalue == 0:
                        del classes[name]

            # Check comma and add between classes
            for n in range(1,firstclass):
                name = "class%s" % n
                if name not in classes:
                    classes[name] = '.'

            classes['vocabulary'] = 'modern'
            if 'datatype' in classes:
                finalclasses.append(classes)
            else:
                no_dtype += 1
        
        logging.info("numbers of modern class records: %d, skipping %d without datatype" % (len(data), no_dtype))
        
    logging.info("total number of class records: %d" % len(finalclasses))
    return finalclasses

# [eof]
