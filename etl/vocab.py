import re
import urllib
import pandas as pd
from StringIO import StringIO
import numpy as np
import vocab
import psycopg2
import psycopg2.extras
import pprint
import collections
import getopt
import ConfigParser

def vocabulary(host, apikey, ids):
    lexicon = []
    
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
            lexicon.append(vocab)        
    
    return pd.concat(lexicon)

def classupdate():
    cparser = ConfigParser.RawConfigParser()
    cpath = "/etc/apache2/rusrep.config"
    cparser.read(cpath)
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (cparser.get('config', 'dbhost'), cparser.get('config', 'dbname'), cparser.get('config', 'dblogin'), cparser.get('config', 'dbpassword'))
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    sql = "select distinct base_year, value_unit, value_label, datatype, histclass1, histclass2, histclass3, histclass4, histclass5, histclass6, histclass7, histclass8, histclass9, histclass10 from russianrepository";
    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [desc[0] for desc in cursor.description]
    finalclasses = []
    if data:
        for valuestr in data:
            classes = {}
            for i in range(len(valuestr)):
                name = sqlnames[i]
                value = valuestr[i]
		#print "%s %s" % (name, value)
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

    sql = "select distinct base_year, value_unit, value_label, datatype, class1, class2, class3, class4, class5, class6, class7, class8, class9, class10 from russianrepository";
    cursor.execute(sql)
    data = cursor.fetchall()
    sqlnames = [desc[0] for desc in cursor.description]
    if data:
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
                name = "histclass%s" % n
                if name not in classes:
                    classes[name] = '.'

            classes['vocabulary'] = 'modern'
            if 'datatype' in classes:
                finalclasses.append(classes)

    return finalclasses

