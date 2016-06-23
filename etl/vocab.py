import re
import urllib
import pandas as pd
from StringIO import StringIO
import numpy as np
import vocab

def vocabulary(host, apikey, ids):
    lexicon = []
    
    for thisid in ids:
        url = "%s/api/access/datafile/%s?&key=%s&show_entity_ids=true&q=authorName:*" % (host, thisid, apikey)    
	print url
        f = urllib.urlopen(url)
        data = f.read()
        csvio = StringIO(str(data))
        dataframe = pd.read_csv(csvio, sep='\t', dtype='unicode')        
    
        filtercols = []  
        mapping = {}
        for col in dataframe.columns:    
            findval = re.search(r'RUS|EN|ID', col)    
            if findval:            
                mapping[col] = findval.group(0)
                filtercols.append(col)

        vocab = {}
        if filtercols:
            vocab = dataframe[filtercols]
            newcolumns = []
            for field in vocab:
                newcolumns.append(mapping[field])
            vocab.columns = newcolumns
            vocab = vocab.dropna()
            lexicon.append(vocab)        
    
    return pd.concat(lexicon)
