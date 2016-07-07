#!/usr/bin/python

import json
from pymongo import MongoClient
import openpyxl
from openpyxl.cell import get_column_letter
import re

def preprocessor(datakey):
    dataset = []
    lexicon = {}
    lands = {}
    if datakey:
	clientcache = MongoClient()
        dbcache = clientcache.get_database('datacache')
        result = dbcache.data.find({"key": datakey })
        for rowitem in result:
            del rowitem['key']
            del rowitem['_id']
	    for item in rowitem['data']:
	        dataitem = item
	        if 'path' in item:
		    classes = item['path']
		    del item['path']
		    clist = {}
		    for classname in classes:
		        dataitem[classname] = classes[classname]

	        itemlexicon = dataitem
		lands = {}
		if 'ter_code' in itemlexicon:
		    lands[itemlexicon['ter_code']] = itemlexicon['total']
		    del itemlexicon['ter_code']
                if 'total' in itemlexicon:
                    del itemlexicon['total']
	
		lexkey = json.dumps(itemlexicon)
		itemlexicon['lands'] = lands
		if lexkey in lexicon:
		    currentlands = lexicon[lexkey]
		    for item in lands:
			currentlands[item] = lands[item]
		    lexicon[lexkey] = currentlands
		else:
		    lexicon[lexkey] = lands

		dataset.append(dataitem)
    return lexicon

def aggregate_dataset(fullpath, result):
    wb = openpyxl.Workbook(encoding='utf-8')
    ws = wb.get_active_sheet()
    ws.title = "Dataset"

    i = 9
    for itemchain in result:
        j = 0
        if i == 9:
    #        ws.column_dimensions["C"].width = 80
    #        ws.column_dimensions["D"].width = 20
    #        ws.column_dimensions["O"].width = 100
    #        ws.column_dimensions["P"].width = 100
            chain = json.loads(itemchain)
            terdata = result[itemchain]
            for name in sorted(chain):
                c = ws.cell(row=i, column=j)
                c.value = name
                j+=1
            for ter_code in terdata:
                c = ws.cell(row=i, column=j)
                ter_value = terdata[ter_code]
                ter_value = re.sub(r'\.0', '', str(ter_value))
                c.value = ter_code
                j+=1
	    i+=1

	if itemchain:
	    j = 0
	    chain = json.loads(itemchain)
	    terdata = result[itemchain]
	    for name in sorted(chain):
	        c = ws.cell(row=i, column=j)
	        c.value = chain[name] 
		j+=1
	    for ter_code in terdata:
                c = ws.cell(row=i, column=j)
		ter_value = terdata[ter_code]
		ter_value = re.sub(r'\.0', '', str(ter_value))
                c.value = ter_value
		j+=1
	    i+=1

    wb.save(fullpath)
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

