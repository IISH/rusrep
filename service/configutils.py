#!/usr/bin/python

# Copyright (C) 2016 International Institute of Social History.
# @author Vyacheslav Tykhonov <vty@iisg.nl>
#
# This program is free software: you can redistribute it and/or  modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the GNU Affero General Public License in all respects
# for all of the code used other than as permitted herein. If you modify
# file(s) with this exception, you may extend this exception to your
# version of the file(s), but you are not obligated to do so. If you do not
# wish to do so, delete this exception statement from your version. If you
# delete this exception statement from all source files in the program,
# then also delete it in the license file.

# VT-07-Jul-2016 latest change by VT
# FL-08-Aug-2017 latest change

# future-0.16.0 imports for Python 2/3 compatibility
from __future__ import ( absolute_import, division, print_function, unicode_literals )
from builtins import ( ascii, bytes, chr, dict, filter, hex, input, int, list, map, 
    next, object, oct, open, pow, range, round, super, str, zip )

from six.moves import configparser, urllib

import os
import json
import pandas as pd
import re
import sys
import ldap

from flask import Flask, request, redirect


class Configuration:
    def __init__( self ):
        RUSSIANREPO_CONFIG_PATH = os.environ[ "RUSSIANREPO_CONFIG_PATH" ]
        #print( "RUSSIANREPO_CONFIG_PATH: %s" % RUSSIANREPO_CONFIG_PATH )
        
        config_path = RUSSIANREPO_CONFIG_PATH
        if not os.path.isfile( config_path ):
            print( "in %s" % __file__ )
            print( "configpath %s FILE DOES NOT EXIST" % config_path )
            print( "EXIT" )
            sys.exit( 1 )
        
        self.config = {}
        #configparser = ConfigParser.RawConfigParser()  # Py2 only
        config_parser = configparser.RawConfigParser()
        config_parser.read( config_path )
        path_items = config_parser.items( "config" )
        for key, value in path_items:
            #print( "key: |%s|, value: |%s|" % ( key, value ) )
            self.config[ key ] = value
        
        # Extract host for Dataverse connection
        findhost = re.search('(http\:\/\/|https\:\/\/)(.+)', self.config['dataverse_root'])
        if findhost:
            self.config['hostname'] = findhost.group(2)
        self.config['remote'] = ''
    
    
    # Primary URL validation 
    def is_valid_uri(self, url):
        regex = re.compile(
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return url is not None and regex.search(url)
    
    
    # Validate url for some forbidden words to prevent SQL and shell injections
    def are_parameters_valid(self, actualurl):
        # primary check
        if not self.is_valid_uri(actualurl):
            return False
        
        # complex validation
        self.error = False
        self.config['message'] = ''
        # Web params check
        semicolon = re.split(FORBIDDENPIPES, actualurl)
        if len(semicolon) <= 1:
            cmd = 'on'
        else:
            self.error = True
            self.config['message'] = ERROR1
            return False
        
        # Check for vocabulary words in exploits
        threat = re.search(r'%s' % FORBIDDENURI, actualurl)
        
        if threat:
            self.error = True
            self.config['message'] = ERROR2
            return False
        
        return True



class DataFilter(Configuration):
    def __init__(self, params):
        Configuration.__init__(self)
        self.datafilter = {}
        self.datafilter['selected'] = ''
        self.datafilter['classification'] = 'historical'
        for item in params:
            self.datafilter[item] = params.get(item)
    
    
    def countries(self):
        ctrparam = 'ctrlist'
        if ctrparam in self.datafilter:
            customcountrycodes = ''
            tmpcustomcountrycodes = self.datafilter[ctrparam]
            if tmpcustomcountrycodes:
                c = tmpcustomcountrycodes.split(',')
                for ids in sorted(c):
                    if ids:
                        customcountrycodes = str(customcountrycodes) + str(ids) + ','
                customcountrycodes = customcountrycodes[:-1]
                if len(customcountrycodes):
                    countriesNum = len(customcountrycodes.split(','))
                    categoriesMax = 5 #self.config['categoriesMax']
                    if countriesNum < categoriesMax:
                        if countriesNum >= 1:
                            categoriesMax = countriesNum
                            self.datafilter['categoriesMax'] = categoriesMax
                
                self.datafilter['ctrlist'] = tmpcustomcountrycodes
            else:
                self.datafilter['ctrlist'] = ''
        return self.datafilter['ctrlist']


    def parameters(self):
        return self.datafilter


    def maxyear(self):
        if 'before' in self.datafilter:
            return int(self.datafilter['before'])
        else:
            return int(self.config['maxyear'])


    def minyear(self):
        if 'after' in self.datafilter:
            return int(self.datafilter['after'])
        else:
            return int(self.config['minyear'])


    def classification(self):
        if 'classification' in self.datafilter:
            return self.datafilter['classification']
        else:
            return 'modern'


    def selected(self):
        return self.datafilter['selected']


    def countryfilter(self):
        if 'name' in self.datafilter:
            return self.datafilter['name']
        else:
            return False


    def showframe(self):
        if 'action' in self.datafilter:
            if self.datafilter['action'] == 'geocoder':
                return True


    def allsettings(self):
        return self.config['categoriesMax']



# OpenLDAP class to implement authentification services
class OpenLDAP(Configuration):
    def __init__(self):
        Configuration.__init__(self) 
        self.ldapconn = ldap.open(host=self.config['ldapserver'], port=int(self.config['ldapport']))
        self.ldapconn.set_option(ldap.OPT_REFERRALS, 0)
        self.ldapconn.set_option(ldap.OPT_PROTOCOL_VERSION, 3)


    def authentificate(self, username, password):
        Base = "cn=%s,ou=users,%s" % (username, self.config['ldapbase'])
        self.ldapconn.simple_bind_s(Base, password)
        user_ids = self.ldapconn.search(Base, ldap.SCOPE_SUBTREE, "cn="+username, None)
        result_type, user_data = self.ldapconn.result(user_ids, 0)
        return user_data


    def searchuser(self, username):
        Base = "cn=admin,%s" % self.config['ldapbase']
        Baseusers = "ou=users,%s" % self.config['ldapbase']
        self.ldapconn.simple_bind_s(Base, self.config['ldapsecret'])
        user_ids = self.ldapconn.search(Baseusers, ldap.SCOPE_SUBTREE, "cn="+username, None)
        result_type, user_data = self.ldapconn.result(user_ids, 0)
        return user_data



class Utils(Configuration):
    def loadjson(self, apiurl):
        jsondataurl = apiurl
        
        req = urllib2.Request(jsondataurl)
        opener = urllib2.build_opener()
        f = opener.open(req)
        #dataframe = simplejson.load(f)
        dataframe = json.load(f)
        return dataframe


    def webmapper_geocoder(self):
        coder = {}
        #config = configuration()
        apiroot = self.config['apiroot'] + "/collabs/static/data/" + self.config['geocoder'] + ".json"
        print( apiroot )
        geocoder = self.loadjson(apiroot)
        for item in geocoder:
            if item['ccode']:
                coder[int(item['ccode'])] = item['Webmapper numeric code']
        return coder


    def findpid(self, handle):
        ids = re.search(r'hdl\:\d+\/(\w+)', handle, re.M|re.I)
        (pid, fileid, revid, cliohandle, clearpid) = ('', '', '', '', '')
        if ids:
            clearpid = ids.group(0)
            pid = ids.group(1)
            files = re.search(r'hdl\:\d+\/\w+\:(\d+)\:(\d+)', handle, re.M|re.I)
            if files:
                fileid = files.group(1)
                revid = files.group(2)
                cliohandle = pid + str(fileid) + '_' + str(revid)
        return (pid, revid, cliohandle, clearpid)


    def pidfrompanel(self, pid):
        # Check Panel
        pids = []
        (thispid, revpid, cliopid, pidslist) = ('', '', '', '')
        match = re.match(r'Panel\[(.+)', pid)
        if match:
            pidstr = match.group(1)
            # Remove quotes
            clearpids = re.sub('["\']+', '', pidstr)
            ptmpids = clearpids.split(',')
            for fullhandle in ptmpids:            
                (thispid, revpid, cliopid, clearpid) = findpid(fullhandle)            
                pids.append(clearpid)
            pidslist = pidslist + thispid + ','
        else:
            pids.append(pid)
            pidslist = pid
        
        pidslist = pidslist[0:-1]
        return (pids, pidslist)


    def load_dataverse(self, apiurl):
        dataframe = self.loadjson(apiurl)
        
        info = []
        # Panel
        panel = {}
        panel['url'] = 'url'
        panel['name'] = 'Panel data'
        panel['topic'] = 'Topic'
        panel['pid'] = 'Panel'
        panel['citation'] = 'citation'
        info.append(panel)
        
        for item in dataframe['data']['items']:
            datasets = {}
            datasets['url'] = item['url']
            datasets['pid'] = item['global_id']
            datasets['name'] = item['name']
            datasets['topic'] = item['description'] 
            datasets['citation'] = item['citation']
            info.append(datasets)
        
        return info


    def load_fullmetadata(self, dataset):
        data = {}
        config = configuration()
        if dataset:
            url = config['dataverse_root'] + '/api/search?q=' + dataset + "&key=" + config['key'] + "&per_page=1000"
        result = json.load(urllib2.urlopen(url))
        try:
            data = result['data']['items']
        except:
            data = {}
        
        return data


    def load_metadata(self, dataset):
        config = configuration()
        (pid, fileid, cliohandle, clearpid) = findpid(dataset)
        
        data = {}
        if pid:
            query = pid
            apiurl = config['dataverse_root'] + "/api/search?q=" + query + '&key=' + config['key'] + '&type=dataset&per_page=1000'
            data = load_dataverse(apiurl)
        return (data, pid, fileid, cliohandle)


    def get_citation(self, citejson):    
        metadata = {}    
        latestversion = ''
        title = ''
        for item in citejson:
            if not latestversion:
                latestversion = item
        
        cite = latestversion['metadataBlocks']['citation']
        notfound = 0
        for meta in cite:
            for item in cite[meta]:
                #print item
                try:
                    typeName = item['typeName']
                    if typeName == 'author':
                        value = item['value']
                        try:
                            metadata['org'] = value[0]['authorAffiliation']['value']
                        except:
                            notfound = 1
                        try:
                            metadata['authors'] = value[0]['authorName']['value']
                        except:
                            notfound = 1
                    
                    if typeName == 'title':
                        value = item['value']
                        title = value
                except:
                    notfound = 0 
        
        citation = ''
        for item in sorted(metadata):        
            citation = citation + metadata[item] + ', '
        citation = citation[:-2]
        return (title, citation)


    def dataverse2indicators(self, branch):
        config = configuration()
        rows = 10
        start = 0
        page = 1
        condition = True # emulate do-while
        datasets = {}
        while (condition):
            url = self.config['dataverse_root'] + '/api/search?q=*' + "&key=" + config['key'] + "&start=" + str(start) 
            #+ "&type=dataset"
            if branch:
                url = url + "&subtree=" + branch
            data = json.load(urllib2.urlopen(url))
            total = data['data']['total_count']
            for i in data['data']['items']:
                url = i['url']
                handles = re.search(r'(\d+)\/(\w+)', url, re.M|re.I)
                handle = ''
                if handles:
                    handle = 'hdl:' + handles.group(1) + '/' + handles.group(2)
                handle = handle + ':114:115'
                
                datasets[i['name']] = handle
            
            start = start + rows
            page += 1
            condition = start < total
        
        return datasets


    def graphlinks(self, handle):
        links = {}
        links['chartlib'] = "/collabs/chartlib?start=on" + handle + "&logscale="
        links['barlib'] = "/collabs/graphlib?start=on&arr=on" + handle
        links['panellib'] = '/collabs/panel?start=on&aggr=on&hist=' + handle
        links['treemaplib'] = '/collabs/treemap?' + handle
        
        return links

