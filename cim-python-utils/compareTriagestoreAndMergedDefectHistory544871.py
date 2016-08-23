#!/usr/bin/python
import json
#from pprint import pprint

with open('cid-20132-triagestore.json') as data_file:    
    data = json.load(data_file)
tst=data["TS20132"]
for ts in tst['triageStates']:
#    print ts
#    if 'owner' in ts :
		print  ts['dateCreated'], ts['userCreated'], ts['userCreatedLdapServerName']

with open('cid-20132-getmergeddefecthistory.json') as data_file1:    
    data1 = json.load(data_file1)
tst1=data1["MDH20132"]
for dco in tst1:
#    print ts
#    if 'owner' in ts :
        print   dco['dateModified'], dco['userModified']
