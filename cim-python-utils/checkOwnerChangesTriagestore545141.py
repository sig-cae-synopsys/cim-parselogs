#!/usr/bin/python
import json
from pprint import pprint

with open('TS4-2.json') as data_file:    
    data = json.load(data_file)
tst=data["TS4"]
for t in tst:
	for ts in t['triageStates']:
		if 'owner' in ts :
			print  t['cid'], t['mergeKey'], ts['dateCreated'], ts['userCreated'], ts['userCreatedLdapServerName'], ts['owner'], ts['ownerLdapServerName']