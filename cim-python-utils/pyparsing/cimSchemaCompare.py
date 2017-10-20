#!/usr/bin/python
import json
import sys

def printtable(tablename,tdict):
        print 'table',tablename, 'primary key',tdict[tablename]['primary key'] 
        print 'columns',tdict[tablename]['columns']
        print 'constraints',tdict[tablename]['constraints']
        print 'foreign keys',tdict[tablename]['foreign keys']

def printtables(tdict):
    for tablename in sorted(tdict.keys()):
        printtable(tablename,tdict)

def compschemas(sch1,sch2):
    seq1 = sorted(sch1['sequences'].keys())
    seq2 = sorted(sch2['sequences'].keys())

    print '--------------Comparing', len(seq1),len(seq2),'sequences'
    for seqid in seq1: 
        if seqid not in seq2: print sch1['sequences'][seqid]['id'], 'missing'
        
    idx1,idx2 = sorted(sch1['indices'].keys()),sorted(sch2['indices'].keys())
    print '--------------Comparing', len(idx1),len(idx2),'indices'
    for idxid in idx1: 
        if idxid not in idx2: print sch1['indices'][idxid]['id'], 'missing'

    idx1,idx2 = sorted(sch1['tables'].keys()),sorted(sch2['tables'].keys())
    print '--------------Comparing', len(idx1),len(idx2),'tables'
    for idxid in idx1: 
        if idxid not in idx2: 
            print idxid, 'missing'#sch1['tables'][idxid].keys()#['id']

    idx1,idx2 = sorted(sch1['constraints'].keys()),sorted(sch2['constraints'].keys())
    print '--------------Comparing', len(idx1),len(idx2),'constraints'
    for idxid in idx1: 
        if idxid not in idx2: 
            print idxid, 'missing'#sch1['tables'][idxid].keys()#['id']

    idx1,idx2 = sorted(sch1['fkeys'].keys()),sorted(sch2['fkeys'].keys())
    print '--------------Comparing', len(idx1),len(idx2),'fkeys'
    for idxid in idx1: 
        if idxid not in idx2: 
            print idxid, 'missing'#sch1['tables'][idxid].keys()#['id']

    idx1,idx2 = sorted(sch1['pkeys'].keys()),sorted(sch2['pkeys'].keys())
    print '--------------Comparing', len(idx1),len(idx2),'pkeys'
    for idxid in idx1: 
        if idxid not in idx2: 
            print idxid, 'missing'#sch1['tables'][idxid].keys()#['id']

def loadtables(fromfile):
    with open(fromfile) as data_file:    
        data = json.load(data_file)
    tabledict={}
    pkeydict={}
    fkeydict={}
    constraintsdict={}
    indexdict={}
    sequencedict={}
    for table in data['tables']: 
        tabledict[table[0]]={'columns':table[1],'primary key':[],'constraints':[],'foreign keys':[]}
    for pkey in data['pkeys']: 
        tabledict[pkey[0]]['primary key']=pkey[1]
        pkeydict[pkey[0]]=pkey[1]
    for fkey in data['fkeys']: 
        tabledict[fkey[0]]['foreign keys'].append(fkey[1])
        fkeydict[fkey[0]]=fkey[1]
    for constraint in data['constraints']: 
        tabledict[constraint[0]]['constraints'].append({'id':constraint[1],'type':constraint[2],'expression':constraint[3]})
        constraintsdict[constraint[1]]={'table':constraint[0],'type':constraint[2],'expression':constraint[3]}
    for idx in data['indices']:
        indexdict[idx[0]]={'id':idx[0],'table':idx[1],'nfields':idx[2],'fields': idx[3]}
    for seq in data['sequences']:
        sequencedict[seq[0]]= {'id':seq[0]}
    print len(data['tables']),'tables',len(data['pkeys']),'pkeys',len(data['constraints']), 'constraints',len(data['fkeys']),'fkeys',len(data['indices']),'indices',len(data['sequences']),'sequences'
    #print 'dictionaries', len(tabledict),'tables',len(pkeydict),'pkeys',len(constraintsdict), 'constraints',len(fkeydict),'fkeys',len(indexdict),'indices',len(sequencedict),'sequences'
    return {'tables':tabledict,'indices':indexdict,'pkeys':pkeydict,'constraints':constraintsdict,'sequences':sequencedict,'fkeys':fkeydict}

def main():
    f1, f2 = sys.argv[1] , sys.argv[2]
    print 'Loading schema extract JSON file:',f1,
    schema1= loadtables(f1)
    print 'Loading schema extract JSON files:',f2,
    schema2= loadtables(f2)
    print
    print '--------------'
    print 'lookup: ' , f1, '---keys in---', f2
    print '--------------'
    print
    compschemas(schema1, schema2)
    print '--------------'
    print 'lookup: ' , f2, '---keys in---', f1
    print '--------------'
    print
    compschemas(schema2, schema1)
    print
    print '--------------done'
    
        
if __name__ == '__main__':
    main()
