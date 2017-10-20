#
import sys
import os
import glob
import json
import pytz
from dateutil import parser
import time
from datetime import datetime, timedelta
from distutils.util import grok_environment_error
from lib2to3.pgen2.tokenize import group
from test.test_datetime import OTHERSTUFF

try:
    from pyparsing import (Literal, CaselessLiteral, Word, delimitedList, Optional, 
        Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, 
        ZeroOrMore, restOfLine, Keyword, upcaseTokens, Suppress, OneOrMore,
        CharsNotIn,StringEnd)
except ImportError:
    import sys
    sys.exit("pyparsing is required")

DASH=Literal("-")
LCOLON = Literal(":")
COLON = Suppress(LCOLON)

num = Word(nums)
dnum = Combine(num+Literal('.')+num)
wd=Word(alphas)
wd1= Combine( Literal('<') + wd + Literal('>'))
wd2= Combine(wd+Literal('_')+num)
wd3=( wd2 | wd1 )
timezone=(Literal(' CST:') | Literal(' IST:'))
datetz=Combine(num+DASH+num+DASH+num+Literal(' ')+num+LCOLON+num+LCOLON+num+timezone) #wd+COLON
typ=oneOf('ERROR DETAIL STATEMENT LOG CONTEXT WARNING')+COLON

logline=Suppress(Literal('duration: '))+dnum+Suppress(Literal('ms')+( Literal('execute ') | Literal('parse ') | Literal('bind ') ))+ (wd3|Literal('<unnamed>') )+COLON+restOfLine() 
logline2=Suppress(Literal('duration: '))+dnum+Suppress(Literal('ms')+( Literal('statement') | Literal('fastpath function call') ))+COLON+restOfLine() 


#2017-08-24 19:07:44 IST: LOG:  duration: 1203.018 ms  fastpath function call: "lowrite" (OID 955)
#2017-08-24 19:07:44 IST: LOG:  duration: 6139.761 ms  statement: COMMIT


#2017-09-11 10:23:07 CST: ERROR:  duplicate key value violates unique constraint "uk_9qhla3rx07qakft8wroj76xp"
#2017-09-11 10:23:07 CST: DETAIL:  Key (md5)=(fa0563e4726caadf3d330929c5a3c04c) already exists.
#2017-09-11 10:23:07 CST: STATEMENT:  insert into checker_properties (checker_category_id, checker_type_id, cwe, impact, quality, security, test, md5, rule_strength, id) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
#2017-09-11 10:24:01 CST: LOG:  duration: 7620.332 ms  execute S_121: select function0_.id as col_0_0_, function0_.mangled_name_md5 as col_1_0_ from function function0_
#2017-09-11 10:24:01 CST: LOG:  duration: 1908.021 ms  execute S_56: insert into function (display_name, html_display_name, language, mangled_name, mangled_name_md5, merge_name, search_name, simple_name, id) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
#2017-09-11 10:24:01 CST: DETAIL:  parameters: $1 = 'com.taobao.inventory.core.service.trade.impl.InvFxTradeOccupyServiceImpl.occupyInventory(com.taobao.inventory.base.innerdomain.dto.InvTradeOccupyWrapDTO)', $2 = '<span class="func-container">com.&#8203;taobao.&#8203;inventory.&#8203;core.&#8203;service.&#8203;trade.&#8203;impl.&#8203;</span><span class="func-container-elided">&#x02026;</span><span class="func-name">InvFxTradeOccupyServiceImpl.&#8203;occupyInventory</span>(<span class="func-args">com.&#8203;taobao.&#8203;inventory.&#8203;base.&#8203;innerdomain.&#8203;dto.&#8203;InvTradeOccupyWrapDTO</span><span class="func-args-elided">&#x02026;</span>)', $3 = 'JAVA', $4 = 'com.taobao.inventory.core.service.trade.impl.InvFxTradeOccupyServiceImpl.occupyInventory(com.taobao.inventory.base.innerdomain.dto.InvTradeOccupyWrapDTO)', $5 = '588fb78aecf7380b4757aee3c3ef5d9e', $6 = 'com.taobao.inventory.core.service.trade.impl.InvFxTradeOccupyServiceImpl.occupyInventory(com.taobao.inventory.base.innerdomain.dto.InvTradeOccupyWrapDTO)', $7 = 'InvFxTradeOccupyServiceImpl.occupyInventory', $8 = 'occupyInventory', $9 = '8063458'

logentry=  (datetz+ typ + (logline |logline2 |restOfLine()) | restOfLine())    

def days_hours_minutes(td):
    return td.days, td.seconds//3600, (td.seconds//60)%60, td.seconds%60

def sec_ts(ts):
    #t1 =parser.parse(ts)
    #'2017-09-11 10:23:07 CST:'
    #format='%Y-%m-%d %H:%M:%S %Z:'# https://bugs.python.org/issue22377  %Z in strptime doesn't match EST and others (only GMT and UTC)
    #format='%Y-%m-%d %H:%M:%S %z:'# ValueError: 'z' is a bad directive in format '%Y-%m-%d %H:%M:%S %z:' ???
    t1=datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
    t2= datetime.fromtimestamp(0)
    td=(t1-t2).total_seconds()
    return td

def main():
    columns=[]
    rows=0
    data={}
    nextevent=[]
    dp=[]
    mintstamp=0
    mintz=''
    maxtstamp=0
    maxtz=''
    firsttz=''
    firstts=0
    timetz=''
    timestamp=0
    tss=''
    result=[]
    #-----------stats:
    totals={ "no": 0, "min": 999999999, "max": 0, "total": 0 } 
    execs={}
    queries={}
    dur=0
    exe=''
    qry=''
    #-----------error stats
    errors={}
    err=''
    etotals=0
    filterederrors=0
    errdict={}
    #----------
    for root, dirs, files in os.walk(sys.argv[1]):
        for file in files:
            if file.startswith("postgresql-2017-10-05_073853"):  #-2017-09-11_102307 -2017-09-09_191118 -2017-08-24_190211.log -2017-09-28_124843 -2017-10-05_073853
                path=os.path.join(root, file)
                print file,len(data)
                for myline in open(path):
                    try:
                        result = logentry.parseString(myline)
                        rows+=1
                        if len(result)>1:
                            timetz=result[0][:19]
                            timestamp=sec_ts(timetz)*1000
                            tss= "{0:.0f}".format(timestamp)
                            typ=result[1]
                            if typ=='LOG':    
                                if len(nextevent)>3: 
                                    if nextevent[2] == 'ERROR':
                                        #print len(nextevent), nextevent
                                        err=nextevent[3].strip()
                                        if len(nextevent) > 5:
                                            stmt=nextevent[5]["STATEMENT"].strip()
                                        else:
                                            #print len(nextevent),nextevent
                                            if nextevent[4].has_key('CONTEXT'):
                                                stmt='CONTEXT:'+nextevent[4]["CONTEXT"][0].strip()
                                            else:
                                                if nextevent[4].has_key('STATEMENT '):
                                                    stmt='STATEMENT:'+nextevent[4]["STATEMENT"].strip()                                            
                                        if stmt.startswith('insert into checker') or stmt.startswith('insert into event_tag'): 
                                            #print "filtered error at: ",nextevent[5]["STATEMENT"]
                                            filterederrors +=1                      
                                        else:
                                            if not errdict.has_key(err):
                                                errdict[err]={stmt:1}
                                            else:
                                                if not errdict[err].has_key(stmt):
                                                    errdict[err]={stmt:1}
                                                else:
                                                    errdict[err][stmt]=errdict[err][stmt]+1
                                            #--------etotals stats
                                            etotals+=1
                                            #--------errors stats
                                            if not errors.has_key(err):
                                                errors[err]=0 
                                            errors[err]=errors[err]+1
                                            #--------errors stats                                    
                                            data[nextevent[0]]=nextevent 
                                    else:
                                        data[nextevent[0]]=nextevent 
                                if len(result) > 3:
                                    dur=float(result[2])
                                    #--------totals stats
                                    totals["no"]=totals["no"]+1
                                    totals["total"]=totals["total"]+dur
                                    if totals["min"] == 0 or totals["min"] > dur:
                                        totals["min"] = dur
                                    if totals["max"] < dur:
                                        totals["max"] = dur
                                    if len(result) > 4: 
                                        exe=result[3]
                                        qry=result[4]
                                    else:
                                        exe='undef'
                                        qry=result[3]
                                        
                                    #--------executor stats
                                    if not execs.has_key(exe):
                                        execs[exe]={ "no": 0, "min": 999999999, "max": 0, "total": 0 } 
                                    execs[exe]["no"]=execs[exe]["no"]+1
                                    execs[exe]["total"]=execs[exe]["total"]+dur
                                    if execs[exe]["min"] == 0 or execs[exe]["min"] > dur:
                                        execs[exe]["min"] = dur
                                    if execs[exe]["max"] < dur:
                                        execs[exe]["max"] = dur
                                    #--------executor stats
                                    if not queries.has_key(qry):
                                        queries[qry]={ "no": 0, "min": 999999999, "max": 0, "total": 0 } 
                                    queries[qry]["no"]=queries[qry] ["no"]+1
                                    queries[qry]["total"]=queries[qry]["total"]+dur
                                    if queries[qry]["min"] == 0 or queries[qry]["min"] > dur:
                                        queries[qry]["min"] = dur
                                    if queries[qry]["max"] < dur:
                                        queries[qry]["max"] = dur
                                    #--------
                                    nextevent=[timestamp,timetz,typ,dur,exe,[qry]]
                                else:
                                    #print len(result),result
                                    if len(result) > 2:
                                        nextevent=[timestamp,timetz,typ,result[2].strip()]
                                    else:
                                        nextevent=[]
                                        

                            if typ=='ERROR':
                                if len(nextevent)>5: 
                                    if nextevent[2] == 'ERROR':
                                        stmt=nextevent[5]["STATEMENT"].strip()
                                        if stmt.startswith('insert into checker') or stmt.startswith('insert into event_tag'): 
                                            #print "filtered error at: ",nextevent[5]["STATEMENT"]
                                            filterederrors +=1                      
                                        else:
                                            err=nextevent[3].strip()
                                            if not errdict.has_key(err):
                                                errdict[err]={stmt:1}
                                            else:
                                                if not errdict[err].has_key(stmt):
                                                    errdict[err]={stmt:1}
                                                else:
                                                    errdict[err][stmt]=errdict[err][stmt]+1
                                            #--------etotals stats
                                            etotals+=1
                                            #--------errors stats
                                            if not errors.has_key(err):
                                                errors[err]=0 
                                            errors[err]=errors[err]+1
                                            #--------errors stats                                    
                                            data[nextevent[0]]=nextevent 
                                    else:
                                        data[nextevent[0]]=nextevent 
                                        
                                err=result[2]
                                nextevent=[timestamp,timetz,typ,err]
                            if typ=='STATEMENT':
                                nextevent.append({'STATEMENT':result[2]})
                            if typ=='DETAIL':
                                nextevent.append({'DETAIL':[result[2]]})
                            if typ=='CONTEXT':
                                nextevent.append({'CONTEXT':[result[2]]})
                        else:
                            if len(nextevent) < 7:
                                if len(nextevent) > 5 :
                                    #print nextevent
                                    if isinstance(nextevent[5], list):
                                        nextevent[5].append(result[0])
                                #else:
                                    #print nextevent
                            else:
                                nextevent[6]['DETAIL'].append(result[0])
                    except ParseException as x:
                        print "No Match: {0}==>{1}".format(str(x), myline)
                        #sys.exit()
    print 'finished reading {0} - {1}'.format(mintz,maxtz)
    fout = open(sys.argv[2], 'w')
    fout.write("var postgresd=[") 
    dk=sorted(data.keys())
    for d in dk:
        r=data[d]
        ts= '{0:d}'.format(int(d))
        tz="'"+r[1]+"'"
        typ=r[2]
        dline=ts+', '+tz
        dline+=", '"+typ+"'"
        if typ == 'LOG':
            if len(r)>5:
                if isinstance(r[3],str):
                    print r
                else:
                    dline+=", {0:.3f}".format(r[3])
                    dline+=", '{0}'".format(r[4])            
                    dline+=",  {0} ".format(json.dumps(r[5]))
            else:
                #print len(r),r
                dline+=", 0.00"
                dline+=", '{0}'".format(r[3])            
                if len(r)>4:
                    dline+=", {0} ".format(json.dumps(r[4]))
            #-------ommit optional detail lines
            #if len(r)>6:
                #print",  {0} ".format(json.dumps(r[6]))                            
        else:
            if typ == 'ERROR':
                dline+=", '"+r[3]+"'"
                if len(r)>5:
                    ejson={'DETAIL':r[4]['DETAIL'],'STATEMENT':r[5]['STATEMENT']}
                else:
                    #print len(r),r
                    if r[4].has_key('CONTEXT'):
                        ejson={'DETAIL':'','CONTEXT':r[4]['CONTEXT']}
                    else:
                        if r[4].has_key('STATEMENT'):
                            ejson={'DETAIL':'','STATEMENT':r[4]['STATEMENT']}
                        else:
                            print r[4].keys()
                        
                dline+=", {0} ".format(json.dumps(ejson))         

        #fout.write('[{0}],\n'.format(json.dumps(r)))  
        fout.write('[{0}],\n'.format(dline))  
    fout.write('[]];\n') 
    #------------write stats data
    e1=sorted(errors, key=errors.get, reverse= True)
    s1=sorted(queries.items(), key=lambda x:x[1]['total'],reverse=True)
    s2=sorted(queries.items(), key=lambda x:x[1]['no'],reverse=True)
    #print errdict
    pstats={'totals': totals,'queries_by_total':s1,'queries_by_occurrence':s2,'errors_filtered':filterederrors,'etotals':etotals,'errors':e1,'errors_dict':errdict}
    fout.write('var querystats={0};\n'.format(json.dumps(pstats)))    
    fout.close()
    print 'finished writing\n{0} datapoints'.format(len(dk))  
    print "Error stats: #:{0:,} errors:{1:,} filtered:{2:,} ".format(etotals,len(errors),filterederrors)
    #--------------errors sorted by occurrences
    print "\nErrors sorted by occurrences"
    for e in e1: #sorting by value #sorted(errors.keys()): #sorting by key
        for s in errdict[e]:
            print "{0:80}: {1:6,}  {2:6,}: statement: {3}: ".format(e[0:80],errors[e],errdict[e][s],s)
    #[(' COMMIT', {'total': 1125396.9740000013, 'max': 7099.58, 'min': 1003.569, 'no': 584}), 
    #--------------queries sorted by totals
    print "\nQuery stats: #:{0:,} queries: {5:,} total:{1:,} min: {2:,} max: {3:,} avg: {4:,}(ms)".format(totals['no'],int(totals['total']),int(totals['min']),int(totals['max']),int(totals['total']/totals['no']),len(queries))
    #[(' COMMIT', {'total': 1125396.9740000013, 'max': 7099.58, 'min': 1003.569, 'no': 584}), 
    #--------------queries sorted by occurrences
    print "\nQueries sorted by total"
    for e in s1:
        print "{0:128}: no: {2:6,} total: {1:9,} ms avg: {3:6,} ms".format(e[0].strip()[:127],int(e[1]['total']), e[1]['no'],int(e[1]['total']/e[1]['no']))
    print "\nQueries sorted by occurrences"
    for e in s2:
        print "{0:128}: no: {2:6,} total: {1:9,} ms avg: {3:6,} ms".format(e[0].strip()[:127],int(e[1]['total']), e[1]['no'],int(e[1]['total']/e[1]['no']))
    #--------------errors sorted by value (occurrences
if __name__ == '__main__':
    main()
