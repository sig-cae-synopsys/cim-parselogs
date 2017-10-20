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

try:
    from pyparsing import (Literal, CaselessLiteral, Word, delimitedList, Optional, 
        Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, 
        ZeroOrMore, restOfLine, Keyword, upcaseTokens, Suppress, OneOrMore,
        CharsNotIn,StringEnd)
except ImportError:
    import sys
    sys.exit("pyparsing is required")

COMMA = Suppress(",")
DASH = Literal("-")
PLUS = Literal("+")
COLON = Suppress(":")
LCOLON = Literal(":")
DOT=Literal(".")
EQUAL=Suppress("=")
SEMICOLON  = Suppress(";")
LPAREN= Suppress("(")
RPAREN= Suppress(")")
LSQBRACKET= Suppress("[")
RSQBRACKET= Suppress("]")
LCBRACKET= Suppress("{")
RCBRACKET= Suppress("}")
DQUOTE=Suppress('"')

wd=Word(alphas)
num = Word(nums)
dnum = Combine(Word(nums)+DOT+Word(nums+'E'))
knum = Word(nums)+Suppress('K')
datetz=Combine(num+DASH+num+DASH+num+Literal("T")+num+LCOLON+num+LCOLON+num+DOT+num+(DASH | PLUS)+num)
hostname=Suppress(DQUOTE+Word(alphanums+'-.')+DQUOTE)

pre=Suppress(Literal('"@type":"PerformanceLogEvent","timestamp":'))
ts=DQUOTE+datetz+DQUOTE
mid1=Suppress(Literal(',"level":"INFO","hostname":'))
mid2=Suppress(Literal(',"count":1,"metrics":'))
mid3=Suppress(Optional(Literal(',"parameter":"null"')))
metrics=Group(delimitedList(Group(DQUOTE+wd+DQUOTE+COLON+dnum)))

#{"@type":"PerformanceLogEvent","timestamp":"2017-07-23T20:27:41.945+0000","level":"INFO","hostname":"sjc-asr-197","count":1,
#"metrics":{"backUpInProgress":0.0,"commitExecutorSize":0.0,"unixOneMinuteLoadAverage":0.0,"cimCpuUsage":0.0,"memoryUsed":0.0,
#"webRequestsPerSecond":0.0,"diskBytesRead":0.0,"activeCommitCount":0.0,"commitQueueSize":0.0,"memoryTotal":0.0,
#"wsRequestsPerSecond":0.0,"commitGateOpen":0.0,"diskBytesWritten":0.0,"skeletonizationInProgress":0.0},"parameter":"null"}
logentry=LCBRACKET+pre+ts+mid1+hostname+mid2+LCBRACKET+metrics+RCBRACKET+mid3+RCBRACKET

def kmin(aggr,d):
    res=aggr
    for i in (0,1,2):
        if (d[i]<aggr[i] or aggr[i]<1 ): res[i]=d[i] 
    return res   

def kmax(aggr,d):
    res=aggr
    for i in (0,1,2):
        if (d[i]>aggr[i]): 
            res[i]=d[i] 
    return res  
        
def sizeof_fmt(num, suffix='B'):
    for unit in ['','K','M','G','T','P']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def days_hours_minutes(td):
    return td.days, td.seconds//3600, (td.seconds//60)%60, td.seconds%60

def sec_ts(ts):
    t1 =parser.parse(ts)
    t2= datetime.fromtimestamp(0, pytz.utc)
    td=(t1-t2).total_seconds()
    return td
'''
backUpInProgress: 0.000
commitExecutorSize: 5.000
unixOneMinuteLoadAverage: 0.000  #max
cimCpuUsage: 7.333                #max
memoryUsed: 8331763068.000        #min-max
webRequestsPerSecond: 0.000        #max
diskBytesRead: 0.000               #sum
activeCommitCount: 0.000          #max
commitQueueSize: 0.000            #max
memoryTotal: 11369185280.000      #min-max
wsRequestsPerSecond: 0.000        #max
commitGateOpen: 1.000
diskBytesWritten: 66833.067        sum
skeletonizationInProgress: 0.000
'''

def main():
    columns=[]
    rows=0
    data={}
    dp=[]
    mintstamp=0
    mintz=''
    maxtstamp=0
    maxtz=''
    firsttz=''
    firstts=0
    timetz=''
    timestamp=0
    result=[]
    for root, dirs, files in os.walk(sys.argv[1]):
        for file in files:
            if file .startswith("performance"):  #=='performanceLog.log':
                path=os.path.join(root, file)
                print file,
                for myline in open(path):
                    try:
                        result = logentry.parseString(myline)
                        rows+=1
                        timetz=result[0]
                        timestamp=sec_ts(timetz)*1000
                        if len(columns)<1:
                            columns.append('timestamp')
                            for c in result[1]:
                                columns.append(c[0])
                        if firstts <0.01:
                            firstts=timestamp
                            firsttz=timetz
                            if (mintstamp < 0.01) or (firstts < mintstamp):
                                mintstamp=firstts
                                mintz=firsttz
                        dp=[result[0],result[1]]
                        data[timestamp]=dp
                    except ParseException as x:
                        print "No Match: {0}==>{1}".format(str(x), myline)
                        #sys.exit()
                firstts=0.0
                if timestamp > maxtstamp:
                    maxtstamp=timestamp
                    maxtz=timetz
                print len(data)
    print 'finished reading {0} - {1}'.format(mintz,maxtz)
    '''
    backUpInProgress: 0.000
    commitExecutorSize: 5.000
    unixOneMinuteLoadAverage: 0.000       #max
    cimCpuUsage: 7.333                    #max
    memoryUsed: 8331763068.000            #min-max
    webRequestsPerSecond: 0.000           #max
    diskBytesRead: 0.000                  #sum
    activeCommitCount: 0.000              #max
    commitQueueSize: 0.000                #max
    memoryTotal: 11369185280.000          #min-max
    wsRequestsPerSecond: 0.000            #max
    commitGateOpen: 1.000
    diskBytesWritten: 66833.067           #sum
    skeletonizationInProgress: 0.000
    '''
    maxunix1,maxcimcpu,maxmemused,maxwrpers,sumdiskread,maxactivecommit,maxcommitquesize,maxmemtotal,maxwsrpers,sumdiskwritten=0,0,0,0,0,0,0,0,0,0    
    fout = open(sys.argv[2], 'w')
    fout.write("var perfd=[") 
    dk=sorted(data.keys())
    for d in dk:
        r=data[d]
        ts= '{0:.2f}'.format(d)
        tz='"'+r[0]+'"'
        dline=ts+", "+tz
        metrics=r[1].asList()
        memu=0.0
        for m in metrics:
            k,v=m
            dline+=',{0:.2f}'.format(float(v))
            if k == 'unixOneMinuteLoadAverage': maxunix1 = max(float(v),maxunix1 )
            else:
                if k == 'cimCpuUsage': maxcimcpu = max(float(v),maxcimcpu )
                else:
                    if k == 'memoryUsed': 
                        memu=float(v)
                        maxmemused = max(float(v),maxmemused )
                    else:
                        if k == 'webRequestsPerSecond': maxwrpers = max(float(v),maxwrpers )
                        else:
                            if k == 'wsRequestsPerSecond': 
                                if float(v)>0.01:
                                    #print k,float(v) 
                                    maxwsrpers = max(float(v),maxwsrpers )
                            else:
                                if k == 'activeCommitCount': 
                                    if float(v)>0.01:
                                        #print k,float(v) 
                                        maxactivecommit = max(float(v),maxactivecommit )
                                else:
                                    if k == 'commitQueueSize': 
                                        if float(v)>0.01:
                                            #print k,float(v) 
                                            maxcommitquesize = max(float(v),maxcommitquesize )
                                    else:
                                        if k == 'memoryTotal': maxmemtotal = max(float(v),maxmemtotal )
                                        else:
                                            if k == 'diskBytesRead':
                                                sumdiskread += float(v)
                                            else:
                                                if k == 'diskBytesWritten': sumdiskwritten += float(v)
        #print dline 
        if memu > 512000000 :           
            fout.write('[{0}],\n'.format(dline))  
#        else:
#            print 'omitted:' ,dline 
    megabytes=1024*1024     
    fout.write('];\n') 
    stline='"max.unixOneMinuteLoadAverage = {0:.2f}" ,'. format(maxunix1)
    stline+='" max.cimCpuUsage = {0:.2f}",'. format(maxcimcpu)
    stline+='"  max.memoryUsed = {0:,.0f} MB",'. format(maxmemused/megabytes)
    stline+='"  max.webRequestsPerSecond = {0:.2f}",'. format(maxwrpers)
    stline+='"  max.wsRequestsPerSecond = {0:.2f}",'. format(maxwsrpers)
    stline+='"  max.activeCommitCount = {0:.2f}",'. format(maxactivecommit)
    stline+='"  max.commitQueueSize = {0:.2f}",'. format(maxcommitquesize)
    stline+='"  max.memoryTotal = {0:,.0f} MB",'. format(maxmemtotal/megabytes)
    stline+='"  sum.diskBytesRead = {0:,.0f} MB",'. format(sumdiskread/megabytes)
    stline+='"  sum.diskBytesWritten = {0:,.0f} MB"'. format(sumdiskwritten/megabytes)               
    print stline
    fout.write('var perfstatlines=[{0}];\n'.format(stline))    
    fout.close()
    print 'finished writing\n'  
    

if __name__ == '__main__':
    main()
