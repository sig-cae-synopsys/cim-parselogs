#
import sys
import json
import pytz
from dateutil import parser
import time
from datetime import datetime, timedelta
from pyparsing import oneOf

try:
    from pyparsing import (Literal, CaselessLiteral, Word, delimitedList, Optional, 
        Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, 
        ZeroOrMore, restOfLine, Keyword, upcaseTokens, Suppress, OneOrMore,
        CharsNotIn,StringEnd)
except ImportError:
    import sys
    sys.exit("pyparsing is required")

COMMA = Suppress(",")
SIGN = oneOf("- +")
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

num = Word(nums)
dnum = Combine(Word(nums)+DOT+Word(nums))
knum = Word(nums)+Suppress('K')
datetz=Combine(num+DASH+num+DASH+num+Literal("T")+num+LCOLON+num+LCOLON+num+DOT+num+SIGN+num)

cause=(Literal("Ergonomics")|Literal("Allocation Failure")| Literal("Metadata GC Threshold")| Literal("GCLocker Initiated GC"))
data=Group(knum+Suppress(Literal("->"))+knum+LPAREN+knum+RPAREN)
gen=LSQBRACKET+Suppress(oneOf("PSYoungGen: ParOldGen: ParOldGen: Metaspace:"))+data+RSQBRACKET
times=Group(LSQBRACKET+Suppress(Literal("Times: user="))+dnum+Suppress(Literal("sys="))+dnum+COMMA+Suppress(Literal("real="))+dnum+Suppress(Literal("secs"))+RSQBRACKET)

nongc   =oneOf("Java Memory CommandLine")+restOfLine
#https://stackoverflow.com/questions/1174976/what-does-gc-mean-in-a-java-garbage-collection-log for GC-- explanation
gc      =datetz+COLON+dnum+COLON+LSQBRACKET+Literal("GC")+LPAREN+cause+RPAREN+Suppress(Optional(DASH+DASH))+gen+data+COMMA+dnum+Suppress(Literal('secs'))+RSQBRACKET+times#+restOfLine #
fullgc  =datetz+COLON+dnum+COLON+LSQBRACKET+Literal("Full GC")+LPAREN+cause+RPAREN+gen+gen+data+COMMA+gen+COMMA+dnum+Suppress(Literal('secs'))+RSQBRACKET+times#+restOfLine

memline=Literal("Memory:")+num+Literal("k page, physical")+num+Suppress('k')+LPAREN+num+Literal("k free")+RPAREN+COMMA+Literal("swap")+num+Suppress('k')+LPAREN+num+Literal("k free")+RPAREN

numoption = Group(Literal("-XX:")+Word(alphas)+EQUAL+num)
flagoption = Group(Literal("-XX:+")+Word(alphas))
cmdline=Literal("CommandLine flags:")+ Group(ZeroOrMore(flagoption | numoption  ))

logentry = (nongc | gc | fullgc)

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

def secoffset(ts,sec):
    t1 =parser.parse(ts)
    t2= datetime.fromtimestamp(0, pytz.utc)
    td=(t1-t2).total_seconds()
    return td -sec

def main():
    tstamp,gctype,young,old,meta,total='','','','','',''
    tsecs,tdur =0.0,0.0
    sumtdur=0.0
    youngmin=[0,0,0]
    youngmax=[0,0,0]
    oldmin=[0,0,0]
    oldmax=[0,0,0]
    totalmin=[0,0,0]
    totalmax=[0,0,0]
    metamin=[0,0,0]
    metamax=[0,0,0]
    lastmeta=[0,0,0]
    lastold=[0,0,0]
    #tline={}
    commandline=''
    memory=''
    javaline=''
    allgcs=0
    gcs=0
    fullgcs=0
    gcdur=0
    maxgcdur=0
    fullgcdur=0
    maxfullgcdur=0
    phys=0
    physfree=0
    swap=0
    swapfree=0
    initialheapsize, maxheapsize=0,0
    t1970Jan1=0
    fout = open(sys.argv[2], 'w')
    fout.write("var d=[")                
    for myline in open(sys.argv[1]):
        try:
            result = logentry.parseString(myline)
            if result[0] not in ("Java", "Memory","CommandLine"):
                allgcs+=1
                tstamp=result[0]
                tsecs=float(result[1])
                if t1970Jan1 < 1 :
                    t1970Jan1= secoffset(tstamp,tsecs)                
                gctype=result[2]
                gccause=result[3]
                young=[int(result[4].asList()[0]),int(result[4].asList()[1]),int(result[4].asList()[2])]
                if gctype == 'Full GC':
                    old=[int(result[5].asList()[0]), int(result[5].asList()[1]), int(result[5].asList()[2])]    
                    total=[int(result[6][0]), int(result[6][1]), int(result[6][2])]
                    meta=[int(result[7].asList()[0]), int(result[7].asList()[1]), int(result[7].asList()[2])]
                    tdur=float(result[8])
                    fullgcs +=1
                    fullgcdur +=tdur
                    maxfullgcdur=max(maxfullgcdur,tdur)
                    lastold=old
                    lastmeta=meta
                else:
                    total=[int(result[5].asList()[0]), int(result[5].asList()[1]), int(result[5].asList()[2])] 
                    tdur=float(result[6])
                    gcs+=1
                    gcdur +=tdur
                    maxgcdur=max(maxgcdur,tdur)
                    old=lastold
                    meta=lastmeta
                youngmin=kmin(youngmin,young)
                youngmax=kmax(youngmax,young)
                oldmin=kmin(oldmin,old)
                oldmax=kmax(oldmax,old)
                totalmin=kmin(totalmin,total)
                totalmax=kmax(totalmax,total)
                metamin=kmin(metamin,meta)
                metamax=kmax(metamax,meta)
                sumtdur+=tdur
                #tline[tsecs]=[tstamp,gctype,gccause,young,old,total,meta,sumtdur]
                #gcViewer:Timestamp(unix/#),Used(K),Total(K),Pause(sec),GC-Type  
                #1499383794,131584,502784,0.0296548,GC (Allocation Failure)
                fout.write('[{0:.2f}, {1:d}, {2:d}, {3:.2f}, "{4}"],\n'.format((tsecs+t1970Jan1)*1000,total[1]/1024,total[2]/1024,tdur,gctype+" ("+gccause+")"))                
            else:
                if result[0]=="Memory": 
                    memory=result[0]+result[1]
                    res=memline.parseString(memory)
                    phys,physfree=int(res[3])*1024,int(res[4])*1024
                    swap,swapfree=int(res[7])*1024,int(res[8])*1024
                else:
                    if result[0]=="CommandLine": 
                        commandline=result[0]+result[1]
                        res=cmdline.parseString(commandline)
                        for opt in res[1]:
                            if opt[1] == 'InitialHeapSize':
                                initialheapsize=int(opt[2])
                            if opt[1] == 'MaxHeapSize':
                                maxheapsize=int(opt[2])
                    else:
                        if result[0]=="Java": 
                            javaline=result[0]+result[1]
                            javaline=javaline.replace('"',"'")
        except ParseException as x:
            print "No Match: {1}\n{0}".format(str(x),myline)
            sys.exit()
    fout.write('[{0:.2f}, {1:d}, {2:d}, {3:.2f}, "{4}"]]\n'.format((tsecs+t1970Jan1)*1000,total[1]/1024,total[2]/1024,tdur,gctype+" ("+gccause+")"))                
    print javaline
    print memory
    print commandline    
    td=days_hours_minutes(timedelta(seconds=tsecs))
    ymin,ymax=map((lambda x:sizeof_fmt(x*1024)), youngmin), map((lambda x:sizeof_fmt(x*1024)), youngmax)
    omin,omax=map((lambda x:sizeof_fmt(x*1024)), oldmin),   map((lambda x:sizeof_fmt(x*1024)), oldmax)
    tmin,tmax=map((lambda x:sizeof_fmt(x*1024)), totalmin), map((lambda x:sizeof_fmt(x*1024)), totalmax)
    mmin,mmax=map((lambda x:sizeof_fmt(x*1024)), metamin),  map((lambda x:sizeof_fmt(x*1024)), metamax)

    print  '-------------------summary stats----------------------'
    stline1 ='elapsed: {1:,.3f} secs ({2:d} days {3:02d}:{4:02d}:{5:02d}), {0:,d} pauses ,  spent {6:,.3f} secs in all GCs, {7:.3f} %'.format(allgcs,tsecs,td[0],td[1],td[2],td[3],sumtdur,sumtdur/tsecs*100)
    stline15='Full GC {0:,d} max: {1:,.3f}, avg: {2:,.3f} total: {3:,.3f} secs {4:.3f}%, GC {5:,d} max: {6:,.3f}, avg: {7:,.3f} total: {8:,.3f} secs {9:.3f}% '.format(fullgcs,maxfullgcdur,fullgcdur/fullgcs,fullgcdur,fullgcdur/tsecs*100,gcs,maxgcdur,gcdur/gcs,gcdur,gcdur/tsecs*100)
    stline2 ='physical memory (at startup): {0} ({1} free), swap: {2}({3} free), '.format(sizeof_fmt(phys),sizeof_fmt(physfree),sizeof_fmt(swap),sizeof_fmt(swapfree))
    stline3 ='initial heap size: {0}, max heap size: {1}'.format(sizeof_fmt(initialheapsize),sizeof_fmt(maxheapsize))
    stline4 ='max:  younggen: {0}/{1}, oldgen: {2}/{3}, total: {4}/{5},(used max {8:.1f}% of the free phys mem, {9:.1f}% of the max heap)  metaspace: {6}/{7} '.format(ymax[0],ymax[2],omax[0],omax[2],tmax[0],tmax[2],mmax[0],mmax[2],totalmax[0]*1024*100/physfree,totalmax[0]*1024*100/maxheapsize)
    stline5 ='min:  younggen: {0}/{1}, oldgen: {2}/{3}, total: {4}/{5},  metaspace: {6}/{7} '.format(ymin[1],ymin[2],omin[1],omin[2],tmin[1],tmin[2],mmin[1],mmin[2])
    print stline1+"\n"+stline15+"\n"+stline2+stline3+"\n"+stline4+"\n"+stline5
    fout.write('var javaline="{0}";\n'.format(javaline))                
    fout.write('var commandline="{0}";\n'.format(commandline))                
    fout.write('var memory="{0}";\n'.format(memory))
    fout.write('var statlines=["{0}",\n"{1}",\n"{2}",\n"{3}",\n"{4}"]\n'.format(stline1,stline15,stline2+stline3,stline4,stline5))    
    fout.close()
#    with open("gctimeline.json", 'w') as out_data:
#        out_data.write(json.dumps(tline,indent=2,sort_keys=True ))
if __name__ == '__main__':
    main()
