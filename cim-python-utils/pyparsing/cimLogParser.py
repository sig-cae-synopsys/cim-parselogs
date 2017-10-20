#
import sys
import os
import glob
import json

try:
    from pyparsing import (Literal, CaselessLiteral, Word, delimitedList, Optional, 
        Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, 
        ZeroOrMore, restOfLine, Keyword, upcaseTokens, Suppress, OneOrMore,
        CharsNotIn,StringEnd)
except ImportError:
    import sys
    sys.exit("pyparsing is required")
import pyparsing; print pyparsing.__version__

COMMA = Suppress(",")
DASH = Suppress("-")
COLON = Suppress(":")
DOT=Suppress(".")

SEMICOLON  = Suppress(";")
LPAREN= Suppress("(")
RPAREN= Suppress(")")



ident = Word( alphas, alphanums + "_$" ) #.addParseAction(lowercaseTokens) #.setName("identifier")
num = Word(nums)
utcdate= Group(num+DASH+num+DASH+num)+Group(num+COLON+num+COLON+num+DOT+num)+Suppress(Literal("utc"))

ehcacheexp=0
def ehcache(s,loc,toks):
    global ehcacheexp
    ehcacheexp+= 1
    #print toks[0]
    
javaexceptions=0
def exception(s,loc,toks):
    global javaexceptions
    javaexceptions+=1
    #print toks[0]
    
errors=0
warnings=0
debugs=0
infos=0
curtime=0
prevtime=0
#[['2017', '07', '21'], ['04', '04', '57', '568'], 'INFO', 'MethodExecutionAdvice', ['streamLock-9-th-5'], '106', ' Executed: HibernateAnalysisSummariesInstanceDao.flushSession time(ms): 2139.169']
def log4j(s,loc,toks):   
    global errors,warnings,debugs,infos,curtime,prevtime
    res=toks[0]
    date,time=res[0],res[1]
    prevtime=curtime
    etype=res[2]
    service=res[3]
    source=res[4]
    msg=res[6]
    if etype=='INFO':
        infos+=1
    else:
        if etype=='DEBUG':
            debugs+=1
        else:
            if etype=='WARN':
                warnings+=1
            else:
                if etype=='ERROR':
                    errors+=1
                    #timestamp=date[0]+'-'+date[1]+'-'+date[2]+' '+time[0]+':'+time[1]+':'+time[2]+'.'+time[3]+' '
                    #mstring=timestamp + etype+ ' '+service+ ' '+source+ ' :'+msg
                    #print mstring
        
    curtime=[date,time]
    
    #print toks[0]
    
service= Combine(Word(alphas+"_")+ZeroOrMore(DASH + Word(alphas+"_")))  
servicehttp= Combine(Literal("http-nio-")+num+Literal("-exec") )
triagepersister = Literal("Triage Persister")
stacktrace=Group(Group(Literal("java")+restOfLine)+ZeroOrMore(Group(oneOf("at Caused ...")+restOfLine))).setParseAction(exception)
softlocked=Group(Combine(Literal("A soft-locked")+restOfLine)).setParseAction(ehcache)
#(servicehttp  | service | triagepersister ) +Group( Optional(DASH +num) + Optional(DASH + Suppress(Literal("thread")|Literal("th") )+ DASH + num))
log4jline = Group(utcdate + oneOf('INFO DEBUG WARN ERROR') + ident + COMMA + Group(CharsNotIn(":"))  + COLON + num + DASH + restOfLine) .setParseAction(log4j)

logentries = OneOrMore(log4jline | stacktrace |softlocked) #+ StringEnd()

def parse_cimlog(f):
    print "logfile",f
    try:
        result = logentries.parseFile(f,parseAll=True)
        print "Match entries: ",len(result),':',
        print ehcacheexp,"ehcache_msgs,", javaexceptions,'javaexceptions,',  errors,'error', warnings, 'warning',debugs,'debug', infos,'info messages'
        '''
        print "Last 3 parsed entries:"
        print result[len(result)-3]
        print result[len(result)-2]
        print result[len(result)-1]
        print
        '''
    except ParseException as x:
        print "No Match: {0}".format(str(x))

def main():
    for root, dirs, files in os.walk(sys.argv[1]):
        for file in files:
            if file.startswith("cim"):  #-2017-09-11_102307 -2017-09-09_191118
                path=os.path.join(root, file)
                print file #,len(data)
                parse_cimlog(file)

if __name__ == '__main__':
    main()
