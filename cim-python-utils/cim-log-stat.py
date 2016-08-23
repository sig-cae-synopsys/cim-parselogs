import sys
import re
def main():
    cimlog = open( sys.argv[1], "r" )
    #2013-06-28 00:08:08.293  INFO RemoteCommitProtocol2,pool-12-thread-36:120 - Received control message: REQUESTING ...
    d='([0-9]{4}\-[0-9]{2}\-[0-9]{2})'
    t='([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})(utc|)'
    typ='(INFO)'
    source='(RemoteCommitProtocol[0-9]*|DefectMessageProcessorImpl|BasicCommitImpl)'
    pool='pool-([0-9]{2})'
    thread='thread\s*\-([0-9]*):([0-9]*)'
    action='([a-zA-Z]*)(:|)'
    commitevent='^'+d+'\s'+t+'\s*'+typ+'\s'+source+','+pool+'\s*\-'+thread+'\s*\-\s*'+action+'\s*'+'(.*)$'
    #-----------------RemoteCommitProtocol2 regexes
    p = re.compile(commitevent)
    #Received control message: REQUESTING basic_commit_v0?clientVersion=COVERITY (20101119)&domain=2&stream=AA-F3Trunk-1_506218_BKG1.CCD4QQ&askForAllFiles=false
    received='([a-z]*\s[a-z]*):\s([a-zA-Z]*)\s([a-zA-Z0-9_]*)\?([a-zA-Z]*)=(.*)\&([a-zA-Z]*)=(.*)\&([a-zA-Z]*)=(.*)\&([a-zA-Z]*)=(.*)'
    pb = re.compile(received)
    #sending terminator ActionTerminator{, status=OK, exception=null}
    sending='([a-z]*)\s([a-zA-Z]*)\{,\s([a-zA-Z]*)=([a-zA-Z]*),\s([a-zA-Z]*)=(.*)\}'
    pe = re.compile(sending)
    #-----------------RemoteCommitProtocol2 strings
    pb1='Received GUI Probe'
    pe1='Closing BufferedReader'
    #Product: metropltf_lr13l_metro-metroP3041:metropltf_lr13l_metro-metroP3041
    product='([a-zA-Z0-9_\-\.]*):([a-zA-Z0-9_\-\.]*)'
    ps1 = re.compile(product)
    #----------------------------------------------
    #Receiving 1720 files, 5438 functions, 40 models, 275 defects
    receiving='([0-9]*)\s([a-z]*)'
    pr1 = re.compile(receiving)
    
    events = {}
    commitCount=0
    commitsbythreads={}
    commitsbystreams={}
    closing='BufferedReader'
    carryover=[]
    hungcommits=[]
    aborted=[]
    normal=[]
    print 'xin,currentcommits,timestamp,event,thread,stream,status,exception'
    for line in cimlog:
        m=p.match(line)
        if m:
            #print line
            #print m.group(3)
            #print m.group(4)
            #print m.group(5)
            #print m.group(8)
            #print m.group(9)
            #print m.group(10)
            tstamp=m.group(2)
            tiempo=tstamp.split(':')
            hrs,min,secmsec=tiempo[0],tiempo[1],tiempo[2].split('.')
            sec,msec=secmsec[0],secmsec[1]
            tempus=float((int(hrs)*60+int(min))*60+int(sec)+float(msec)/1000)
            threadid=''
            utc=m.group(3)
            eventtype=m.group(4)
            eventsource=m.group(5)
            threadid=m.group(7)
            eventaction=m.group(9)
            eventparams=m.group(11)
            if eventsource=='RemoteCommitProtocol2':
                if eventaction=='Received':
                    commitCount+=1
                    mb=pb.match(eventparams)
                    if mb:
                        stream=mb.group(9)
                        commitsbythreads[threadid]={'thread':threadid,'stream':stream, 'begin':tstamp, 'end':'?','xin':tempus,'xout':'?','status':'?','exception':'?','defects':'?','files':'?','functions':'?','models':'?'}
                        print tempus,',',commitCount,',',tstamp,',','START_',',',threadid,',',stream
                    else:
                        print 'no match for Received:',eventparams
                if eventaction=='sending':
                    commitCount-=1
                    me=pe.match(eventparams)
                    if me:
                        status=me.group(4)
                        exception=me.group(6)
                        stream='?'
                        if threadid in commitsbythreads.keys():
                            commitsbythreads.get(threadid)['end']=tstamp
                            commitsbythreads.get(threadid)['xout']=tempus
                            commitsbythreads.get(threadid)['status']=status
                            commitsbythreads.get(threadid)['exception']=exception
                            stream=commitsbythreads.get(threadid)['stream']
                        else:
                            commitsbythreads[threadid]={'thread':threadid, 'stream':'?', 'begin':'?','xin':'?','defects':'?','files':'?','functions':'?','models':'?'}
                            commitsbythreads.get(threadid)['end']=tstamp
                            commitsbythreads.get(threadid)['xout']=tempus
                            commitsbythreads.get(threadid)['status']=status
                            commitsbythreads.get(threadid)['exception']=exception
                            carryover.append(commitsbythreads.get(threadid))
                        if status!='OK':
                            aborted.append(commitsbythreads.get(threadid)) 
                        else:
                            normal.append(commitsbythreads.get(threadid))                         
                        print tempus,',',commitCount,',',tstamp,',','FINISH',',',threadid,',',stream,',',status,',',exception
                        if stream in commitsbystreams.keys():
                           commitsbystreams.get(stream)[tstamp]=commitsbythreads.get(threadid)
                        else:
                           commitsbystreams[stream]={tstamp:commitsbythreads.get(threadid)}                                          
                    else:
                        print 'no match for sending:',eventparams
            if eventsource=='BasicCommitImpl' :
                if eventaction == 'Receiving':
                    mr2=pr1.match(eventparams)
                    if mr2:
                        what2=mr2.group(2)
                        howmuch2=mr2.group(1)
                        if threadid in commitsbythreads.keys():
                            commitsbythreads.get(threadid)[what2]=howmuch2
                            if what2 != 'files' or howmuch2 != '4' :
                                commitsbythreads.get(threadid)[what2]=howmuch2
                        else:
                            commitsbythreads[threadid]={'thread':threadid,'stream':'?', 'begin':'?', 'end':'?','xin':'?','xout':'?','status':'?','exception':'?','defects':'?','files':'?','functions':'?','models':'?'}
                            commitsbythreads.get(threadid)[what2]=howmuch2
            
            if eventsource=='DefectMessageProcessorImpl':
                if eventaction == 'Receiving':
                    mr=pr1.match(eventparams)
                    if mr:
                        what=mr.group(2)
                        howmuch=mr.group(1)
                        if threadid in commitsbythreads.keys():
                            commitsbythreads.get(threadid)[what]=howmuch
                            if what != 'files' or howmuch != '4' :
                                commitsbythreads.get(threadid)[what]=howmuch
                        else:
                            commitsbythreads[threadid]={'thread':threadid,'stream':'?', 'begin':'?', 'end':'?','xin':'?','xout':'?','status':'?','exception':'?','defects':'?','files':'?','functions':'?','models':'?'}
                            commitsbythreads.get(threadid)[what]=howmuch
            if eventsource=='RemoteCommitProtocol':
                if eventaction=='Received' and eventparams=='GUI Probe':    #'Received GUI Probe'
                    commitCount+=1
                    stream='?'
                    commitsbythreads[threadid]={'thread':threadid,'stream':stream, 'begin':tstamp, 'end':'?','xin':tempus,'xout':'?','status':'?','exception':'?','defects':'?','files':'?','functions':'?','models':'?'}
                    #print tempus,',',commitCount,',',tstamp,',','START_',',',threadid,',',stream
                if eventaction=='Received' and eventparams=='BYE':    #'Received BYE'
                    if threadid in commitsbythreads.keys():
                        commitsbythreads.get(threadid)['status']='OK'
                        commitsbythreads.get(threadid)['exception']=''
                    else:
                        commitsbythreads[threadid]={'thread':threadid,'stream':'?', 'begin':'?', 'end':'?','xin':'?','xout':'?','status':'OK','exception':'','defects':'?','files':'?','functions':'?','models':'?'}
                if eventaction == 'Receiving':
                    mr=pr1.match(eventparams)
                    if mr:
                        what=mr.group(2)
                        howmuch=mr.group(1)
                        if threadid in commitsbythreads.keys():
                            if what != 'files' or howmuch != '4' :
                                commitsbythreads.get(threadid)[what]=howmuch
                        else:
                            commitsbythreads[threadid]={'thread':threadid,'stream':'?', 'begin':'?', 'end':'?','xin':'?','xout':'?','status':'?','exception':'?','defects':'?','files':'?','functions':'?','models':'?'}
                            commitsbythreads.get(threadid)[what]=howmuch
                if eventaction=='Closing' and eventparams=='BufferedReader':    #'Closing BufferedReader'
                    commitCount-=1
                    if threadid in commitsbythreads.keys():
                        commitsbythreads.get(threadid)['end']=tstamp
                        commitsbythreads.get(threadid)['xout']=tempus
                        xin=commitsbythreads.get(threadid)['xin']
                        if xin == '?': #entry generated by Received BYE
                            carryover.append(commitsbythreads.get(threadid)) 
                    else:
                        commitsbythreads[threadid]={'thread':threadid,'stream':'?', 'begin':'?', 'end':tstamp,'xin':'?','xout':tempus,'status':'?','exception':'?','defects':'?','files':'?','functions':'?','models':'?'}
                        carryover.append(commitsbythreads.get(threadid))
                    #------------------------------lets sort out the results
                    stream=commitsbythreads.get(threadid)['stream']
                    status=commitsbythreads.get(threadid)['status']
                    exception=commitsbythreads.get(threadid)['exception']
                    xin=commitsbythreads.get(threadid)['xin']
                    print tempus,',',commitCount,',',tstamp,',','FINISH',',',threadid,',',stream,',',status,',',exception
                    if status!='OK':
                        aborted.append(commitsbythreads.get(threadid)) 
                    else:
                        normal.append(commitsbythreads.get(threadid))                                                 
                    if stream in commitsbystreams.keys():
                       commitsbystreams.get(stream)[tstamp]=commitsbythreads.get(threadid)
                    else:
                       commitsbystreams[stream]={tstamp:commitsbythreads.get(threadid)}                                          
                if eventaction=='Product':    #Product: metropltf_lr13l_metro-metroP3041:metropltf_lr13l_metro-metroP3041
                    ms=ps1.match(eventparams)
                    if ms:
                        stream=ms.group(1)
                        #print stream
                        if threadid in commitsbythreads.keys():
                            c1=commitsbythreads.get(threadid)
                            c1['stream']=stream
                            print c1['xin'],',',commitCount,',',c1['begin'],',','START_',',',threadid,',',stream
                        else:
                            print 'no matching thread for Product:',eventparams
                    else:
                        print 'no match for Product:',eventparams
                
#        else:
#            print 'no match:', line           
#-----------------------------------------------------------------------hungcommits
    for key in commitsbythreads.keys():
        if commitsbythreads.get(key)['end']=='?':
            hungcommits.append(commitsbythreads.get(key))

    print 'xin,xout,begin,end,thread,stream,status,exception,defects,files,functions,models,span (min.)'
    print ',,,,,carryovers,',len(carryover)
    for cmt in carryover:
        print cmt["xin"],',',cmt["xout"],',',cmt["begin"] ,',',cmt["end"],',',cmt["thread"],',',cmt["stream"],',',cmt["status"],',',cmt["exception"],',',cmt["defects"],',',cmt["files"],',',cmt["functions"],',',cmt["models"]
    print ',,,,,unfinished,',len(hungcommits)
    for cmt in hungcommits:
        print cmt["xin"],',',cmt["xout"],',',cmt["begin"] ,',',cmt["end"],',',cmt["thread"],',',cmt["stream"],',',cmt["status"],',',cmt["exception"],',',cmt["defects"],',',cmt["files"],',',cmt["functions"],',',cmt["models"]
    #------------------------------------------------
    print ',,,,,aborted,',len(aborted)
    sumtime=0
    count=0
    for cmt in aborted:
        xin=cmt["xin"]
        xout=cmt["xout"]
        span=0
        if xin != '?' and xout!='?' :
            span=(xout-xin)/60
            sumtime+=span
            count+=1            
        print cmt["xin"],',',cmt["xout"],',',cmt["begin"] ,',',cmt["end"],',',cmt["thread"],',',cmt["stream"],',',cmt["status"],',',cmt["exception"],',',cmt["defects"],',',cmt["files"],',',cmt["functions"],',',cmt["models"],',', span
    if count > 0 :        
        print 'average time',sumtime/count,'min.'
    #------------------------------------------------
        
    print ',,,,,normal,',len(normal)
    sumtime=0
    count=0
    for cmt in normal:
        xin=cmt["xin"]
        xout=cmt["xout"]
        span=0
        if xin != '?' and xout!='?' :
            span=(xout-xin)/60
            sumtime+=span
            count+=1            
        #print cmt    
        print cmt["xin"],',',cmt["xout"],',',cmt["begin"] ,',',cmt["end"],',',cmt["thread"],',',cmt["stream"],',',cmt["status"],',',cmt["exception"],',',cmt["defects"],',',cmt["files"],',',cmt["functions"],',',cmt["models"],',', span
    if count > 0 :        
        print 'average time',sumtime/count,'min.'
    #------------------------------------------------
        
    print ',,,,,streams,',len(commitsbystreams)
    for stream in commitsbystreams.keys():
        print ',,,,stream,',stream,',',len(commitsbystreams.get(stream))
        sumtime=0
        count=0
        for key in commitsbystreams.get(stream).keys():
            cmt=commitsbystreams.get(stream)[key]
            xin=cmt["xin"]
            xout=cmt["xout"]
            span=0
            if xin != '?' and xout!='?' :
                span=(xout-xin)/60
                sumtime+=span
                count+=1            
            print cmt["xin"],',',cmt["xout"],',',cmt["begin"] ,',',cmt["end"],',',cmt["thread"],',',cmt["stream"],',',cmt["status"],',',cmt["exception"],',',cmt["defects"],',',cmt["files"],',',cmt["functions"],',',cmt["models"],',', span
        if count > 0 :        
            print 'average time',sumtime/count,'min.'
    #------------------------------------------------
    #print normal
    
if __name__ == '__main__':
    main()

            
            
            
     
