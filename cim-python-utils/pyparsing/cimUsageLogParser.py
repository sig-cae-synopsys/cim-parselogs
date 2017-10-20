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

def main():
    rows=0
    data={}
    mintstamp=0
    mintz=''
    maxtstamp=0
    maxtz=''
    firsttz=''
    firstts=0
    #-----------
    typ=''
    timetz=''
    userId=''
    #-----------
    hostname=''
    level=''
    parameter=''
    count=0
    ipv6=False
    remoteHost=''
    #-----------
    timestamp=0
    result=[]
    typez={} 
    #----------
    skipped=0
    skipping=[]
    #---------
    for root, dirs, files in os.walk(sys.argv[1]):
        for afile in files:
            if afile.startswith("usageLog.log"):  #=='usageLog.log': #.startswith("usageLog.log"):  #=='usageLog.log': #
                path=os.path.join(root, afile)
                print afile,
                for myline in open(path):
                    #print myline
                    if not myline.startswith('{"@type"'):
                        skipped += 1
                        skipping.append(myline)
                        print "?",
                        #print len(myline)
                        #print "skipping",myline
                    else:
                        try:
                            result = json.loads(myline)
                            
                            typ=result['@type']
                            if not typez.has_key(typ):
                                typez[typ]={ "no": 0, "min": 0, "max": 0, "total": 0, "remoteHosts":{},"userIds":{} } #"fields": sorted(result.keys()),
                                #print typ, typez[typ]['fields']
                            rows+=1                        
                            timetz=result['timestamp']
                            timestamp=sec_ts(timetz)*1000
                            if firstts <0.01:
                                firstts=timestamp
                                firsttz=timetz
                                if (mintstamp < 0.01) or (firstts < mintstamp):
                                    mintstamp=firstts
                                    mintz=firsttz
                            data[timestamp]=[timestamp,result['timestamp'],typ,result['userId']]
                            if hostname == '':
                                hostname=result['hostname']
                            if typ not in ('DoesNothingMeansNothing', 'SystemHealthFileDownloadEvent', 'SystemHealthQueryEvent', 'EmailNotificationLoggingEvent','ProjectScopeChangeEvent','UserPasswordChangeEvent'): 
        
                                if hostname != result['hostname'] or result['count'] > 1 or result['ipv6'] == True or not result['parameter']: 
                                    print result
        
                                hostname,level,parameter,userId,remoteHost,count,ipv6=result['hostname'],result['level'],result['parameter'],result['userId'],result['remoteHost'],result['count'],result['ipv6']
         
                                typez[typ]["no"]=typez[typ]["no"]+1
                                
                                if not typez[typ]["userIds"].has_key(userId):
                                    typez[typ]["userIds"][userId]=1
                                else: 
                                    typez[typ]["userIds"][userId]= typez[typ]["userIds"][userId]+1    
                                                                                
                                if typ == 'WebServiceAccessEvent':
                                    clientUserName,duration,endpoint,method,usingAuthenticationKey,version,=result['clientUserName'],result['duration'],result['endpoint'],result['method'],result['usingAuthenticationKey'],result['version']
                                    data[timestamp].append([clientUserName,duration,endpoint,method,usingAuthenticationKey,version,remoteHost])
                                    typez[typ]["total"]=typez[typ]["total"]+duration
                                    if typez[typ]["min"] > duration:
                                        typez[typ]["min"] = duration
                                    if typez[typ]["max"] == 0 or typez[typ]["max"] < duration:
                                        typez[typ]["max"] = duration
                                        
                                    if not typez[typ]["remoteHosts"].has_key(remoteHost):
                                        typez[typ]["remoteHosts"][remoteHost]=1
                                    else: 
                                        typez[typ]["remoteHosts"][remoteHost]= typez[typ]["remoteHosts"][remoteHost]+1    
        
                                    if not typez[typ].has_key('clientUserNames'):
                                        typez[typ]['clientUserNames']={clientUserName : 1 }
                                    else:
                                        if not typez[typ]['clientUserNames'].has_key(clientUserName):
                                            typez[typ]['clientUserNames'][clientUserName]= 1 
                                        else:
                                            typez[typ]['clientUserNames'][clientUserName]= typez[typ]['clientUserNames'][clientUserName] +1 
        
                                    if not typez[typ].has_key('methods'):
                                        typez[typ]['methods']={method : 1 }
                                    else:
                                        if not typez[typ]['methods'].has_key(method):
                                            typez[typ]['methods'][method]= 1 
                                        else:
                                            typez[typ]['methods'][method]= typez[typ]['methods'][method] +1 
        
        
                                    if not typez[typ].has_key('versions'):
                                        typez[typ]['versions']={version : 1 }
                                    else:
                                        if not typez[typ]['versions'].has_key(version):
                                            typez[typ]['versions'][version]= 1 
                                        else:
                                            typez[typ]['versions'][version]= typez[typ]['versions'][version] +1 
        
                                else:                            
                                    if typ == 'WebAccessEvent':
                                        duration,languages,url,userAgent=result['duration'],result['languages'],result['url'],result['userAgent']
                                        data[timestamp].append([duration,languages,url,userAgent])
                                        typez[typ]["total"]=typez[typ]["total"]+duration
                                        if typez[typ]["min"] > duration:
                                            typez[typ]["min"] = duration
                                        if typez[typ]["max"] == 0 or typez[typ]["max"] < duration:
                                            typez[typ]["max"] = duration
                                            
                                        if not typez[typ].has_key('urls'):
                                            typez[typ]['urls']={url : 1 }
                                        else:
                                            if not typez[typ]['urls'].has_key(url):
                                                typez[typ]['urls'][url]= 1 
                                            else:
                                                typez[typ]['urls'][url]= typez[typ]['urls'][url] +1 
                                        if not typez[typ].has_key('userAgents'):
                                            typez[typ]['userAgents']={userAgent : 1 }
                                        else:
                                            if not typez[typ]['userAgents'].has_key(userAgent):
                                                typez[typ]['userAgents'][userAgent]= 1 
                                            else:
                                                typez[typ]['userAgents'][userAgent]= typez[typ]['userAgents'][userAgent] +1 
                                    else:
                                        
                                        if typ == 'LogInEvent':
                                            authenticationSource,failureReason,logInSucceeded,protocol,userName=result['authenticationSource'],result['failureReason'],result['logInSucceeded'],result['protocol'],result['userName']
                                            data[timestamp].append([authenticationSource,failureReason,logInSucceeded,protocol,userName,remoteHost])       
                                            if not typez[typ]["remoteHosts"].has_key(remoteHost):
                                                typez[typ]["remoteHosts"][remoteHost]=1
                                            else: 
                                                typez[typ]["remoteHosts"][remoteHost]= typez[typ]["remoteHosts"][remoteHost]+1    
        
                                            if not typez[typ].has_key('userNames'):
                                                typez[typ]['userNames']={userName : 1 }
                                            else:
                                                if not typez[typ]['userNames'].has_key(userName):
                                                    typez[typ]['userNames'][userName]= 1 
                                                else:
                                                    typez[typ]['userNames'][userName]= typez[typ]['userNames'][userName] +1 
        
                                            if not typez[typ].has_key('protocols'):
                                                typez[typ]['protocols']={protocol : 1 }
                                            else:
                                                if not typez[typ]['protocols'].has_key(protocol):
                                                    typez[typ]['protocols'][protocol]= 1 
                                                else:
                                                    typez[typ]['protocols'][protocol]= typez[typ]['protocols'][protocol] +1 
        
                                            if not typez[typ].has_key('logInSucceededs'):
                                                typez[typ]['logInSucceededs']={logInSucceeded : 1 }
                                            else:
                                                if not typez[typ]['logInSucceededs'].has_key(logInSucceeded):
                                                    typez[typ]['logInSucceededs'][logInSucceeded]= 1 
                                                else:
                                                    typez[typ]['logInSucceededs'][logInSucceeded]= typez[typ]['logInSucceededs'][logInSucceeded] +1 
                                            
                                            if not typez[typ].has_key('failureReasons'):
                                                typez[typ]['failureReasons']={failureReason : 1 }
                                            else:
                                                if not typez[typ]['failureReasons'].has_key(failureReason):
                                                    typez[typ]['failureReasons'][failureReason]= 1 
                                                else:
                                                    typez[typ]['failureReasons'][failureReason]= typez[typ]['failureReasons'][failureReason] +1 
        
                                            if not typez[typ].has_key('authenticationSources'):
                                                typez[typ]['authenticationSources']={authenticationSource : 1 }
                                            else:
                                                if not typez[typ]['authenticationSources'].has_key(authenticationSource):
                                                    typez[typ]['authenticationSources'][authenticationSource]= 1 
                                                else:
                                                    typez[typ]['authenticationSources'][authenticationSource]= typez[typ]['authenticationSources'][authenticationSource] +1 
                                            
        
                                        else:
                                            if typ == 'SessionCreatedEvent':
                                                sessionId=result['sessionId']
                                                data[timestamp].append([sessionId])       
                                            else:
                                                if typ == 'UserViewSelectionEvent':
                                                    viewId=result['viewId']
                                                    data[timestamp].append([viewId])             
                                                    if not typez[typ].has_key('viewIds'):
                                                        typez[typ]['viewIds']={viewId : 1 }
                                                    else:
                                                        if not typez[typ]['viewIds'].has_key(viewId):
                                                            typez[typ]['viewIds'][viewId]= 1 
                                                        else:
                                                            typez[typ]['viewIds'][viewId]= typez[typ]['viewIds'][viewId] +1 
                                                    
                                                else:
                                                    if typ == 'UserIssueAccessEvent':
                                                        cid,issueInstanceId,projectId=result['cid'],result['issueInstanceId'],result['projectId']
                                                        data[timestamp].append([cid,issueInstanceId,projectId])             
        
                                                        if not typez[typ].has_key('cids'):
                                                            typez[typ]['cids']={cid : 1 }
                                                        else:
                                                            if not typez[typ]['cids'].has_key(cid):
                                                                typez[typ]['cids'][cid]= 1 
                                                            else:
                                                                typez[typ]['cids'][cid]= typez[typ]['cids'][cid] +1 
        
                                                        if not typez[typ].has_key('issueInstanceIds'):
                                                            typez[typ]['issueInstanceIds']={issueInstanceId : 1 }
                                                        else:
                                                            if not typez[typ]['issueInstanceIds'].has_key(issueInstanceId):
                                                                typez[typ]['issueInstanceIds'][issueInstanceId]= 1 
                                                            else:
                                                                typez[typ]['issueInstanceIds'][issueInstanceId]= typez[typ]['issueInstanceIds'][issueInstanceId] +1 
        
                                                        if not typez[typ].has_key('projectIds'):
                                                            typez[typ]['projectIds']={projectId : 1 }
                                                        else:
                                                            if not typez[typ]['projectIds'].has_key(projectId):
                                                                typez[typ]['projectIds'][projectId]= 1 
                                                            else:
                                                                typez[typ]['projectIds'][projectId]= typez[typ]['projectIds'][projectId] +1 
        
                                                    else:
                                                        if typ == 'IssueViewedInSourceEvent':
                                                            cid,filename=result['cid'],result['filename']
                                                            data[timestamp].append([cid,filename])             
        
                                                            if not typez[typ].has_key('cids'):
                                                                typez[typ]['cids']={cid : 1 }
                                                            else:
                                                                if not typez[typ]['cids'].has_key(cid):
                                                                    typez[typ]['cids'][cid]= 1 
                                                                else:
                                                                    typez[typ]['cids'][cid]= typez[typ]['cids'][cid] +1 
        
                                                            if not typez[typ].has_key('filenames'):
                                                                typez[typ]['filenames']={filename : 1 }
                                                            else:
                                                                if not typez[typ]['filenames'].has_key(filename):
                                                                    typez[typ]['filenames'][filename]= 1 
                                                                else:
                                                                    typez[typ]['filenames'][filename]= typez[typ]['filenames'][filename] +1 
                                                        else:
                                                            if typ == 'UserViewEditEvent':
                                                                viewId=result['viewId']
                                                                data[timestamp].append([viewId])             
                    
                                                                if not typez[typ].has_key('viewIds'):
                                                                    typez[typ]['viewIds']={viewId : 1 }
                                                                else:
                                                                    if not typez[typ]['viewIds'].has_key(viewId):
                                                                        typez[typ]['viewIds'][viewId]= 1 
                                                                    else:
                                                                        typez[typ]['viewIds'][viewId]= typez[typ]['viewIds'][viewId] +1 
                                                            else:
                                                                if typ == 'SystemSettingChangeEvent':
                                                                    configurationChange=result['configurationChange']
                                                                    data[timestamp].append([configurationChange])             
                        
                                                                    if not typez[typ].has_key('configurationChanges'):
                                                                        typez[typ]['configurationChanges']={configurationChange : 1 }
                                                                    else:
                                                                        if not typez[typ]['configurationChanges'].has_key(configurationChange):
                                                                            typez[typ]['configurationChanges'][configurationChange]= 1 
                                                                        else:
                                                                            typez[typ]['configurationChanges'][configurationChange]= typez[typ]['configurationChanges'][configurationChange] +1     
                                                                else:
                                                                    if typ == 'UserPreferenceChangeEvent':
                                                                        setting=result['setting']
                                                                        data[timestamp].append([setting])             
                            
                                                                        if not typez[typ].has_key('settings'):
                                                                            typez[typ]['settings']={setting : 1 }
                                                                        else:
                                                                            if not typez[typ]['settings'].has_key(setting):
                                                                                typez[typ]['settings'][setting]= 1 
                                                                            else:
                                                                                typez[typ]['settings'][setting]= typez[typ]['settings'][setting] +1         
                                                                    else:
                                                                        if typ == 'SessionDeletedEvent':
                                                                            sessionId=result['sessionId']
                                                                            data[timestamp].append([sessionId])             
                                                                        else:
                                                                            if typ == 'UserCreatedEvent':
                                                                                targetId=result['targetId']
                                                                                username=result['username']
                                                                                data[timestamp].append([targetId,username])             
                                    
                                                                                if not typez[typ].has_key('targetIds'):
                                                                                    typez[typ]['targetIds']={targetId : 1 }
                                                                                else:
                                                                                    if not typez[typ]['targetIds'].has_key(targetId):
                                                                                        typez[typ]['targetIds'][targetId]= 1 
                                                                                    else:
                                                                                        typez[typ]['targetIds'][targetId]= typez[typ]['targetIds'][targetId] +1
                                                                                                 
                                                                                if not typez[typ].has_key('usernames'):
                                                                                    typez[typ]['usernames']={username : 1 }
                                                                                else:
                                                                                    if not typez[typ]['usernames'].has_key(username):
                                                                                        typez[typ]['usernames'][username]= 1 
                                                                                    else:
                                                                                        typez[typ]['usernames'][username]= typez[typ]['usernames'][username] +1         
                                                                            else:
                                                                                if typ == 'UserViewCreationEvent':
                                                                                    userId=result['userId'] #redundant, but viewId is missing here!!!
                                                                                else:
                                                                                    if typ == 'AuthenticationKeyCreationEvent':
                                                                                        keyId=result['keyId'] 
                                                                                        data[timestamp].append([keyId])             
                                                                                    else:
                                                                                        if typ == 'SourceFileViewedEvent':
                                                                                            filename=result['filename']
                                                                                            data[timestamp].append([filename])             
                                                                            
                                                                                            if not typez[typ].has_key('filenames'):
                                                                                                typez[typ]['filenames']={filename : 1 }
                                                                                            else:
                                                                                                if not typez[typ]['filenames'].has_key(filename):
                                                                                                    typez[typ]['filenames'][filename]= 1 
                                                                                                else:
                                                                                                    typez[typ]['filenames'][filename]= typez[typ]['filenames'][filename] +1 
                                                                                        else:                                                                                
                                                                                            print "Unknown entry:{0}:{1}\n".format(typ,myline)
                        except: 
                            #print myline 
                            print "?",#skipped,':',len(myline),                                                            
                            skipped += 1
                            skipping.append(myline)

                firstts=0.0
                if timestamp > maxtstamp:
                    maxtstamp=timestamp
                    maxtz=timetz
                print len(data)
    print 'finished reading {0} - {1} skipped {2:d}'.format(mintz,maxtz,skipped)
    fout = open(sys.argv[2], 'w')
    #------------write timeline data
    fout.write("var usaged=[") 
    dk=sorted(data.keys())
    for d in dk:
        #r=data[d]
        #print r
        #ts= '{0:.2f}'.format(d)
        #dline=ts+", "
        #dline+=json.dumps(r)
        #print dline 
        fout.write('{0},\n'.format(json.dumps(data[d])))  
    fout.write('[]];\n') 
    #------------write stats data
    fout.write('var usagestats={0};\n'.format(json.dumps(typez)))    
    fout.close()
    print 'finished writing\n'  

    if typez.has_key('WebServiceAccessEvent'):
        print "WebServiceAccessEvent stats: #:{0:,} total:{1:,} min:{2:,} max:{3:,} avg:{4:,}(ms)".format(typez['WebServiceAccessEvent']['no'],typez['WebServiceAccessEvent']['total'],typez['WebServiceAccessEvent']['min'],typez['WebServiceAccessEvent']['max'],typez['WebServiceAccessEvent']['total']/typez['WebServiceAccessEvent']['no'])
        print "  ",len(typez['WebServiceAccessEvent']['remoteHosts']),'remoteHosts'#,typez['WebServiceAccessEvent']['remoteHosts']
        print "  ",len(typez['WebServiceAccessEvent']['userIds']),'userIds'#,typez['WebServiceAccessEvent']['userIds']
        print "  ",len(typez['WebServiceAccessEvent']['clientUserNames']),'clientUserNames',typez['WebServiceAccessEvent']['clientUserNames']
        print "  ",len(typez['WebServiceAccessEvent']['methods']),'methods',typez['WebServiceAccessEvent']['methods']
        print "  ",len(typez['WebServiceAccessEvent']['versions']),'versions',typez['WebServiceAccessEvent']['versions']
    if typez.has_key('WebAccessEvent'):
        print "WebAccessEvent stats: #:{0:,} total:{1:,} min:{2:,} max:{3:,} avg:{4:,} (ms)".format(typez['WebAccessEvent']['no'],typez['WebAccessEvent']['total'],typez['WebAccessEvent']['min'],typez['WebAccessEvent']['max'],typez['WebAccessEvent']['total']/typez['WebAccessEvent']['no'])
        print "  ",len(typez['WebAccessEvent']['userIds']),'userIds',typez['WebAccessEvent']['userIds']
        print "  ",len(typez['WebAccessEvent']['userAgents']),'userAgents'#,typez['WebAccessEvent']['userAgents']
        print "  ",len(typez['WebAccessEvent']['urls']),'urls'#,typez['WebAccessEvent']['urls']
    if typez.has_key('LogInEvent'):
        print "LogInEvent stats ",typez['LogInEvent']['no']
        print "  ",len(typez['LogInEvent']['remoteHosts']),'remoteHosts'#,typez['LogInEvent']['remoteHosts']
        print "  ",len(typez['LogInEvent']['userIds']),'userIds',typez['LogInEvent']['userIds']
        print "  ",len(typez['LogInEvent']['userNames']),'userNames'#,typez['LogInEvent']['userNames']
        print "  ",len(typez['LogInEvent']['protocols']),'protocols'#,typez['LogInEvent']['protocols']
        print "  ",len(typez['LogInEvent']['logInSucceededs']),'logInSucceededs'#,typez['LogInEvent']['logInSucceededs']
        print "  ",len(typez['LogInEvent']['failureReasons']),'failureReasons'#,typez['LogInEvent']['failureReasons']
        print "  ",len(typez['LogInEvent']['authenticationSources']),'authenticationSources'#,typez['LogInEvent']['authenticationSources']
    if typez.has_key('SessionCreatedEvent'):
        print "SessionCreatedEvent stats ",typez['SessionCreatedEvent']['no']
        print "  ",len(typez['SessionCreatedEvent']['userIds']),'userIds'#,typez['SessionCreatedEvent']['userIds']
        print "UserViewSelectionEvent stats ",typez['UserViewSelectionEvent']['no']
        print "  ",len(typez['UserViewSelectionEvent']['userIds']),'userIds'#,typez['UserViewSelectionEvent']['userIds']
        print "  ",len(typez['UserViewSelectionEvent']['viewIds']),'viewIds'#,typez['UserViewSelectionEvent']['viewIds']
    if typez.has_key('UserIssueAccessEvent'):
        print "UserIssueAccessEvent stats ",typez['UserIssueAccessEvent']['no']
        print "  ",len(typez['UserIssueAccessEvent']['userIds']),'userIds'#,typez['UserIssueAccessEvent']['userIds']
        print "  ",len(typez['UserIssueAccessEvent']['cids']),'cids'#,typez['UserIssueAccessEvent']['cids']
        print "  ",len(typez['UserIssueAccessEvent']['issueInstanceIds']),'issueInstanceIds'#,typez['UserIssueAccessEvent']['issueInstanceIds']
        print "  ",len(typez['UserIssueAccessEvent']['projectIds']),'projectIds'#,typez['UserIssueAccessEvent']['projectIds']
    if typez.has_key('UserViewEditEvent'):
        print "IssueViewedInSourceEvent stats ",typez['IssueViewedInSourceEvent']['no']
        print "  ",len(typez['IssueViewedInSourceEvent']['userIds']),'userIds'#,typez['IssueViewedInSourceEvent']['userIds']
        print "  ",len(typez['IssueViewedInSourceEvent']['cids']),'cids'#,typez['IssueViewedInSourceEvent']['cids']
        print "  ",len(typez['IssueViewedInSourceEvent']['filenames']),'filenames'#,typez['IssueViewedInSourceEvent']['filenames']
    #{"@type":"UserViewEditEvent","timestamp":"2017-08-02T02:09:34.663+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":10465,"viewId":18700,"parameter":"null"}
    if typez.has_key('UserViewEditEvent'):
        print "UserViewEditEvent stats ",typez['UserViewEditEvent']['no']
        print "  ",len(typez['UserViewEditEvent']['userIds']),'userIds'
        print "  ",len(typez['UserViewEditEvent']['viewIds']),'viewIds'
    #{"@type":"SystemSettingChangeEvent","timestamp":"2017-08-02T17:59:17.954+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":10086,"configurationChange":"LdapUpdateConfig","parameter":"null"}
    if typez.has_key('SystemSettingChangeEvent'):
        print "SystemSettingChangeEvent stats ",typez['SystemSettingChangeEvent']['no']
        print "  ",len(typez['SystemSettingChangeEvent']['userIds']),'userIds'
        print "  ",len(typez['SystemSettingChangeEvent']['configurationChanges']),'configurationChanges'
    #{"@type":"UserPreferenceChangeEvent","timestamp":"2017-08-02T12:59:25.429+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":10283,"setting":"RECENT_PROJECTS","parameter":"null"}
    if typez.has_key('UserPreferenceChangeEvent'):
        print "UserPreferenceChangeEvent stats ",typez['UserPreferenceChangeEvent']['no']
        print "  ",len(typez['UserPreferenceChangeEvent']['userIds']),'userIds'
        print "  ",len(typez['UserPreferenceChangeEvent']['settings']),'settings'
    #{"@type":"SessionDeletedEvent","timestamp":"2017-08-04T12:58:08.589+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":10568,"sessionId":"168E848EA3D070F0D5B5C02A074ED43D","parameter":"null"}
    if typez.has_key('SessionDeletedEvent'):
        print "SessionDeletedEvent stats ",typez['SessionDeletedEvent']['no']
        print "  ",len(typez['SessionDeletedEvent']['userIds']),'userIds'
        #{"@type":"UserCreatedEvent","timestamp":"2017-08-03T06:52:26.249+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":null,"targetId":10619,"username":"502764889","parameter":"null"}
    if typez.has_key('UserCreatedEvent'):
        print "UserCreatedEvent stats ",typez['UserCreatedEvent']['no']
        print "  ",len(typez['UserCreatedEvent']['targetIds']),'targetIds'
        print "  ",len(typez['UserCreatedEvent']['usernames']),'usernames'
    #{"@type":"UserViewCreationEvent","timestamp":"2017-08-08T18:03:13.130+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":10086,"parameter":"null"}
    if typez.has_key('UserViewCreationEvent'):
        print "UserViewCreationEvent stats ",typez['UserViewCreationEvent']['no']
        print "  ",len(typez['UserViewCreationEvent']['userIds']),'userIds'
    #{"@type":"AuthenticationKeyCreationEvent","timestamp":"2017-08-18T14:30:19.572+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":10218,"keyId":10003,"parameter":"null"}
    if typez.has_key('AuthenticationKeyCreationEvent'):
        print "AuthenticationKeyCreationEvent stats ",typez['AuthenticationKeyCreationEvent']['no']
        print "  ",len(typez['AuthenticationKeyCreationEvent']['userIds']),'userIds'
    #{"@type":"SourceFileViewedEvent","timestamp":"2017-08-30T12:36:13.696+0000","level":"INFO","hostname":"USMKEETMIV1897","count":1,"ipv6":false,"remoteHost":null,"userId":10086,"filename":"/id-core/id-dal/src/main/java/com/ge/hcit/id/core/repository/service/IDWorklistService.java","parameter":"null"}
    if typez.has_key('SourceFileViewedEvent'):
        print "SourceFileViewedEvent stats ",typez['SourceFileViewedEvent']['no']
        print "  ",len(typez['SourceFileViewedEvent']['userIds']),'userIds',typez['SourceFileViewedEvent']['userIds']
        print "  ",len(typez['SourceFileViewedEvent']['filenames']),'filenames',typez['SourceFileViewedEvent']['filenames']
    
    print "skipped lines:",skipped
#    for ll in skipping:
#        print ll
    
if __name__ == '__main__':
    main()
