import os
import gc
import re
import gzip
import json
import datetime
import dbapi
import pickle

def deal_num(s):
    while len(s) < 8:
        s = '0' + s
    return s

start = datetime.datetime.now()

client = dbapi.connect('10.1.0.160',30015,'SYSTEM','Xia550505')
cursor = client.cursor()
#ret=cursor.execute('create table sessions( matrix CLOB, ttt varchar(10)) ')
client2 = dbapi.connect('10.1.0.160',30015,'SYSTEM','Xia550505')
cursor2 = client2.cursor()

urls = {}
n = 0
kinds = open('/home/workload/workload/analysis/base_url.txt', 'r+')
for line in kinds:
    now = line.split()
    urls[now[0]] = n
    n += 1
kinds.close()

session = {}
ip_n = 0
id = 1
count = 0

startb1 = datetime.datetime.now()
temp = cursor.execute('select top 100000 ip,time,event_type from logs')
#row = cursor.fetchone()
row = ''
while row is not None:
    try:
        row = cursor.fetchone()
       # print row
        if row is None:
            break
        ip = row[0]
        time = row[1]
        event_type = row[2]
        count += 1
       # print count
    
        obj_ip = ip
        obj_time = datetime.datetime.strptime(time[:19], '%Y-%m-%dT%H:%M:%S')
        obj_url = None
      
        for key in urls:  
            if re.search(key, event_type[1:]) is not None:
                obj_url = urls[key]
                break
    
        if obj_url is None:
            continue

        if obj_ip not in session:
            ip_n += 1
            session[obj_ip] = {}
            session[obj_ip]['url'] = obj_url
            session[obj_ip]['time'] = obj_time
            session[obj_ip]['matrix'] = [[0 for j in range(n)] for i in range(n)]
        else:
            prev_url = session[obj_ip]['url']
            prev_time = session[obj_ip]['time']
            if prev_time+datetime.timedelta(minutes = 30) >= obj_time:
                session[obj_ip]['matrix'][prev_url][obj_url] += 1
            else:                                 
                tmp_matrix = pickle.dumps(session[obj_ip]['matrix'])
                ret = cursor2.execute('insert into sessions values(?,?)',(id,tmp_matrix))
                id += 1
                session[obj_ip]['matrix'] = [[0 for j in range(n)] for i in range(n)]
      
        session[obj_ip]['url'] = obj_url
        session[obj_ip]['time'] = obj_time

   except Exception, e:
        print e
        continue
   
endb1 = datetime.datetime.now()
startb2 = datetime.datetime.now()
for ip in session:
    tmp_matrix1 = pickle.dumps(session[ip]['matrix'])
    ret = cursor2.execute('insert into sessions values(?,?)', (id, tmp_matrix1))
    id += 1

client2.commit()
session.clear()
gc.collect()
endb2 = datetime.datetime.now()
end = datetime.datetime.now()
print "id: {0}".format(id-1)
print “total time: {0}”.format(end - start)
print count
print “block1 time: {0}”.format(endb1 - startb1)
print “block2 time: {0}”.format(endb2 - startb2)
