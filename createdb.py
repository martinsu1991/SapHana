import os
import gc
import re
import gzip
import json
import datetime
import dbapi
import sys


client = dbapi.connect('10.1.0.160', 30015, 'SYSTEM', 'Xia550505')
cursor = client.cursor()
if client.isconnected() == False:
    print 'error connection'
    sys.exit()

temp = cursor.execute('select max(id) from temp2')
data = cursor.fetchall()
initCount = data[0][0]


start_file = 'tracking.log-20140126.gz'
end_file = 'tracking.log-20140126.gz'

start3 = datetime.datetime.now()
count = initCount
session_n = 0
for num in ['/home/workload/workload/204']:
    print '====================================', num, ' ========================================'
    for root, dirs, files in os.walk(num + '/log'):
        if root == num + '/log':
            for log_file in files:
                if log_file >= start_file and log_file <= end_file:
                    #print log_file
                    logs = gzip.open(root + '/' + log_file, 'rt')
                    for log in logs:
                        try:
                                start1 = datetime.datetime.now()
                                obj = json.loads(log)
                                end1 = datetime.datetime.now()
                                # if 'ip' in obj and 'time' in obj and 'event_type' in obj:
                                count += 1    
                                print count
                               # ret = cursor.execute('insert into temp1 values(?,?,?,?,?,?,?,?,?,?)',\
                               # (count, obj['username'], obj['event_source'], obj['event_type'],obj['ip'],obj['agent'],\
                               # obj['page'],obj['host'], obj['time'], obj['event'])  )
                                start2 = datetime.datetime.now()
                                ret = cursor.execute('insert into temp2 values(?,?,?,?)',\
                                (count, obj['ip'],obj['time'],obj['event_type']))
                                end2 = datetime.datetime.now()
                                print 'text time {0}'.format(end1 - start1)
                                print 'insert time {0}'.format(end2 - start2)
                                #break
                           # else:                   
                           #     print obj
                        except Exception as err:
                            print err
                            print objut
                            continue
 
end3 = datetime.datetime.now()
print 'total time {0}'.format(end3 - start3)
print 'over~!'
print count
