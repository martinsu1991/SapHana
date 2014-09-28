import os
import gc
import re
import gzip
import json
import datetime
import pickle

count = 0
start_file = 'tracking.log-20130927.gz'
end_file = 'tracking.log-20140428.gz'
ip_n = 0

for num in ['/Users/yuhansu/Desktop/workload/workload/204', '/Users/yuhansu/Desktop/workload/workload/205']:
    print '====================================', num, ' ========================================'
    for root, dirs, files in os.walk(num + '/log'):
        if root == num + '/log':
            for log_file in files:
                if log_file >= start_file and log_file <= end_file: 
                    print log_file,ip_n
                    tmp=root+ "/" +log_file
                    logs = gzip.open(tmp ,'rb')


                    for log in logs:
                        try: 
                            obj = json.loads(log)
                            count += 1
                            print count
                        except Exception, e:
                            print e
                            continue


print 'total count {0}'.format(count)
       
