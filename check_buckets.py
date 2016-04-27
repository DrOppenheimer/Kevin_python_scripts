#!/usr/bin/env python
# simple script to check filename, size, and md5 for list of files(objects) in buckeet
# written to be run on AWS s3

import os
import time
import hashlib
import argparse
import boto
from boto.s3.key import Key
import boto.s3.connection

parser = argparse.ArgumentParser(description='Script to list buckets in connection')
parser.add_argument('-g','--gateway', help='gateway/host, amazon by default, specify any other', default='https://bionimbus-objstore.opensciencedatacloud.org')
parser.add_argument('-a','--accessKey', help='user', required=True)
parser.add_argument('-s','--secretKey', help='password', required=True)
parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

#def check_md5_size(my_bucket="bucket_name", access_key="some_key", secret_key="some_secret_key", md5_script="/home/ubuntu/git/Kevin_python_scripts/generate_file_md5.py"):
def check_md5_size(access_key, secret_key):    
    #execfile(md5_script)
    log_file="check_buckets.log"
    LOGFILE=open('./' + log_file, 'w+')
    #LOGFILE.write('object_name' + '\t' + 'size(bytes)' + '\t' + 'size(Gb)'+ '\t' + 'md5' + '\t' + 'dl_time(s)' + '\n')
    LOGFILE.write('bucket_name' + '\t' + 'creation_date' + '\n')
    con = boto.connect_s3(aws_access_key_id=args.accessKey, aws_secret_access_key=args.secretKey, host=args.gateway, is_secure = True, calling_format = boto.s3.connection.OrdinaryCallingFormat(),)
    for bucket in con.get_all_buckets():
        name = bucket.name,
        created = bucket.creation_date
        log_string = str(name) + '\t' + str(created) + '\n'
        LOGFILE.write(log_string)
        LOGFILE.flush() 
        
# Main
check_md5_size(access_key=args.accessKey, secret_key=args.secretKey)



