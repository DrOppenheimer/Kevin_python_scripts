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

parser = argparse.ArgumentParser(description='Script to mint ARK ids - handles only simple case of ids for a single file - but with multiple urls')
parser.add_argument('-g','--gateway', help='gateway/host, amazon by default, specify any other', default='s3')
parser.add_argument('-b','--bucketName', help='file with list of metadata filename\tsize_in_bytes\tmd5\turl1\turl...     ', required=True)
parser.add_argument('-a','--accessKey', help='user', required=True)
parser.add_argument('-s','--secretKey', help='password', required=True)
parser.add_argument('-m','--md5script', help='endpoint', default='~/git/Kevin_python_scripts/generate_file_md5.py')
parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

#def check_md5_size(my_bucket="bucket_name", access_key="some_key", secret_key="some_secret_key", md5_script="/home/ubuntu/git/Kevin_python_scripts/generate_file_md5.py"):
def check_md5_size(my_bucket, access_key, secret_key, md5_script):    
    #execfile(md5_script)
    log_file=my_bucket + "." + args.gateway + ".md5_size_check.log"
    LOGFILE=open('./' + log_file, 'w+')
    #LOGFILE.write('object_name' + '\t' + 'size(bytes)' + '\t' + 'size(Gb)'+ '\t' + 'md5' + '\t' + 'dl_time(s)' + '\n')
    LOGFILE.write('object_name' + '\t' + 'size(bytes)' + '\t' + 'size(Gb)'+ '\t' + 'md5' + '\n')
    if args.gateway=='s3':
        con = boto.connect_s3(aws_access_key_id=args.accessKey, aws_secret_access_key=args.secretKey, is_secure = True)
    else:
        con = boto.connect_s3(aws_access_key_id=args.accessKey, aws_secret_access_key=args.secretKey, host=args.gateway, is_secure = True, calling_format = boto.s3.connection.OrdinaryCallingFormat(),)
    bucket=con.get_bucket(args.bucketName)
    ### Get the number of items in the bucket
    count=0
    count2=0
    for key in bucket.list():
        count+=1
    for key in bucket.list():
        ### Print percent complete
        count2+=1
        percentComplete = ( float(count2) / count ) * 100  
        print("Percent complete: ", str(round(percentComplete,2)))
        ### download object, get the time need to download # Removed -- you don't have to download to get the stats! (size and md5 anyways) - code updated 2-10-16
        #tic = time.time()
        #key.get_contents_to_filename(key.name)
        #dlTime = time.time() - tic
        ### get md5 of downloaded object
        print ("calculating md5 " + key.name)
        #fileMd5 = generate_file_md5(key.name) # uses function generate_file_md5 -- in your scripts
        ### md5 of object without downloading it
        fileMd5=bucket.get_key(key).etag[1 :-1]
        ### get size in Gb without downloading it (bytes and Gb size are in the report)
        size_gb = float(key.size) / (2**30)
        ### remove local copy of object
        #print ("delete local copy of " + key.name)
        #os.remove(key.name)
        ### write stats to output
        #log_string = key.name + '\t' + str(key.size) + '\t' + str(size_gb) + '\t' + str(fileMd5) + '\t' + str(dlTime) + '\n'
        log_string = key.name + '\t' + str(key.size) + '\t' + str(size_gb) + '\t' + str(fileMd5) + '\n'
        LOGFILE.write(log_string)
        LOGFILE.flush()

# Main
check_md5_size(my_bucket=args.bucketName, access_key=args.accessKey, secret_key=args.secretKey, md5_script=args.md5script)



