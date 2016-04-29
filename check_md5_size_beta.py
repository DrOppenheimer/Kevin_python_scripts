#!/usr/bin/env python
# Tool to get the "actual" size and md5 for all objects in a bucket
# This means that it has to download the objects and calculate the stats
# locally (anything uploaded with multipart upload, md5 on s3 will be for the last part, not the entire file)


import os
os.environ['PYTHONPATH']='$PYTHONPATH:/home/ubuntu/git/Kevin_python_scripts' # add path to pythonpath
import sys
import json
import mmap
import logging
import argparse
import time
import hashlib
import subprocess
import boto
import boto.s3.connection
from boto.s3.key import Key
from generate_file_md5 import generate_file_md5
try:
    import multipart_upload # make sure is in PYTHONPATH
except:
    print 'multipart_upload did not import -- is the script in PYTHONPATH?'
    sys.exit(1) 

parser = argparse.ArgumentParser(description='Script to download objects in list from specified host/bucket -- calculate their size and md5 locally')
parser.add_argument('-l','--list', help='file with list of file names', default="test")
parser.add_argument('-g','--gateway', help='gateway/host, griffin-objstore.opensciencedatacloud.org or s3.amazonaws.com', default='griffin-objstore.opensciencedatacloud.org')
parser.add_argument('-b','--bucketName', help='Name fot the bucket', default='1000_genome_exome')
parser.add_argument('-a','--accessKey', help='accesskey', required=True)
parser.add_argument('-s','--secretKey', help='secretkey', required=True)
#parser.add_argument('-m','--md5script', help='endpoint', default='~/git/Kevin_python_scripts/generate_file_md5.py')
parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

# Print a header to the log file
LOGFILE = open('./' + args.list + '.md5_size_log.txt', 'w+')
LOGFILE.write('filename\tmd5\tsize(bytes)\tdl_Time\tProcessing_time')
LOGFILE.write('\n')
LOGFILE.flush()

# process through list of files
file_count = 0
with open(args.list) as f:
    for my_line in f:
        file_count += 1
        ### get a filename from the list
        my_file_name = my_line.strip()
        print('Processing ' + str(my_file_name) + ' ( ' + str(file_count) + ' )')
        ### download file
        conn = boto.connect_s3(
            aws_access_key_id = args.accessKey,
            aws_secret_access_key = args.secretKey,
            host = args.gateway,
            #proxy = 'http://cloud-proxy:3128',
            #proxy_port = 3128,
            #is_secure=True,               # uncomment if you are not using ssl
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )
        bucket = conn.get_bucket(args.bucketName)
        key = bucket.get_key(my_file_name)
        tic = time.time()
        key.get_contents_to_filename(my_file_name)
        dlTime = time.time() - tic
        ### calculate md5 & size
        tic = time.time()
        dlFileMd5, dlSize = generate_file_md5(my_file_name)
        psTime = time.time() - tic
        ### write stats to log
        log_str = '\t'.join([str(my_file_name), str(dlFileMd5), str(dlSize), str(dlTime), str(psTime)])
        LOGFILE.write(log_str)
        LOGFILE.write('\n')
        LOGFILE.flush()
        os.remove(my_file_name)
