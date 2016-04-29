#!/usr/bin/env python
# simple script to upload a file to selected bucket
# written to be run on AWS s3

# import packages
import os
os.environ['PYTHONPATH']='/home/ubuntu/git/Kevin_python_scripts' # add path to pythonpath
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
 
def run():
    # parse arguments
    parser = argparse.ArgumentParser(description='Script to upload a single file to s3 object stores')
    parser.add_argument('-g','--gateway', help='gateway/host', default='bionimbus-objstore.opensciencedatacloud.org')
    parser.add_argument('-b','--bucket_name', help='bucket name')
    parser.add_argument('-f','--file_name', help='path/file to be uploaded')
    parser.add_argument('-a','--access_key', help='access key')
    parser.add_argument('-s','--secret_key', help='secret key')
    parser.add_argument('-d','--debug', help='debug mode')
    args = parser.parse_args() 
    print 'File name: ' + str(args.file_name)
    status, ulTime = upload_file(file_name=args.file_name, bucket_name=args.bucket_name, access_key=args.access_key, secret_key=args.secret_key, gateway=args.gateway, debug=args.debug)
    print 'Status:      ' + str(status)
    print 'Upload Time: ' + str(ulTime)
    
def upload_file(file_name, bucket_name, access_key, secret_key, gateway, debug):
    #print 'File name: ' + str(file_name)
    tic = time.time()
    print ("SUB :: uploading :: " + file_name)            #### upload to s4
    # import keys from args to env vars
    os.environ['AWS_ACCESS_KEY_ID'] = access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
    # key_name is the filename
    key_name = os.path.basename(file_name)
    print 'File name: ' + str(file_name)
    print 'Key  name: ' + str(key_name)
    status = subprocess.call(['aws', 's3', 'cp', file_name, 's3://{}/{}'.format(bucket_name, key_name), '--endpoint-url', 'https://'+gateway], env=os.environ)
    ulTime = time.time() - tic
    return status, ulTime
    # unset env vars created above for the key pair
    print ("SUB :: uploading :: " + file_name)            #### upload to s4
    os.system('unset AWS_ACCESS_KEY_ID')
    os.system('unset AWS_SECRET_ACCESS_KEY')
    
if __name__ == '__main__':
    run()
    

# def s3_upload(my_bucket=args.bucket, host=args.gateway, local_file_path=args.file, access_key=args.a_key, secret_key=args.s_key, is_secure=True, calling_format = boto.s3.connection.OrdinaryCallingFormat()):
#     #import os
#     #import time
#     #import hashlib
#     import boto
#     from boto.s3.key import Key
#     import boto.s3.connection
#     # get the filename from the file path
#     splitLine = local_file_path.split("/")
#     fileName = splitLine[ len(splitLine) - 1 ]
#     #fileName = splitLine[1]
#     fileName = fileName.rstrip("\n")
#     # create a connection to the bucket
#     con = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#     bucket=con.get_bucket(my_bucket)
#     key=Key(name=fileName, bucket=bucket)
#     # upload
#     key.set_contents_from_filename(fileName)
#     print("Done uploading :" + str(fileName))
