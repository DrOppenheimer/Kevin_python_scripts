#!/usr/bin/env python
# simple script to check filename, size, and md5 for list of files(objects) in buckeet
# written to be run on AWS s3

import os
import os.path
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
parser.add_argument('-l','--list', help='list of urls', required=True)
parser.add_argument('-c','--cannedacl', default='public-read')
parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

def make_list_public(my_bucket, access_key, secret_key, my_list):    
    if args.gateway=='s3':
        con = boto.connect_s3(aws_access_key_id=args.accessKey, aws_secret_access_key=args.secretKey, is_secure = True)
    else:
        con = boto.connect_s3(aws_access_key_id=args.accessKey, aws_secret_access_key=args.secretKey, host=args.gateway, is_secure = True, calling_format = boto.s3.connection.OrdinaryCallingFormat(),)
    bucket=con.get_bucket(args.bucketName)
    with open(args.list) as f:
        for my_line in f:
            my_line = my_line.rstrip()
            splitline = my_line.split('/')
            file_name = splitline[ len(splitline) - 1 ]
            my_key = bucket.get_key(file_name)
            if args.debug==True:
                print("Filename: " + file_name)
                print( "Key_class: " + str(type(my_key)))
            my_key.set_canned_acl('public-read')
 
# Main
make_list_public(my_bucket=args.bucketName, access_key=args.accessKey, secret_key=args.secretKey, my_list=args.list)

