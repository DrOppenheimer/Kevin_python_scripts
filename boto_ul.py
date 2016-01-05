#!/usr/bin/python

import argparse
import boto
import boto.s3.connection
#import os
#import re
#import json
#import os.path
#from os.path import basename
#from pprint import pprint

print("example file: \"ERR_tar.12Mb.gz\"")

parser = argparse.ArgumentParser(description='Simple script to perform a boto download')
parser.add_argument('-f','--file', help='file to download', required=True, default="test")
parser.add_argument('-a','--access_key', help='file to download', required=True)
parser.add_argument('-s','--secret_key', help='file to download', required=True)
parser.add_argument('-b','--bucket_name', help='file to download', required=True)
parser.add_argument('-g','--gateway', help='file to download', required=True)
parser.add_argument('-p','--useparcel', help='file to download', required=False, action='store_true', default=False)
args = parser.parse_args()

### Open connection
if args.useparcel:
    print "\n\nBoto download Using Parcel\n\n"
    conn = boto.connect_s3(
    aws_access_key_id = args.access_key,
    aws_secret_access_key = args.secret_key,
    host = args.gateway,
    port = 9000,
    #is_secure=False, # uncomment if you are not using ssl
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )
else:
    print "\n\nBoto download Without Parcel\n\n"
    conn = boto.connect_s3(
    aws_access_key_id = args.access_key,
    aws_secret_access_key = args.secret_key,
    host = args.gateway,
    #is_secure=False, # uncomment if you are not using ssl
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

### get existing bucket | create new::
bucket = conn.get_bucket(args.bucket_name)

### Upload object to bucket::
key = my_bucket.new_key(args.file)
key.set_contents_from_filename(args.file)
