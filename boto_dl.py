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
parser.add_argument('-u','--useparcel', help='file to download', required=False)
#parser.add_argument('-p','--port', help='only used with -u|--useparcel, port number for parcel', default="9000")

#parser.add_argument('-b','--bar', help='Description for bar argument', required=True)
#args = vars(parser.parse_args())
args = parser.parse_args()

### Open connection
if args.useparcel == "useparcel":
    conn = boto.connect_s3(
    aws_access_key_id = args.access_key,
    aws_secret_access_key = args.secret_key,
    host = args.gateway,
    #port = 9000
    #is_secure=False, # uncomment if you are not using ssl
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )
else:
    conn = boto.connect_s3(
    aws_access_key_id = args.access_key,
    aws_secret_access_key = args.secret_key,
    host = args.gateway,
    port = 9000,
    #is_secure=False, # uncomment if you are not using ssl
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

### get existing bucke | create newt::
bucket = conn.get_bucket(args.bucket_name)

### downloading an object to local::
key = bucket.get_key(args.file)
key.get_contents_to_filename('./' + args.file)

# key = conn.get_bucket(args.bucket).get_key(args.key)
#     if key is None:
#         key = conn.get_bucket(args.bucket).new_key(args.key)

#     key.set_contents_from_file(sys.stdin)


