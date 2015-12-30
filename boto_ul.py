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

parser = argparse.ArgumentParser(description='Simple script to perform a boto upload')
parser.add_argument('-f','--file', help='file to upload', required=True)
#parser.add_argument('-b','--bar', help='Description for bar argument', required=True)
#args = vars(parser.parse_args())
args = parser.parse_args()

#print("ARGS: "+ args)
print("FILE: "+ args.file)

access_key=
secret_key=
bucket_name=
gateway=

### Open connection
conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = gateway,
        #is_secure=False, # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

### get existing bucke | create newt::
bucket = conn.get_bucket(bucket_name)

### Upload object to bucket::
key = my_bucket.new_key(args.file)
	key.set_contents_from_filename(args.file)
