#!/usr/bin/env python
# simple script to change the acl for a selected file
# written to be run on AWS s3

# import packages
import argparse
import boto
from boto.s3.key import Key
import boto.s3.connection

# parse arguments
parser = argparse.ArgumentParser(description='Script to upload a single file to s3 object stores')
parser.add_argument('-g','--gateway', help='gateway/host', default='bionimbus-objstore.opensciencedatacloud.org')
parser.add_argument('-b','--bucket', help='bucket name')
parser.add_argument('-o','--object', help='object')
parser.add_argument('-a','--a_key', help='access key')
parser.add_argument('-s','--s_key', help='secret key')
parser.add_argument('-c','--acl', default='public-read',help='acl value')
args = parser.parse_args()


def s3_upload(my_bucket=args.bucket, host=args.gateway, my_object=args.object, access_key=args.a_key, secret_key=args.s_key, acl=args.acl, is_secure=True, calling_format = boto.s3.connection.OrdinaryCallingFormat()):
    #import os
    #import time
    #import hashlib
    import boto
    from boto.s3.key import Key
    import boto.s3.connection
    # create a connection to the bucket
    con = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket=con.get_bucket(my_bucket)
    # get object key
    key=bucket.get_key(my_object)
    # get and print the current acl
    acltext=key.get_acl()
    print 'Current acl: ' + str(acltext)
    # set new acl
    key.set_acl(acl)
    # get and print the new acl
    acltext=key.get_acl()
    print 'New acl:     ' + str(acltext)
