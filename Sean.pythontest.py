#!/usr/bin/python                                                                                                                        
import boto, boto.s3.connection, json, requests, os, sys

for env in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY']:
   os.environ[env]=''

access_key = 'AKEY'
secret_key = 'SKEY'

conn = boto.connect_s3(aws_access_key_id= access_key, aws_secret_access_key= secret_key, host='rados-bionimbus-pdc.opensciencedatacloud.\
org', is_secure=True, calling_format = boto.s3.connection.OrdinaryCallingFormat())

for bucket in conn.get_all_buckets():
  print(bucket.name)

conn = boto.connect_s3(aws_access_key_id= access_key, aws_secret_access_key= secret_key, host='bionimbus-objstore.opensciencedatacloud.o\
rg', is_secure=True, calling_format = boto.s3.connection.OrdinaryCallingFormat())

for bucket in conn.get_all_buckets():
  print(bucket.name)

