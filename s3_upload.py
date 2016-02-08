#!/usr/bin/env python
# simple script to upload a file to selected bucket
# written to be run on AWS s3
def s3_upload(my_bucket="bucket_name", local_file_path="some/file/path.txt", access_key="some_key", secret_key="some_secret_key"):
    #import os
    #import time
    #import hashlib
    import boto
    from boto.s3.key import Key
    import boto.s3.connection
    # get the filename from the file path
    splitLine = local_file_path.split("/")
    fileName = splitLine[ len(splitLine) - 1 ]
    #fileName = splitLine[1]
    fileName = fileName.rstrip("\n")
    # create a connection to the bucket
    con = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket=con.get_bucket(my_bucket)
    key=Key(name=fileName, bucket=bucket)
    # upload
    key.set_contents_from_filename(fileName)
    print("Done uploading :" + str(fileName))
