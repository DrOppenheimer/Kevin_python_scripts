#!/usr/bin/env python

import argparse
import os
import time
import hashlib
import subprocess
import boto
import boto.s3.connection
from boto.s3.key import Key
from generate_file_md5 import generate_file_md5

parser = argparse.ArgumentParser(description='Simple script to perform a boto download')
parser.add_argument('-l','--list', help='file with list of ftp addresses', required=True, default="test")
parser.add_argument('-a','--access_key', help='access key', required=True)
parser.add_argument('-s','--secret_key', help='secret key', required=True)
parser.add_argument('-b','--bucket_name', help='bucket name', default="1000_genome_exome")
args = parser.parse_args()

#def ftp_2_s3(my_file=args.list, access_key=args.access_key, args.secret_key="some_secret_key"):
#execfile("/home/ubuntu/git/Kevin_python_scripts/generate_file_md5.py")
#my_bucket="1000_genome_exome"
LOGFILE = open('./' + args.list + '.ul_log.txt', 'w+')
LOGFILE.write('file_name' + '\t' + 'local_size(bytes)' + '\t' + 'local_md5' + '\t' + 'dl_time(s)' + '\t' + 's3_size(bytes)' + '\t' + 's3_md5' + '\t' + 'ul_time(s)' + '\n')
LOGFILE.flush()
sample=0
with open(args.list) as f:
        for line in f:
            sample += 1
            splitLine = line.split("/")
            fileName = splitLine[ len(splitLine) - 1 ]
            fileName = fileName.rstrip("\n")
            print ("Processing sample ( " + str(sample) + " ) :: " + fileName)
            #### download
            print ("downloading " + fileName)
            # create a string to perform the download
            download_string = "wget " + line
            # system call to do the download
            tic = time.time()
            #os.system(download_string)
            ##subprocess.Popen(["wget", line])
            line = line.rstrip("\n")
            wget_status=subprocess.call(["wget", line])
            if wget_status==1:
                dlTime = time.time() - tic
                #### get md5 for file downloaded from ftp
                print ("calculating dl md5 " + fileName)
                dlFileMd5 = generate_file_md5(fileName) # uses function generate_file_md5 -- in your scripts
                #### get size of file downloaded from ftp
                statinfo = os.stat(fileName)
                dlSize = statinfo.st_size
                #size_gb = float(size) / (2**30)
                #### upload to s3
                print ("uploading " + fileName)		
                tic = time.time()
                con = boto.connect_s3(aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key )
                bucket=con.get_bucket(args.bucket_name)
                key=Key(name=fileName, bucket=bucket)
                key.set_contents_from_filename(fileName)
                ulTime = time.time() - tic
                ### remove local copy of file
                print ("delete local copy of " + fileName)
                os.remove(fileName)
                ### Get the md5 for the file on s3
                s3FileMd5=bucket.get_key(key).etag[1 :-1]
                ### Get the size for the file on s3
                s3Size=key.size
                #### print to log
                print ("printing to log " + fileName)
                log_string = fileName + '\t' + str(dlSize) + '\t' + str(dlFileMd5) + '\t' + str(dlTime) + '\t' + str(s3Size) + '\t' + str(s3FileMd5) + '\t' + str(ulTime) + '\n'
                print ("Done processing sample ( " + str(sample) + " ) :: " + fileName)
                LOGFILE.write(log_string)
                LOGFILE.flush()
            else:
                log_string = fileName + '\t' + "wget failed" + '\n'
                LOGFILE.write(log_string)
                LOGFILE.flush()
    
