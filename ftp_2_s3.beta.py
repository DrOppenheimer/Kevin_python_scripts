#!/usr/bin/env python

import argparse
import os
import time
import hashlib
import subprocess
import boto
import boto.s3.connection
import multipart_upload
from boto.s3.key import Key
from generate_file_md5 import generate_file_md5

parser = argparse.ArgumentParser(description='Simple script to perform a boto download')
parser.add_argument('-l','--list', help='file with list of ftp addresses', required=True, default="test")
parser.add_argument('-a','--access_key', help='access key', required=True)
parser.add_argument('-s','--secret_key', help='secret key', required=True)
parser.add_argument('-b','--bucket_name', help='bucket name', default="1000_genome_exome")
parser.add_argument('-c','--credentials', help='credentials file for multipart upload: access_key, secret_key', required=True)
parser.add_argument('-r', '--retry', help='number of times to retry each download', default=10)
parser.add_argument('-k', '--compare_md5', help='provide a list ( name \t md5 ) to compare against', default=0)
args = parser.parse_args()

def get_value(my_key, my_dictionary):
    if my_dictionary.has_key(my_key):
        my_value = my_dictionary.get(my_key)
        my_value = my_value.rstrip("\n")
        return (my_value)
    else:        
        return ("key does not exist")

def ftp_dl(line, fileName, access_key, secret_key, bucket_name, compare_md5, md5_ref_dictionary):
    line = line.rstrip("\n")
    tic = time.time()
    wget_status=subprocess.call(["wget", line])
    dlTime = time.time() - tic
    if wget_status == 0:
        print ("calculating dl md5 " + fileName)   #### get md5 for file downloaded from ftp
        dlFileMd5 = generate_file_md5(fileName)    # uses function generate_file_md5 -- in your scripts
        if compare_md5 != 0:                       ### Option to check against reference md5
            ref_md5 = get_value(my_key=fileName, my_dictionary=md5_ref_dictionary)
            if dlFileMd5 == ref_md5:
                dl_md5_check = "md5_PASS"
            else:
                dl_md5_check = "md5_FAIL"
        else:
            ref_md5 = "NA"
            dl_md5_check = "NA"
        statinfo = os.stat(fileName)               #### get size of file downloaded from ftp
        dlSize = statinfo.st_size                  #size_gb = float(size) / (2**30)
        print ("uploading " + fileName)            #### upload to s3
        tic = time.time()
        con = boto.connect_s3(aws_access_key_id=args.access_key, aws_secret_access_key=args.secret_key )
        bucket=con.get_bucket(args.bucket_name)
        key=Key(name=fileName, bucket=bucket)
        if dlSize > 4*(2**30): # use multipart upload for anything larger than 4Gb 
            upload_string = "multipart_upload.py" + " creds " + args.bucket_name + " " + fileName + " < " + fileName 
            os.system(upload_string)
        else:
            key.set_contents_from_filename(fileName) # maybe do check and multipart for anything over a certain size
        ulTime = time.time() - tic
        print ("delete local copy of " + fileName) ### remove local copy of file
        remove_status=subprocess.call(["rm", fileName])
        if remove_status != 0:
            log_string = fileName + '\t' + "rm failed" + '\n'
            LOGFILE.write(log_string)
            LOGFILE.flush()
        s3FileMd5=bucket.get_key(key).etag[1 :-1]  ### Get the md5 for the file on s3
        if compare_md5 != 0:                       ### Option to check against reference md5
            ref_md5 = get_value(my_key=fileName, my_dictionary=md5_ref_dictionary)
            if dlFileMd5 == ref_md5:
                ul_md5_check = "md5_PASS"
            else:
                ul_md5_check = "md5_FAIL"
        else:
            ref_md5 = "NA"
            ul_md5_check = "NA"
        s3Size=bucket.lookup(fileName).size                            ### Get the size for the file on s3
        print ("printing to log " + fileName)      #### print to log
        if dlSize > 4*(2**30):
            log_string = fileName + '\t' + ref_md5 + '\t' + str(dlSize) + '\t' + str(dlFileMd5) + '\t' + dl_md5_check + '\t' + str(dlTime) + '\t' + str(s3Size) + '\t' + str(s3FileMd5) + '\t' + ul_md5_check + '\t' + str(ulTime) + '\t' + "File > 4Gb (4*(2^30) bytes), used multipart upload - upload md5 WILL NOT match dl md5" '\n'
        else:
            log_string = fileName + '\t' + ref_md5 + '\t' + str(dlSize) + '\t' + str(dlFileMd5) + '\t' + dl_md5_check + '\t' + str(dlTime) + '\t' + str(s3Size) + '\t' + str(s3FileMd5) + '\t' + ul_md5_check + '\t' + str(ulTime) + '\n'
        print ("Done processing sample ( " + str(sample) + " ) :: " + fileName)
        LOGFILE.write(log_string)
        LOGFILE.flush()
    else:
        remove_status=subprocess.call(["rm", fileName])
        if remove_status != 0:
            log_string = fileName + '\t' + "rm of failed download failed" + '\n'
            LOGFILE.write(log_string)
            LOGFILE.flush()
            log_string = fileName + '\t' + "wget failed" + '\n'
            LOGFILE.write(log_string)
            LOGFILE.flush()
            return wget_status

### MAIN ###

# read in compare list if option is specified
if args.compare_md5 != 0:
    md5_ref_dictionary = {}
    with open(args.compare_md5) as f:
        for line in f:
            (key, val) = line.split('\t')
            md5_ref_dictionary[key] = val

# heavy lifting 
LOGFILE = open('./' + args.list + '.ul_log.txt', 'w+')
LOGFILE.write('file_name' + '\t' + 'ref_md5' +'\t' + 'local_size(bytes)' + '\t' + 'local_md5' + '\t' + 'local_md5_check' + '\t' + 'dl_time(s)' + '\t' + 's3_size(bytes)' + '\t' + 's3_md5' + '\t' + 's3_md5_check' + '\t' + 'ul_time(s)' + '\n')
LOGFILE.flush()
sample=0
ftp_status=0
with open(args.list) as f:
    for line in f:
        sample += 1
        splitLine = line.split("/")
        fileName = splitLine[ len(splitLine) - 1 ]
        fileName = fileName.rstrip("\n")
        print ("Processing sample ( " + str(sample) + " ) :: " + fileName)
        while ftp_status < args.retry:
            ftp_status=ftp_dl(line=line, fileName=fileName, access_key=args.access_key, secret_key=args.secret_key, bucket_name=args.bucket_name, compare_md5=args.compare_md5, md5_ref_dictionary=args.md5_ref_dictionary)        
