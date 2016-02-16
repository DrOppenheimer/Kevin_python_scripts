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
parser.add_argument('-k', '--md5_ref_dictionary', help='provide a list ( name \t md5 ) to compare against', default=0)
parser.add_argument('-p', '--proxy', action="store_true", help='run using \"with_proxy\"')
parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

def get_value(my_key, my_dictionary):
    if my_dictionary.has_key(my_key):
        my_value = my_dictionary.get(my_key)
        my_value = my_value.rstrip("\n")
        return (my_value)
    else:        
        return ("key does not exist")

def ftp_dl(line, fileName, access_key, secret_key, bucket_name, md5_ref_dictionary, proxy, debug):
    if debug==True:
        print "FILE_NAME: " + fileName
    line = line.rstrip("\n")
    tic = time.time()
    if proxy==True:
        wget_status=subprocess.call(["with_proxy wget", line])
    else:
        wget_status=subprocess.call(["wget", line])
    if debug==True:
        print ("WGET_STATUS: " + str(wget_status))
    dlTime = time.time() - tic
    if wget_status == 0:
        print ("calculating dl md5 " + fileName)   #### get md5 for file downloaded from ftp
        dlFileMd5 = generate_file_md5(fileName)    # uses function generate_file_md5 -- in your scripts
        if debug == True:
                print( fileName + " :: FTP_MD5 :: " + dlFileMd5  )
        if md5_ref_dictionary != 0:                       ### Option to check against reference md5
            ref_md5 = get_value(my_key=fileName, my_dictionary=md5_ref_dictionary)
            if debug == True:
                print( fileName + " :: REF_MD5 :: " + str(ref_md5)  )
            if dlFileMd5 == ref_md5:
                dl_md5_check = "md5_PASS"
            else:
                dl_md5_check = "md5_FAIL"
        else:
            ref_md5 = "NA"
            dl_md5_check = "NA"
        statinfo = os.stat(fileName)               #### get size of file downloaded from ftp
        dlSize = statinfo.st_size                  #size_gb = float(size) / (2**30)
        if debug==True:
            print(fileName + " :: dl_size :: " + str(dlSize))
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
        if debug == True:
                print( fileName + " :: s3_MD5 :: " + str(s3FileMd5)  )
        if md5_ref_dictionary != 0:                       ### Option to check against reference md5
            ref_md5 = get_value(my_key=fileName, my_dictionary=md5_ref_dictionary)
            if dlFileMd5 == ref_md5:
                ul_md5_check = "md5_PASS"
            else:
                ul_md5_check = "md5_FAIL"
        else:
            ref_md5 = "NA"
            ul_md5_check = "NA"
        s3Size=bucket.lookup(fileName).size        ### Get the size for the file on s3
        if debug==True:
            print(fileName + " :: s3_size :: " + str(s3Size))
        print ("printing to log " + fileName)      #### print to log
        if dlSize > 4*(2**30):
            log_string = fileName + '\t' + ref_md5 + '\t' + str(dlSize) + '\t' + str(dlFileMd5) + '\t' + dl_md5_check + '\t' + str(dlTime) + '\t' + str(s3Size) + '\t' + str(s3FileMd5) + '\t' + ul_md5_check + '\t' + str(ulTime) + '\t' + "File > 4Gb (4*(2^30) bytes), used multipart upload - upload md5 WILL NOT match dl md5" '\n'
        else:
            log_string = fileName + '\t' + ref_md5 + '\t' + str(dlSize) + '\t' + str(dlFileMd5) + '\t' + dl_md5_check + '\t' + str(dlTime) + '\t' + str(s3Size) + '\t' + str(s3FileMd5) + '\t' + ul_md5_check + '\t' + str(ulTime) + '\n'
        print ("Done processing sample ( " + str(sample) + " ) :: " + fileName)
        LOGFILE.write(log_string)
        LOGFILE.flush()
        return wget_status
    else:
        remove_status=subprocess.call(["rm", fileName])
        if remove_status != 0:
            log_string = fileName + '\t' + " :: download and/or rm failed" + '\n'
            LOGFILE.write(log_string)
            LOGFILE.flush()
            #log_string = fileName + '\t' + "wget failed" + '\n'
            #LOGFILE.write(log_string)
            #LOGFILE.flush()
            return wget_status

### MAIN ###

#read in compare list if option is specified
# read in compare list if option is specified
#     my_md5_ref_dictionary = {}
#     for line in open(args.md5_ref_dictionary):
#         (key,val) = line.split('\t')
#         my_md5_ref_dictionary[key] = val
# else:
#     my_md5_ref_dictionary = 0 
if args.md5_ref_dictionary != 0:
    my_md5_ref_dictionary = {}
    with open(args.md5_ref_dictionary) as f:
        for line in f:
            line = line.rstrip("\n")
            (key, val) = line.split('\t')
            my_md5_ref_dictionary[key] = val
else:
    my_md5_ref_dictionary = 0

    
# heavy lifting 
LOGFILE = open('./' + args.list + '.ul_log.txt', 'w+')
LOGFILE.write('file_name' + '\t' + 'ref_md5' +'\t' + 'local_size(bytes)' + '\t' + 'local_md5' + '\t' + 'local_md5_check' + '\t' + 'dl_time(s)' + '\t' + 's3_size(bytes)' + '\t' + 's3_md5' + '\t' + 's3_md5_check' + '\t' + 'ul_time(s)' + '\n')
LOGFILE.flush()
sample=0

with open(args.list) as f:
    for my_line in f:
        sample += 1
        splitLine = my_line.split("/")
        my_fileName = splitLine[ len(splitLine) - 1 ]
        my_fileName = my_fileName.rstrip("\n")
        print ("Processing sample ( " + str(sample) + " ) :: " + my_fileName)
        ftp_status=1
        my_attempt=0
        if my_attempt <= args.retry:
            if ftp_status != 0:
                my_attempt += 1
                if args.debug == True:
                    print("Attempt " + str(my_attempt))
                time.sleep(1)
                print("STARTING download and upload attempt ( " + str(my_attempt) + " ) for " + my_fileName)
                ftp_status=ftp_dl(line=my_line, fileName=my_fileName, access_key=args.access_key, secret_key=args.secret_key, bucket_name=args.bucket_name, md5_ref_dictionary=my_md5_ref_dictionary, proxy=args.proxy, debug=args.debug)
                if ftp_status == 0:
                    print(my_fileName + " download and upload FINISHED on attempt ( " + str(my_attempt) + " )")
                if args.debug==True:
                    print( "FTP_STATUS: " + str(ftp_status) )
                if my_attempt == args.retry:
                    if ftp_status != 0:
                        print("final download attempt ( " + str(my_attempt) + " ) FAILED")
            else:
                print(my_fileName + "download and upload FINISHED on attempt ( " + str(my_attempt) + " )")

