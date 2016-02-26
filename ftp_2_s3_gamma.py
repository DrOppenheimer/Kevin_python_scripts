#!/usr/bin/env python

# test with
# ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000_genomes_project/data/PJL/HG03633/exome_alignment/HG03633.alt_bwamem_GRCh38DH.20150826.PJL.exome.cram
# 131171985 bytes (0.12Gb) # smallest of the set of exome files
# and these for testing multipart upload
# ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000_genomes_project/data/TSI/NA20787/exome_alignment/NA20787.alt_bwamem_GRCh38DH.20150826.TSI.exome.cram
# 4589641085 bytes (4.27Gb)
# ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000_genomes_project/data/ASW/NA20313/exome_alignment/NA20313.alt_bwamem_GRCh38DH.20150826.ASW.exome.cram
# 24010297995 bytes (22.36Gb)
# 
# 

import sys
#import json
import mmap
#import logging
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



def run():
    parser = argparse.ArgumentParser(description='Simple script to perform a boto download')
    parser.add_argument('-l','--list', help='file with list of ftp addresses', required=True, default="test")
    parser.add_argument('-a','--access_key', help='access key')
    parser.add_argument('-s','--secret_key', help='secret key')
    parser.add_argument('-g','--gateway', help='s3 host/gateway', default='griffin-objstore.opensciencedatacloud.org')
    parser.add_argument('-b','--bucket_name', help='bucket name', default='1000_genome_exome')
    parser.add_argument('-r', '--retry', help='number of times to retry each download', default=10)
    parser.add_argument('-k', '--md5_ref_dictionary', help='provide a list ( name \t md5 ) to compare against', default=0)
    parser.add_argument('-p', '--proxy', action="store_true", help='run using \"with_proxy\"')
    parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
    parser.add_argument('-fd', '--force-download', action="store_true", help='force to download even if file exists')
    args = parser.parse_args()
    if args.proxy:
        os.environ['http_proxy'] = 'http://cloud-proxy'
        os.environ['https_proxy'] = 'http://cloud-proxy'

    # optional, you can also put secrets in {HOME}/aws/.credentials
    if args.access_key:
        os.environ['AWS_ACCESS_KEY_ID'] = args.access_key
    if args.secret_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = args.secret_key

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
            my_file_name = splitLine[ len(splitLine) - 1 ]
            my_file_name = my_file_name.rstrip("\n")
            print ("MAIN :: Processing sample ( " + str(sample) + " ) :: " + my_file_name)
            ftp_status=1
            my_attempt=0
            if my_attempt <= args.retry:
                if ftp_status != 0:
                    my_attempt += 1
                    if args.debug == True:
                        print("MAIN :: Attempt " + str(my_attempt))
                        print("MAIN :: Bucket_name: " + args.bucket_name)
                    time.sleep(1)
                    print("MAIN :: STARTING download and upload attempt ( " + str(my_attempt) + " ) for " + my_file_name)
                    ftp_status=ftp_download(
                        line=my_line, file_name=my_file_name, 
                        debug=args.debug, force_download=args.force_download)
                    if ftp_status == 0:
                        dl_md5_check = check_md5_and_size(file_name, my_md5_ref_dictionary)
                        if dl_md5_check == "md5_PASS":
                            upload_file(file_name, bucket_name, args.gateway, debug=args.debug)

                    return

                    if ftp_status == 0:
                        print("MAIN :: " + my_file_name + " download and upload FINISHED on attempt ( " + str(my_attempt) + " )")
                    if args.debug==True:
                        print( "MAIN :: FTP_STATUS: " + str(ftp_status) )
                    if my_attempt == args.retry:
                        if ftp_status != 0:
                            print("MAIN :: final download attempt ( " + str(my_attempt) + " ) FAILED")
                else:
                    print("MAIN :: " + my_file_name + "download and upload FINISHED on attempt ( " + str(my_attempt) + " )")


def get_value(my_key, my_dictionary):
    if my_dictionary.has_key(my_key):
        my_value = my_dictionary.get(my_key)
        my_value = my_value.rstrip("\n")
        return (my_value)
    else:        
        return ("key does not exist")

def ftp_download(line, file_name, debug, force_download):
    if debug==True:
        print "SUB :: FILE_NAME: " + file_name
    if not force_download and os.path.exists(file_name):
        if debug:
            print "SUB :: File {} exists, skip downloading from ftp".format(file_name)
        return 0, 0

    line = line.rstrip("\n")
    if debug==True:
        print("SUB :: Length line :: " + str(len(line))  )
    if len(line) == 0:
        sys.exit("SUB :: This line of the list is empty, terminating script")
    tic = time.time()
    if sys.platform == 'darwin':
        with open(file_name, 'wb') as f:
            wget_status=subprocess.call(["curl", line], env=os.environ, stdout=f)
    else:
        wget_status=subprocess.call(["wget", line], env=os.environ)
    if debug==True:
        print ("SUB :: WGET_STATUS :: " + str(wget_status))
    dlTime = time.time() - tic
    if wget_status !=0:
        remove_status=subprocess.call(["rm", file_name])
        if remove_status != 0:
            log_string = file_name + '\t' + " :: download and/or rm failed" + '\n'
            LOGFILE.write(log_string)
            LOGFILE.flush()
    return wget_status, dlTime


def check_md5_and_size(file_name, md5_ref_dictionary):
    print ("SUB :: calculating dl md5 :: " + file_name)   #### get md5 for file downloaded from ftp
    dlFileMd5, dlSize = generate_file_md5(file_name)    # uses function generate_file_md5 -- in your scripts
    if debug == True:
            print( "SUB :: " + file_name + " :: FTP_MD5 :: " + dlFileMd5  )
    if md5_ref_dictionary != 0:                       ### Option to check against reference md5
        ref_md5 = get_value(my_key=file_name, my_dictionary=md5_ref_dictionary)
        if debug == True:
            print( "SUB :: " + file_name + " :: REF_MD5 :: " + str(ref_md5)  )
        if dlFileMd5 == ref_md5:
            dl_md5_check = "md5_PASS"
        else:
            dl_md5_check = "md5_FAIL"
    else:
        ref_md5 = "NA"
        dl_md5_check = "NA"
    if debug==True:
        print("SUB :: " + file_name + " :: dl_size :: " + str(dlSize))
    return dl_md5_check


def upload_file(file_name, bucket_name, gateway, debug=True):
    print ("SUB :: uploading :: " + file_name)            #### upload to s4
    tic = time.time()
    key_name = os.path.basename(file_name)
    status = subprocess.call(['aws', 's3', 'cp', file_name, 's3://{}/{}'.format(bucket_name, key_name), '--endpoint-url', 'https://'+gateway], env=os.environ)
    
    ulTime = time.time() - tic
    return status, ulTime

def cleanup():
    print ("SUB :: delete local copy of " + file_name) ### remove local copy of file
    #remove_status=subprocess.call(["rm", file_name])
    delete_command = "sudo rm -f " + file_name
    remove_status=os.system(delete_command)
    if remove_status != 0:
        log_string = file_name + '\t' + "rm failed" + '\n'
        LOGFILE.write(log_string)
        LOGFILE.flush()
    s3FileMd5=bucket.get_key(key).etag[1 :-1]  ### Get the md5 for the file on s3
    #s3FileMd5="fix later"
    if debug == True:
            print( "SUB :: " + file_name + " :: s3_MD5 :: " + str(s3FileMd5)  )
    if md5_ref_dictionary != 0:                       ### Option to check against reference md5
        ref_md5 = get_value(my_key=file_name, my_dictionary=md5_ref_dictionary)
        if dlFileMd5 == ref_md5:
            ul_md5_check = "md5_PASS"
        else:
            ul_md5_check = "md5_FAIL"
    else:
        ref_md5 = "NA"
        ul_md5_check = "NA"
    s3Size=bucket.lookup(file_name).size        ### Get the size for the file on s3
    if debug==True:
        print("SUB :: " + file_name + " :: s3_size :: " + str(s3Size))
    print ("SUB :: printing to log " + file_name)      #### print to log
    if dlSize > 4*(2**30):
        # download the object from the object store and calculate md5 (only for objects that used multipart upload)
        remove_status=subprocess.call(["rm", file_name])
        key = bucket.get_key(file_name)
        key.get_contents_to_file_name(file_name)
        s3FileMd5 = generate_file_md5(file_name)
        statinfo = os.stat(file_name)               #### get size of file downloaded from ftp
        s3Size = statinfo.st_size                  #size_gb = float(size) / (2**30)
        if md5_ref_dictionary != 0:                       ### Option to check against reference md5
            ref_md5 = get_value(my_key=file_name, my_dictionary=md5_ref_dictionary)
            if s3FileMd5 == ref_md5:
                ul_md5_check = "md5_PASS"
            else:
                ul_md5_check = "md5_FAIL"
        else:
            ref_md5 = "NA"
            dl_md5_check = "NA"
        #log_string = file_name + '\t' + ref_md5 + '\t' + str(dlSize) + '\t' + str(dlFileMd5) + '\t' + dl_md5_check + '\t' + str(dlTime) + '\t' + str(s3Size) + '\t' + str(s3FileMd5) + '\t' + ul_md5_check + '\t' + str(ulTime) + '\t' + "File > 4Gb (4*(2^30) bytes), used multipart upload - upload md5 WILL NOT match dl md5" '\n'
    #else:
    log_string = file_name + '\t' + ref_md5 + '\t' + str(dlSize) + '\t' + str(dlFileMd5) + '\t' + dl_md5_check + '\t' + str(dlTime) + '\t' + str(s3Size) + '\t' + str(s3FileMd5) + '\t' + ul_md5_check + '\t' + str(ulTime) + '\n'
    print ("SUB :: Done processing sample ( " + str(sample) + " ) :: " + file_name)
    LOGFILE.write(log_string)
    LOGFILE.flush()
    return wget_status



if __name__ == '__main__':
    run()
