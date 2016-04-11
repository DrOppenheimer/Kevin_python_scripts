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

# make sure that the awscli package is installed this way 
# pip install awscli
# and run
# aws config
# should use ~/.aws/credentials -- but looks like it just use ~/.aws/config

# when you do this -- keys will be in that file, and you won't have to supply -a or -s

# Make sure that python scripts are in the path for sourcing
# import sys
# sys.path.append('/home/ubuntu/git/Kevin_python_scripts')
# import multipart_upload
# OR
# add this to ~/.bashrc and source
# export PYTHONPATH=$PYTHONPATH:/home/ubuntu/git/Kevin_python_scripts

import sys
import json
import mmap
import logging
import argparse
import os
import time
import hashlib
import subprocess
import boto
import boto.s3.connection
try:
    import multipart_upload # make sure is in PYTHONPATH
except:
    print 'multipart_upload did not import -- is the script in PYTHONPATH?'    
from boto.s3.key import Key
from generate_file_md5 import generate_file_md5

my_proxy='http://cloud-proxy:3128'


def run():
    parser = argparse.ArgumentParser(description='Simple script to perform a boto download')
    parser.add_argument('-l','--list', help='file with list of ftp addresses', required=True, default="test")
    parser.add_argument('-a','--access_key', help='access key')
    parser.add_argument('-s','--secret_key', help='secret key')
    parser.add_argument('-g','--gateway', help='s3 host/gateway', default='griffin-objstore.opensciencedatacloud.org') # s3.amazonaws.com
    parser.add_argument('-b','--bucket_name', help='bucket name', default='1000_genome_exome')
    parser.add_argument('-r', '--retry', help='number of times to retry each download', default=10)
    parser.add_argument('-k', '--md5_ref_dictionary', help='provide a list ( name \t md5 ) to compare against', default=0)
    #parser.add_argument('-p', '--proxy', action="store_true", help='run using \"with_proxy\"')
    parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
    parser.add_argument('-sf', '--status_file', help='file to store download status')
    parser.add_argument('-fd', '--force-download', action="store_true", help='force to download even if file exists')
    args = parser.parse_args()

    

    #else:
    #    del os.environ['http_proxy']
    #    del os.environ['https_proxy']
    #    del os.environ['ftp_proxy']
        
    # optional, you can also put secrets in {HOME}/aws/.credentials
    if args.access_key:
        os.environ['AWS_ACCESS_KEY_ID'] = args.access_key
    if args.secret_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = args.secret_key

    status_file = args.status_file if args.status_file else args.list + '_status.json'
    print status_file
    if os.path.exists(status_file):
        with open(status_file, 'r') as f:
            try:
                final_status = json.load(f)
            except:
                final_status = {'succeed_files': []}
    else:
        final_status = {'succeed_files': []}
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
    LOGFILE.write("\t".join(
        ["file_name", "ref_md5", "download_md5", "download_size(bytes)", "download_time(s)",
        "upload_md5", "upload_size(bytes)", "upload_time(s)", "redownload_time(s)"])+'\n')
    LOGFILE.flush()
    sample=0
    metrics = {}
    try:
        process_file(args, LOGFILE, metrics, final_status, my_md5_ref_dictionary, debug=args.debug)
    finally:
        with open(status_file, 'w') as f:
            final_status['succeed_files'] = list(set(final_status['succeed_files']))
            json.dump(final_status, f)


def get_stats_func(stats):
    def get_stats(key):
        return str(stats.get(key, 'UNKNOWN'))
    return get_stats


def process_file(args, LOGFILE, metrics, final_status, my_md5_ref_dictionary, debug):

    with open(args.list) as f:
        for my_line in f:
            splitLine = my_line.split("/")
            my_file_name = splitLine[ len(splitLine) - 1 ].strip()
            if debug==True:
                print 'FILENAME: ' + my_file_name
            if my_file_name in final_status['succeed_files']:
                print "File {} already processed, skipping".format(my_file_name)
                continue
            stats = {}
            stats['file_name'] = my_file_name
            for my_attempt in xrange(args.retry):
                if args.debug == True:
                    print("MAIN :: Attempt " + str(my_attempt))
                    print("MAIN :: Bucket_name: " + args.bucket_name)
                time.sleep(1)
                print("MAIN :: STARTING download and upload attempt ( " + str(my_attempt) + " ) for " + my_file_name)
                (ftp_status, download_time) = ftp_download(
                    line=my_line, LOGFILE=LOGFILE, file_name=my_file_name, 
                    debug=args.debug, force_download=args.force_download, my_proxy=my_proxy,
                    stats=stats)
                if ftp_status == 0:
                    dl_md5_check = check_md5_and_size(my_file_name, my_md5_ref_dictionary, stats=stats)
                    if dl_md5_check == "md5_PASS" or dl_md5_check == "md5_NA":
                        status, upload_time = upload_file(my_file_name, args.bucket_name, args.gateway, stats=stats)
                        if status == 0:
                            (status, check_time) = check_uploaded_file(my_file_name, args.bucket_name,
                                args.gateway,
                                my_md5_ref_dictionary,
                                debug=True, stats=stats)
                            if status == 0:
                                cleanup_files(my_file_name)
                                final_status['succeed_files'].append(my_file_name)
                                break
                        else:
                            print "UPLOAD FAILED"
                            #sys.exit(1)
                    else:
                        print "FTP DOWNLOAD FAIL"
                        cleanup_files(my_file_name)
                        #sys.exit(1) 
                else:
                    cleanup_files(my_file_name)
            metrics[my_file_name] = stats
            get_stats = get_stats_func(stats)
            log_str = '\t'.join(
                map(get_stats,
                    ['file_name', 'reference_md5', 'download_md5',
                     'download_size', 'download_time', 'upload_md5',
                     'upload_size', 'upload_time', 'redownload_time']))
            LOGFILE.write(log_str)
            LOGFILE.write('\n')
            LOGFILE.flush()




def get_value(my_key, my_dictionary):
    if my_dictionary.has_key(my_key):
        my_value = my_dictionary.get(my_key)
        my_value = my_value.rstrip("\n")
        return (my_value)
    else:        
        return ("key does not exist")

def ftp_download(line, LOGFILE, file_name, debug, force_download, my_proxy, stats={}):
    os.environ['http_proxy'] = my_proxy # assume remote location for ftp - so use proxy for the dl
    os.environ['https_proxy'] = my_proxy
    os.environ['ftp_proxy'] = my_proxy
    if debug==True:
        print "SUB :: FILE_NAME: " + file_name
    if not force_download and os.path.exists(file_name):
        if debug==True:
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
            #log_string = file_name + '\t' + " :: download and/or rm failed" + '\n'
            #LOGFILE.write(log_string)
            #LOGFILE.flush()
            stats['download_time'] = dlTime
    return wget_status, dlTime


def check_md5_and_size(
        file_name, md5_ref_dictionary, debug=True, stats={},
        action='download', key_name=''):
    if not key_name:
        key_name = file_name
    print ("SUB :: calculating dl md5 :: " + file_name)   #### get md5 for file downloaded from ftp
    if md5_ref_dictionary!=0:
        dlFileMd5, dlSize = generate_file_md5(file_name)    # uses function generate_file_md5 -- in your scripts
        if debug == True:
            print( "SUB :: " + file_name + " :: FTP_MD5 :: " + dlFileMd5  )
        if md5_ref_dictionary != 0:                       ### Option to check against reference md5
            ref_md5 = get_value(my_key=key_name, my_dictionary=md5_ref_dictionary)
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
        stats['reference_md5'] = ref_md5
        stats['{}_md5'.format(action)] = dlFileMd5
        stats['{}_size'.format(action)] = dlSize
        return dl_md5_check
    else:
        dl_md5_check = "md5_NA"
        return dl_md5_check
        
def upload_file(file_name, bucket_name, gateway, debug=True, stats={}):
    del os.environ['http_proxy'] # do not use the proxy for upload -- assume it's a local transfer
    del os.environ['https_proxy']
    del os.environ['ftp_proxy']
    print ("SUB :: uploading :: " + file_name)            #### upload to s4
    tic = time.time()
    key_name = os.path.basename(file_name)
    if debug==True:
        print 'TYPE UPLOAD::FILENAME: ' + str( type(file_name) )
        print 'TYPE UPLOAD::BUCKET:   ' + str( type(bucket_name) )
        print 'TYPE UPLOAD::GATEWAY:  ' + str( type(gateway) )
        print 'TYPE UPLOAD::KEY:      ' + str( type(key_name) )
    status = subprocess.call(['aws', 's3', 'cp', file_name, 's3://{}/{}'.format(bucket_name, key_name), '--endpoint-url', 'https://'+gateway], env=os.environ)
    ulTime = time.time() - tic
    stats['upload_time'] = ulTime
    return status, ulTime

#def upload_file(file_name, bucket_name, gateway, debug=True, stats={}):
#    print ("SUB :: uploading :: " + file_name)            #### upload to s4
#    tic = time.time()
#    key_name = os.path.basename(file_name)
#    if debug==True:
#        print 'TYPE UPLOAD::FILENAME: ' + type(file_name)
#        print 'TYPE UPLOAD::BUCKET:   ' + type(bucket_name)
#        print 'TYPE UPLOAD::GATEWAY:  ' + type(gateway)
#        print 'TYPE UPLOAD::KEY:      ' + type(key_name)
#    status = subprocess.call(['aws', 's3', 'cp', file_name, 's3://{}/{}'.format(bucket_name, key_name), '--endpoint-url', 'https://'+gateway], env=os.environ)
#    ulTime = time.time() - tic
#    stats['upload_time'] = ulTime
#    return status, ulTime


def check_uploaded_file(file_name, bucket_name, gateway,
                        md5_ref_dictionary, debug=True, stats={}):
    print ("SUB :: checking uploaded :: " + file_name)            #### upload to s4
    tmp_file = file_name + 's3'
    tic = time.time()
    key_name = os.path.basename(file_name)
    status = subprocess.call(
        ['aws', 's3', 'cp', 's3://{}/{}'.format(bucket_name, key_name),
         tmp_file,'--endpoint-url', 'https://'+gateway], env=os.environ)
    if status == 0:
        md5_check = check_md5_and_size(tmp_file, md5_ref_dictionary, action='upload', key_name=file_name, stats=stats)

    cleanup_files(tmp_file)
    ulTime = time.time() - tic
    stats['redownload_time'] = ulTime
    return status, ulTime

def cleanup_files(*files):
    for f in files:
        try:
            os.remove(f)
        except:
            pass



if __name__ == '__main__':
    run()
