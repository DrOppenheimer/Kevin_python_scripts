#!/usr/bin/env python

# modified slighltly -- comment out creds -- kevin 2-11-16
import sys
import json
import mmap
import logging
import argparse
import boto
import boto.s3.connection

GIG = 2**30

if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Multipart from stdin. - modified from Mark\'s version')
    parser.add_argument('-a','--access_key', help='access key', required=True)
    parser.add_argument('-s','--secret_key', help='secret key', required=True)
    parser.add_argument('-g','--gateway', help='s3 gateway/host', required=True)
    parser.add_argument('-b','--bucket_name', help='bucket name', required=True)
    parser.add_argument('-k','--bucket_key', help='bucket key', required=True)
    parser.add_argument('-d','--debug', help='debug')
    args = parser.parse_args()
    
    #parser = argparse.ArgumentParser(description='Multipart from stdin.')

    parser.add_argument('-d', '--debug',
        action='store_true',
        help='Enabled debug-level logging.',
    )

    #parser.add_argument('credentials',
    #    type=argparse.FileType('r'),
    #    help='Credentials file.',
    #)

    #parser.add_argument('bucket')
    #parser.add_argument('key')
    
    #args = parser.parse_args()

    #logging.basicConfig(
    #    level=logging.DEBUG if args.debug else logging.INFO,
    #    format='%(asctime)s %(name)-6s %(levelname)-4s %(message)s',
    #)

    #credentials= json.load(args.credentials)

    if debug==True:
        print("access_key :: " + args.access_key)
        print("secret_key :: " + args.secret_key)
        print("gateway    :: " + args.gateway)
        print("bucket     :: " + args.bucket_name)
        print("key        :: " + args.bucket_key)
        
    conn = boto.connect_s3(
        aws_access_key_id     = args.access_key, #credentials.get('access_key'),
        aws_secret_access_key = args.secret_key, #credentials.get('secret_key'),
        host                  = args.gateway, #credentials.get('host'),
        #port                  = credentials.get('port'),
        is_secure             = True, #credentials.get('is_secure', True),
        calling_format        = boto.s3.connection.OrdinaryCallingFormat(),
    )

    mp = conn.get_bucket(args.bucket_name).initiate_multipart_upload(args.bucket_key)

    i = 0
    while True:
        i += 1
        
        ramdisk = mmap.mmap(-1, GIG)
        ramdisk.write(sys.stdin.read(GIG))
        
        size = ramdisk.tell()
        if not size:
            break
        
        ramdisk.seek(0)
        #logging.info('Uploading chunk {}'.format(i))
        
        try: mp.upload_part_from_file(ramdisk, part_num=i, size=size)
        except Exception as err:
            #logging.error('Failed writing part - cancelling multipart.')
            mp.cancel_upload()
            raise

    #logging.info('Completing multipart.')
    mp.complete_upload()
