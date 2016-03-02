import sys
import json
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

from ftp_2_s3 import upload_file
from ftp_2_s3 import check_uploaded_file




status, upload_time = upload_file(my_file_name, args.bucket_name, args.gateway, stats=stats, debug=args.debug)
                        if status == 0:
                            (status, check_time) = check_uploaded_file(my_file_name, args.bucket_name,
                                args.gateway,
                                my_md5_ref_dictionary,
                                debug=True, stats=stats)
                            if status == 0:
                                cleanup_files(my_file_name)
                                final_status['succeed_files'].append(my_file_name)
                                break
