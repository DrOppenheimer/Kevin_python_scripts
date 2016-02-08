#!/usr/bin/env python
# simple script to check filename, size, and md5 for list of files(objects) in buckeet
# written to be run on AWS s3
def check_md5_size(my_bucket="bucket_name", access_key="some_key", secret_key="some_secret_key"):
    import os
    import time
    import hashlib
    import boto
    from boto.s3.key import Key
    import boto.s3.connection
    execfile("/home/ubuntu/git/Kevin_python_scripts/generate_file_md5.py")
    log_file=my_bucket + ".dictionary.txt"
    LOGFILE=open('./' + log_file, 'w+')
    LOGFILE.write('object_name' + '\t' + 'size(Gb)' + '\t' + 'md5' + '\t' + 'dl_time(s)' + '\n')
    con = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket=con.get_bucket(my_bucket)
    ### Get the number of items in the bucket
    count=0
    count2=0
    for key in bucket.list():
        print(key)

    #            count+=1
    # for key in bucket.list():
    #     ### Print percent complete
    #     count2+=1
    #     percentComplete = ( float(count2) / count ) * 100  
    #     print("Percent complete: ", str(round(percentComplete,2)))
    #     ### download object, get the time need to download
    #     #key = bucket.get_key('testobject.txt')
    #     tic = time.time()
    #     key.get_contents_to_filename(key)
    #     dlTime = time.time() - tic
    #     ### get md5 of downloaded object
    #     print ("calculating md5 " + key)
    #     fileMd5 = generate_file_md5(key) # uses function generate_file_md5 -- in your scripts
    #     ### get size of downloaded object
    #     statinfo = os.stat( key )
    #     size = statinfo.st_size
    #     size_gb = float(size) / (2**30)
    #     ### remove local copy of object
    #     print ("delete local copy of " + key)
    #     os.remove( key )
    #     ### write stats to output
    #     LOGFILE.write(log_string)
    #     LOGFILE.flush()
