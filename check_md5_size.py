#!/usr/bin/env python
# simple script to check filename, size, and md5 for list of files(objects) in buckeet
# written to be run on AWS s3
def check_md5_size(my_bucket="bucket_name", access_key="some_key", secret_key="some_secret_key", md5_script="/home/ubuntu/git/Kevin_python_scripts/generate_file_md5.py"):
    import os
    import time
    import hashlib
    import boto
    from boto.s3.key import Key
    import boto.s3.connection
    execfile(md5_script)
    log_file=my_bucket + ".dictionary.txt"
    LOGFILE=open('./' + log_file, 'w+')
    #LOGFILE.write('object_name' + '\t' + 'size(bytes)' + '\t' + 'size(Gb)'+ '\t' + 'md5' + '\t' + 'dl_time(s)' + '\n')
    LOGFILE.write('object_name' + '\t' + 'size(bytes)' + '\t' + 'size(Gb)'+ '\t' + 'md5' + '\n')
    con = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    bucket=con.get_bucket(my_bucket)
    ### Get the number of items in the bucket
    count=0
    count2=0
    for key in bucket.list():
        count+=1
    for key in bucket.list():
        ### Print percent complete
        count2+=1
        percentComplete = ( float(count2) / count ) * 100  
        print("Percent complete: ", str(round(percentComplete,2)))
        ### download object, get the time need to download # Removed -- you don't have to download to get the stats! (size and md5 anyways) - code updated 2-10-16
        #tic = time.time()
        #key.get_contents_to_filename(key.name)
        #dlTime = time.time() - tic
        ### get md5 of downloaded object
        print ("calculating md5 " + key.name)
        fileMd5 = generate_file_md5(key.name) # uses function generate_file_md5 -- in your scripts
        ### md5 of object without downloading it
        fileMd5=bucket.get_key(key).etag[1 :-1]
        ### get size in Gb without downloading it (bytes and Gb size are in the report)
        size_gb = float(key.size) / (2**30)
        ### remove local copy of object
        #print ("delete local copy of " + key.name)
        #os.remove(key.name)
        ### write stats to output
        log_string = key.name + '\t' + str(key.size) + '\t' + str(size_gb) + '\t' + str(fileMd5) + '\t' + str(dlTime) + '\n'
        LOGFILE.write(log_string)
        LOGFILE.flush()

