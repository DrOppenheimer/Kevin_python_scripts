#!/usr/bin/env python
def ftp_2_s3(my_file="list_of_ftp_addresses", access_key="some_key", secret_key="some_secret_key"):
import os
import time
import hashlib
import boto
from boto.s3.key import Key
import boto.s3.connection
execfile("/home/ubuntu/git/Kevin_python_scripts/generate_file_md5.py")
my_bucket="1000_genome_exome"
#my_file="777.reamining_ul.2-8-16.split.aa"
#my_file="777.reamining_ul.2-8-16.split.ab"
#my_file="777.reamining_ul.2-8-16.split.ac"
#my_file="777.reamining_ul.2-8-16.split.ad"
#my_file="777.reamining_ul.2-8-16.split.ae"
#my_file="777.reamining_ul.2-8-16.split.af"
#my_file="777.reamining_ul.2-8-16.split.ag"
#my_file="777.reamining_ul.2-8-16.split.ah"
log_file=my_file + ".ul_log.txt"
LOGFILE=open('./' + log_file, 'w+')
LOGFILE.write('file_name' + '\t' + 'size(Gb)' + '\t' + 'md5' + '\t' + 'dl_time(s)' + '\t' + 'ul_time(s)' + '\n')
LOGFILE.flush()
sample=0
with open(my_file) as f:
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
	os.system(download_string)
	dlTime = time.time() - tic
	#### get md5
	print ("calculating md5 " + fileName)
	# hashlib.md5(fileName).hexdigest() # fix this (only obtaining md5 for the filename string)	
	#md5_string = "md5sum "+fileName
	#fileMd5 = os.system(md5_string).hexdigest()
	#fileMd5 = hashlib.md5(open(fileName,'rb').read()).hexdigest()
	fileMd5 = generate_file_md5(fileName) # uses function generate_file_md5 -- in your scripts
	#### get size
	statinfo = os.stat(fileName)
        size = statinfo.st_size
        size_gb = float(size) / (2**30)
	#### upload
	print ("uploading " + fileName)		
	tic = time.time()
	con = boto.connect_s3(aws_access_key_id=access_key, aws_secret_access_key=secret_key )
	bucket=con.get_bucket(my_bucket)
	key=Key(name=fileName, bucket=bucket)
	key.set_contents_from_filename(fileName)
	ulTime = time.time() - tic
	### remove local copy
	print ("delete local copy of " + fileName)
	os.remove(fileName)
	#### print to log
	print ("printing to log " + fileName)
	log_string = fileName + '\t' + str(size_gb) + '\t' + str(fileMd5) + '\t' + str(dlTime) + '\t' + str(ulTime) + '\n'
	print ("Done processing sample ( " + str(sample) + " ) :: " + fileName)
	LOGFILE.write(log_string)
	LOGFILE.flush()
