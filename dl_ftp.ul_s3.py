import os
import time
import hashlib
import boto
from boto.s3.key import Key
import boto.s3.connection
my_bucket="1000_genome_exome"
#my_file="1000_genome_exome_ftp.1_to_500.1-29-16.txt"
#my_file="1000_genome_exome_ftp.501_to_1000.1-29-16.txt"
#my_file="1000_genome_exome_ftp.1001_to_1500.1-29-16.txt"
my_file="1000_genome_exome_ftp.1501_to_2000.1-29-16.txt"
#my_file="1000_genome_exome_ftp.2001_to_2500.1-29-16.txt"
#my_file="1000_genome_exome_ftp.2501_to_2692.1-29-16.txt"
log_file=my_file + ".ul_log.txt"
LOGFILE=open('./' + log_file, 'w+')
LOGFILE.write('file_name' + '\t' + 'size(Gb)' + '\t' + 'md5' + '\t' + 'dl_time(s)' + '\t' + 'ul_time(s)' + '\n')
LOGFILE.flush()
with open(my_file) as f:
    for line in f:
	splitLine = line.split("/")
	fileName = splitLine[ len(splitLine) - 1 ]
	fileName = fileName.rstrip("\n")
	print ("processing " + fileName)
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
	md5_string = "md5sum "+fileName
	fileMd5 = os.system(md5_string)
	#### get size
	statinfo = os.stat(fileName)
        size = statinfo.st_size
        size_gb = float(size) / (2**30)
	#### upload
	print ("uploading " + fileName)		
	tic = time.time()
	con = boto.connect_s3(aws_access_key_id="ACCESSKEYHERE", aws_secret_access_key="SECRETKEYHERE" )
	bucket=con.get_bucket(my_bucket)
	key=Key(name=fileName, bucket=bucket)
	key.set_contents_from_filename(fileName)
	ulTime = time.time() - tic
	### remove local copy
	print ("delete local copy of " + fileName)
	os.remove(fileName)
	#### print to log
	print ("printing to log " + fileName)
	log_string = fileName + '\t' + str(size_gb) + '\t' + fileMd5 + '\t' + str(dlTime) + '\t' + str(ulTime) + '\n'
	LOGFILE.write(log_string)
	LOGFILE.flush()
