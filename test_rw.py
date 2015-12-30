###################################################################################
###################################################################################
###################################################################################

# NOTES
# Do tests with and without parcel
# Iterate ~5 times and take average


# Script outline (from 12-15-15) -- tested, this works

###################################################################################
###################################################################################
###################################################################################

# DOWNLOAD ALL OBJECTS FROM BUCKET AND RECORD TIME IT TAKES (NON-PARCEL)
import time

access_key=
secret_key=
bucket_name = 
gateway = 
test_file='ERR188416_1.fastq.gz'

### Open connection
conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = gateway,
        #is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

### get existing bucket::
bucket = conn.get_bucket(bucket_name)

### downloading an object to local::
tic = time.clock()
key = bucket.get_key(test_file)
key.get_contents_to_filename('./' + test_file)
toc = time.clock()
toc - tic

## DOWNLOAD WHOLE BUCKET
my_dir="/mnt/bucket"
log_file="dl_log.txt"
LOGFILE=open('./' + log_file, 'w+')
LOGFILE.write('file_name\tsize(bytes)\tsize(Gb)\tdl_time(s)\n')
  
import os
import time
import boto
import boto.s3.connection

os.mkdir(my_dir)
os.chdir(my_dir)

bucket_list=list()
for key in bucket.list():
	bucket_list.append(key.name)

for i in bucket_list:
	tic = time.time()
	print('downloading ' + i)
	key = bucket.get_key(i)
    size = key.size
    size_gb = float(size) / (2**30)
	key.get_contents_to_filename('./' + i)
	print('done downloading ' + i)
	toc = time.time()
	dl_time = toc - tic
	log_string = i + '\t' + str(size) + '\t' + str(size_gb) + '\t' + str(dl_time) + '\n'
	print(log_string)
	LOGFILE.write(log_string)

LOGFILE.close()

###################################################################################
###################################################################################
###################################################################################

# CREATE AND UPLOAD TO BUCKET (NON-PARCEL)

### creating an object directly::
# key = bucket.new_key('testobject.txt')
# key.set_contents_from_string('working with s3 is fun')

import os
import time
import boto
import boto.s3.connection

my_dir="/mnt/bucket"
os.chdir(my_dir)
log_file="ul_log.txt"
LOGFILE=open('./' + log_file, 'w+')
LOGFILE.write('file_name\tsize(bytes)\tsize(Gb)\tdl_time(s)\n')

### Open connection
conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = gateway,
        #is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
        )

### create bucket::
bucket_name='test_bucket'
bucket = conn.create_bucket(bucket_name)

### load existing files to the object storage::
# files_to_put = ['myfavoritefile.txt','yourfavoritefile.txt']
files_to_put = ['ERR_tar.1Gb.gz','ERR_tar.11Gb.gz','ERR_tar.59Gb.gz']
#files_to_put = ['ERR_tar.1Gb.gz']

for i in files_to_put:
        tic = time.time()
        print('uploading ' + i)
        key = bucket.new_key(i)
        key.set_contents_from_filename(i)
        statinfo = os.stat(i)
        size = statinfo.st_size
        size_gb = float(size) / (2**30)
        print('done downloading ' + i)
        toc = time.time()
        dl_time = toc - tic
        log_string = i + '\t' + str(size) + '\t' + str(size_gb) + '\t' + str(dl_time) + '\n'
        print(log_string)
        LOGFILE.write(log_string)

LOGFILE.close()

###################################################################################
###################################################################################
###################################################################################


# # From Satish 12-3-15 # INSTALLING AND USING PARCEL
# # Install
# python setup.py develop
# sudo apt-get install python-pip
# sudo python setup.py develop
# # Setup
# sudo vi /etc/hosts  - add 127.0.0.1 parcel.opensciencedatacloud.org
# parcel-tcp2udt 192.170.232.76:9000 &
# parcel-udt2tcp localhost:9000 &
# wget https://parcel.opensciencedatacloud.org:9000/asgc-geuvadis/ERR188021.tar.gz
# # so if u see here.. I have  'python setup.py develop' twice.. this is because it failed first and then I had to do a apt-get install python-pip





























        
### list objects in bucket::

### list objects in bucket::
for key in bucket.list():
        print "{name}\t{size}\t{modified}".format(
                name = key.name,
                size = key.size,
                modified = key.last_modified,
                )

### creating an object directly::
key = bucket.new_key('testobject.txt')
key.set_contents_from_string('working with s3 is fun')

### load existing files to the object storage::
files_to_put = ['myfavoritefile.txt','yourfavoritefile.txt']

for k in files_to_put:
        key = bucket.new_key(k)
        key.set_contents_from_filename(k)

### list objects in bucket::
for key in bucket.list():
        print "{name}\t{size}\t{modified}".format(
                name = key.name,
                size = key.size,
                modified = key.last_modified,

### delete item in bucket
bucket.delete_key('goodbye.txt')
                
### deleting a bucket -- bucket must be empty::
conn.delete_bucket(bucket.name)








walt: are you using boto?
[10:33am] walt: https://www.opensciencedatacloud.org/support/griffin.html#example-using-python-s-boto-package-to-interact-with-s3
[10:33am] walt: conn.delete_bucket(bucket.name)
[10:34am] walt: ^ -- to delete bucket
[10:35am] walt: to delete item in bucket
[10:35am] walt: http://docs.ceph.com/docs/master/radosgw/s3/python/
[10:35am] walt: bucket.delete_key('goodbye.txt')




# WITH PARCEL

# # From Satish 12-3-15 # INSTALLING AND USING PARCEL
# # Install
# python setup.py develop
# sudo apt-get install python-pip
# sudo python setup.py develop
# # Setup
# sudo vi /etc/hosts  - add 127.0.0.1 parcel.opensciencedatacloud.org
# parcel-tcp2udt 192.170.232.76:9000 &
# parcel-udt2tcp localhost:9000 &
# wget https://parcel.opensciencedatacloud.org:9000/asgc-geuvadis/ERR188021.tar.gz
# # so if u see here.. I have  'python setup.py develop' twice.. this is because it failed first and then I had to do a apt-get install python-pip



