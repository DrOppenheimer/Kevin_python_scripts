#!/usr/bin/python

# 11-6-15 This is a simple script to parse Mark's ARKs to get  the URLs
#### ARK Parser

# load packages etc.
import os
import re
import json
import os.path
from os.path import basename
from pprint import pprint

debug = 0

print("example ark: \"http://192.170.232.74:8080/alias/ark:/31807/DC0-25a0b68b-12d0-47ea-b7eb-03bc5fc832b6\"")

# enter the ark
example_ark = input('Enter your ARK Address (surounded by quotes): ')

# select_source
# my_source = input('Enter your source ("s3" or "ftp"): ')

# get the ark basename
ark_basename = basename(example_ark).rstrip()

# create a string to perform the download
download_string = "curl " + example_ark + " -o " + ark_basename
if ( debug==1 ):
    print("DOWNLOAD STRING " + download_string)

# make a system call to download the file -- it will be named for the the last portion in the ark path = basename
# the file will be a json object
os.system(download_string)

# print the contents of the file (in a bit of a wonky way using a system call)
print_string = "less " + ark_basename + " | python -mjson.tool"
os.system(print_string)

# import the ark json from file
with open(ark_basename) as data_file:    
    ark_json = json.load(data_file)
    #pprint(ark_json)

# get the URLS
ark_urls = ark_json["urls"]
# u' just indicates that it is unicode
# print mail_accounts[0]["i"]

# pull out
for x in ark_urls:
    # print(x)
    match_ftp = re.match( "^ftp://", x )
    match_s3 = re.match( "^s3://", x )
    if match_ftp:
        ftp_address = x
        print("ftp_address: " + x)
    else:
        ftp_address = "Sorry, there is no ftp address"        
    if match_s3:
        s3_address = x
        print("s3_address:  " + x)
    else:
        ftp_address = "Sorry, there is no s3 address"

os.remove(ark_basename)
#if my_source == "S3":
#    print(s3_address)

#if my_source == "ftp":
#    print(ftp_address)
