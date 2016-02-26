#!/usr/bin/python

# 11-6-15 This is a simple script to parse Mark's ARKs to get  the URLs
#### ARK Parser

# load packages etc.
import os,  urllib, re, json, os.path, argparse
from os.path import basename
from pprint import pprint

# example ark id
# https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242
# example record ( curl https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242 | python -mjson.tool )
# {
#     "hashes": {
#         "md5": "12f38c03368865ff8a671c98d471e782"
#     },
#     "host_authorities": [],
#     "keeper_authority": "CRI",
#     "limit": 100,
#     "metadata": null,
#     "name": "ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242",
#     "release": "private",
#     "rev": "ff750840",
#     "size": 131171985,
#     "start": 0,
#     "urls": [
#         "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000_genomes_project/data/PJL/HG03633/exome_alignment/HG03633.alt_bwamem_GRCh38DH.20150826.PJL.exome.cram",
#         "https://1000_genome_exome.s3.amazonaws.com/HG03633.alt_bwamem_GRCh38DH.20150826.PJL.exome.cram"
#     ]
# }
# https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242

# parse arguments
parser = argparse.ArgumentParser(description='Script to display contents of ARK record, or download content from a url')
parser.add_argument('-a','--ark', help='ark id', default="https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242")
parser.add_argument('-d','--download', action="store_true",  help='option to download data that the ARK points to')
parser.add_argument('-p','--pattern', help='pattern from beginning of url to match if there is more than one url in the record', default='ftp')
parser.add_argument('-b','--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

# remove any trailing newline characters
my_ark = args.ark.rstrip()

# download the ARK record
response = urllib.urlopen(my_ark)
my_json = json.load(response)
    
# check to see if option is to read or download
if args.download==True:
  
  json_urls = my_json["urls"]
  # make sure that there are urls
  try:
      json_urls
  except NameError:
    exit("There are no urls in this record")

  if args.debug==True:
    for x in json_urls:
      print("xx" + x + "xx")
         
  for x in json_urls:
    if re.match( ("^" + args.pattern), x ):
      download_url = x
    # make sure that there is a pattern matching url before downloading

  try:
    download_url
  except NameError:
    print "The urls (below) do not start with pattern :: " + str(args.pattern)
    for i in json_urls:
        print("\t" + i)
    exit("Please try again with a valid pattern from the urls(s) above")
  else:
    # get the filename from the url
    filename = basename(download_url).rstrip()
    # create a string to perform the download
    download_string = "curl " + str(download_url) + " -o " + str(filename)
    # download the file
    os.system(download_string)
    print("Download of " + str(filename) + " is complete")
    exit(0)
    
else:
    print ("Querried this ARK :: " + args.ark)
    if args.ark=="https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242":
        print("This is the built in default - you need to use \"-a\" with a valid ARK")
    print json.dumps(my_json, indent=4, sort_keys=True)
