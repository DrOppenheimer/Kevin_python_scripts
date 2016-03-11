#!/usr/bin/python

# 11-6-15 This is a simple script to parse Mark's ARKs to get  the URLs
#### ARK Parser

# load packages etc.
import os,  urllib, re, json, os.path, argparse, requests
from os.path import basename
from pprint import pprint

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

# parse arguments
parser = argparse.ArgumentParser(description='Script to display contents of ARK record, or download content from a url')
parser.add_argument('-a','--ark', help='ark id', default='https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242')
parser.add_argument('-d','--download', help='option to download data that the ARK points to', action="store_true")
parser.add_argument('-p','--pattern', help='pattern from beginning of url to match if there is more than one url in the record', default='ftp')
parser.add_argument('-up','--use-parcel', help='use parcel for the download (assumes parcel is installed on VM)", action="stor_true')
parser.add_argument('-rp','--remote-parcel-ip', help='remote parcel ip', default='172.16.128.7')
parser.add_argument('-pp', '--parcel-port', help='parcel port', default='9000')
parser.add_argument('-b','--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

# MAIN
def run():
    # start parcel service if that option is selected, exit if fail
    if args.use-parcel==True:
        tcp2udt-command = 'parcel-tcp2udt ' + str(args.remote-parcel-ip) + ":" + str(args.parcel-port) # e.g. 'parcel-tcp2udt 192.170.232.76:9000'
        udt2tcp-command = 'parcel-udt2tcp localhost:' + str(args.parcel-portargs.parcel-port) # e.g. 'parcel-udt2tcp localhost:9000'
        tcp2udt-status = os.spawnl(os.P_DETACH, tcp2udt-command)
        udt2tcp-status = os.spawnl(os.P_DETACH, udt2tcp-command)
        if tcp2udt-status != 0 or udt2tcp-status != 0:
            quit('parcel is not installed or configured properly')
        os.system('sleep 5') # sleep for 5 seconds  -- is this overkill?
    
    # remove any trailing newline characters
    my_ark = args.ark.rstrip()
    print(my_ark)
    
    # download the ARK record
    #response = urllib.urlopen(my_ark)
    response = requests.get(my_ark)
    print(response)
    #my_json = json.load(response)
    my_json = response.json()
    
    # get the urls
    json_urls = my_json["urls"]

    # make sure that there are urls
    try:
      json_urls
    except NameError:
        exit("There are no urls in this record")
    
    # check to see if option is to read or download
    if args.download==True:
        # download with parcel
        if args.use-parcel==True:
            download_with_parcel(urls=json_urls, pattern=args.pattern, remote-parcel-ip=args.remote-parcel-ip, parcel-port=args.parcel-port, debug=args.debug)
        # download without parcel    
        else:
            download_without_parcel(urls=json_urls, pattern=args.pattern, debug=args.debug)
    else:
        print ("Querried this ARK :: " + args.ark)
        if args.ark=="https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242":
            print("This is the built in default - you need to use \"-a\" with a valid ARK")
        print json.dumps(my_json, indent=4, sort_keys=True)

# SUB to download without parcel
def download_without_parcel(urls, pattern, debug):           
  for x in urls:
    if re.match( ("^" + pattern), x ):
        download_url = x
        # make sure that there is a pattern matching url before downloading
        try:
            download_url
        except NameError:
            print "The urls (below) do not start with pattern :: " + str(args.pattern)
            for i in urls:
                print("\t" + i)
                exit("Please try again with a valid pattern from the urls(s) above")
        else:
            # get the filename from the url
            filename = basename(download_url).rstrip()
            # create a string to perform the download
            download_string = "curl " + str(download_url) + " -O "
            # download the file
            os.system(download_string)
            print("Download of " + str(filename) + " is complete")
            exit(0)

# SUB to download with parcel            
def download_with_parcel(urls, pattern, remote-parcel-ip, parcel-port, debug)
    for x in urls:
    if re.match( ("^" + pattern), x ):
        download_url = x
        # make sure that there is a pattern matching url before downloading
        try:
            download_url
        except NameError:
            print "The urls (below) do not start with pattern :: " + str(args.pattern)
            for i in urls:
            print("\t" + i)
            exit("Please try again with a valid pattern from the urls(s) above")
        else:
            # get the filename from the url
            #filename = basename(download_url).rstrip()
            url_string = download_url.rstrip()
            url_vector = url_string.split('/')
            filename = url_vector[ len(url_vector) - 1 ]
            bucketname = url_vector[ len(url_vector) - 2 ]
            # create a string to perform the download
            download_string = "curl -k -O  https://" + str(remote-parcel-ip) + ":" + str(parcel-port) + "/" + str(bucketname) + "/" + str(filename)
            # download the file
            os.system(download_string)
            print("Download of " + str(filename) + " is complete")
            exit(0)
            
if __name__ == '__main__':
    run()

# Example of url,
# downloaded with 
#      wget -k -O https://172.16.128.7:9000/1000_genome_exome/HG00103.alt_bwamem_GRCh38DH.20150826.GBR.exome.cram
# or without parcel    
#      wget -O https://griffin-objstore.opensciencedatacloud.org/1000_genome_exome/HG00103.alt_bwamem_GRCh38DH.20150826.GBR.exome.cram
# 
