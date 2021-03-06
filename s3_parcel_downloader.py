import os
os.environ['PYTHONPATH']='/home/ubuntu/git/Kevin_python_scripts' # add path to pythonpath
import sys
import json
import mmap
import logging
import argparse
import time
import hashlib
import subprocess
import boto
import boto.s3.connection
import ARK_download.gamma.py
from boto.s3.key import Key
from generate_file_md5 import generate_file_md5
try:
    import multipart_upload # make sure is in PYTHONPATH
except:
    print 'multipart_upload did not import -- is the script in PYTHONPATH?'
    sys.exit(1)    

def run():
    parser = argparse.ArgumentParser(description='Simple script to perform a boto download')

    parser.add_argument('-a','--ark_id', help='ark id/ address to querry', default="test")
    parser.add_argument('-g','--gateway', help='s3 host/gateway', default='griffin-objstore.opensciencedatacloud.org') # s3.amazonaws.com
    parser.add_argument('-b','--bucket_name', help='bucket name', default='1000_genome_exome')
    parser.add_argument('-ak','--access_key', help='access key')
    parser.add_argument('-sk','--secret_key', help='secret key')
    parser.add_argument('-wp','--with_parcel', help='download with parcel (in adition to default download without)')
    parser.add_argument('-ps','--parcel_server', help='download with parcel (in adition to default download without)')
    

    
    parser.add_argument('-r', '--retry', help='number of times to retry each download', default=10)


    parser.add_argument('-k', '--md5_ref_dictionary', help='provide a list ( name \t md5 ) to compare against', default=0)
    #parser.add_argument('-p', '--proxy', action="store_true", help='proxy to use for the download')
    parser.add_argument('-p', '--my_proxy', default='http://cloud-proxy:3128', help='proxy to use for the download (upload is assumed local and does not use proxy)')
    parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
    parser.add_argument('-sf', '--status_file', help='file to store download status')
    parser.add_argument('-fd', '--force-download', action="store_true", help='force to download even if file exists')
    args = parser.parse_args()


(Adapat from ARK_download.gamma.py)

# Download ARK record    



# Download data with parcel (n times)

# check (against ARK) (n times)

# download without parcel (n times)

# check (against ARK) (n times)

# print data to new or exisitng record






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
parser.add_argument('-up','--useparcel', action="store_true", help='use parcel for the download (assumes parcel is installed on VM)", action="stor_true')
parser.add_argument('-rp','--remoteparcelip', help='remote parcel ip', default='172.16.128.7')
parser.add_argument('-pp', '--parcelport', help='parcel port', default='9000')
parser.add_argument('-b','--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

# MAIN
def run():
    # check to make sure that parcel local (tcp2udt) is running
    if args.useparcel==True:
        # exit is multiplied by 256, so is a 1 (http://stackoverflow.com/questions/3736320/executing-shell-script-with-system-returns-256-what-does-that-mean)
        # local (tcp2udt)
        tcp2udt_status=os.system('ps -e | grep parcel-tcp2udt')
        udt2tcp_status=os.system('ps -e | grep parcel-udt2tcp')
        #tcp2udt_status=os.system('sudo lsof -i :9000')
        if tcp2udt_status==256:
            print "Parcel tcp2udt (local proxy) is not running -- trying to start it now"
            tcp2udt_command = 'parcel-tcp2udt ' + str(args.remoteparcelip) + ":" + str(args.parcelport) # e.g. 'parcel-tcp2udt 192.170.232.76:9000'
            tcp2udt_status = os.spawnl(os.P_NOWAIT, tcp2udt_command)
        # # server (udt2tcp)
        # udt2tcp_status=os.system('ps -a | grep parcel-udt2tcp')
        # if udt2tcp_status==256:
        #     print "Parcel udt2tcp (server proxy) is not running -- trying to start it now"    
        #     udt2tcp_command = 'parcel-udt2tcp localhost:' + str(args.parcelport) # e.g. 'parcel-udt2tcp localhost:9000'
        #     udt2tcp_status = os.spawnl(os.P_NOWAIT, udt2tcp_command)
        if tcp2udt_status == 1:# or udt2tcp_status == 1:
            quit('Parcel is not running. It may not be installed or could be configured improperly')
        else:
            print('Parcel is running ( 0 indicates running, 256 incates error ): \ntcp2udt_status: ' + str(os.system('ps -e | grep parcel-tcp2udt')) + '\n' + 'udt2tcp_status: ' + str(os.system('ps -e | grep parcel-udt2tcp')))
        os.system('sleep 1') # sleep for 5 seconds  -- is this overkill?
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
        if args.useparcel==True:
            download_with_parcel(urls=json_urls, pattern=args.pattern, remoteparcelip=args.remoteparcelip, parcelport=args.parcelport, debug=args.debug)
        # download without parcel    
        else:
            download_without_parcel(urls=json_urls, pattern=args.pattern, debug=args.debug)
    else:
        print ("Querried this ARK :: " + args.ark)
        if args.ark=="https://signpost.opensciencedatacloud.org/alias/ark:/31807/DC3-fd206d7f-af73-4095-8348-00cf3c8e5242":
            print("This is the built in default - you need to use \"-a\" with a valid ARK")
        print json.dumps(my_json, indent=4, sort_keys=True)

# SUB to make sure that parcel ip is same as ip in url -- if not, try download replacing url ip with parcelip
def check_parcel_url(url, parcelip, parcelport, debug):
    url = str(url)
    parcelip = str(parcelip)
    test = url.find(parcelip)
    if test == -1:
        url_vector = url.split('/')
        num_fields=len(url_vector)
        if debug==True:
            print 'num_fields: ' + str(num_fields)
        new_url_tail=''
        for i in range (4,num_fields+1):
            new_url_tail=new_url_tail + '/' + str( url_vector[i-1] )
            if debug==True:
                print 'new_url_tail: ' + new_url_tail
        new_url= str( url_vector[0] ) + '//' + str(parcelip) + ':' + str(parcelport) + new_url_tail
        print 'The parcelip is not contained in the url for the sample, attempting to fix'
        print '     changing this url: ' + url
        print '     to this url      : ' + new_url 
    else:
        new_url = url
    return new_url
        
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
            download_string = "curl -O -k " + str(download_url)            
            if debug==True:
                print "\nPerforming this download:"
                print download_string + "\n" 
            os.system(download_string)
            # download the file
            print("NON-Parcel Download of " + str(filename) + " is complete")
            exit(0)

# SUB to download with parcel            
def download_with_parcel(urls, pattern, remoteparcelip, parcelport, debug):
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
                # stick the new function here
                url_string = check_parcel_url(url_string, remoteparcelip, parcelport, debug)
                url_vector = url_string.split('/')
                filename = url_vector[ len(url_vector) - 1 ]
                # create a string to perform the download          
                # example that worked once: curl -k -O --noproxy * https://172.16.128.7:9000/1000_genome_exome/HG00103.alt_bwamem_GRCh38DH.20150826.GBR.exome.cram
                # then switched to this:    curl -O -k --noproxy 172.16.128.7 https://172.16.128.7:9000/1000_genome_exome/HG00103.alt_bwamem_GRCh38DH.20150826.GBR.exome.cram
                bucketname = url_vector[ len(url_vector) - 2 ]
                # create a string to perform the download
                download_string = "curl -O -k --noproxy " + str(remoteparcelip) + ' ' + "https://" + str(remoteparcelip) + ":" + str(parcelport) + "/" + str(bucketname) + "/" + str(filename)
                #download_string = "curl -k -O https://" + str(remoteparcelip) + ":" + str(parcelport) + "/" + str(bucketname) + "/" + str(filename)
                if debug==True:
                    print "\nPerforming this download:"
                    print download_string + "\n"
                # download the file
                os.system(download_string)
                print("Parcel Download of " + str(filename) + " is complete")
                exit(0)
            
if __name__ == '__main__':
    run()
























    

 # From Grif with parcel
    echo "#################################" >> $LOG
    echo "DOWNLOAD from Griffin WITH parcel:" >> $LOG
    #GRIF_W_PARCELDL_START_TIME=`date +%s.%N`;
    GRIF_W_PARCELDL_START_TIME=$SECONDS;
    CMD="ARK_download.gamma.py -a $i -p $URLPATTERN1 -up -rp $PARCELIP -d -b";
    echo $CMD >> $LOG;
    eval $CMD &>>$LOG;
    CMD_STATUS=$?;
    echo -e "Command status: \t"$CMD_STATUS >> $LOG;
    #GRIF_W_PARCELDL_ENDTIME=`date +%s.%N`;
    #GRIF_W_ELAPSED_TIME=`echo "$GRIF_W_PARCELDL_ENDTIME - $GRIF_W_PARCELDL_START_TIME" | bc -l`;
    GRIF_W_ELAPSED_TIME=$(($SECONDS - $GRIF_W_PARCELDL_START_TIME));
    if [ $DEBUG==1 ]; then
	echo "GRIF_W_ELAPSED_TIME (s): "$GRIF_W_ELAPSED_TIME;
    fi
    echo -e "Command runtime: \t"$GRIF_W_ELAPSED_TIME >> $LOG;
    URL=`curl $i | python -mjson.tool | grep -e $URLPATTERN1` # get raw URL -- will be quoted and possibly followed by comma
    URL=`echo $URL | sed -e 's/,$//'` # remove trailing comma
    URL=`echo $URL | sed -e 's/^"//'  -e 's/"$//'` # remove quotes
    FILE=`basename $URL`;
    GRIF_W_MD5=`md5sum $FILE | cut -d " " -f1`;
    echo -e "md5: \t"$GRIF_W_MD5 >> $LOG;
    if [ ! -e $FILE ]; then
	MESSAGE="$FILE does not exist/ does not need to be deleted";
	echo $MESSAGE
	echo $MESSAGE >> $LOG;
    else
	rm $FILE;
	echo $MESSAGE >> $LOG;
    fi
    echo "#################################" >> $LOG
    
    # From Grif without parcel    
    echo "#################################" >> $LOG
    echo "DOWNLOAD from Griffin withOUT parcel:" >> $LOG
    #GRIF_WO_PARCEL_DL_START_TIME=`date +%s.%N`;
    GRIF_WO_PARCEL_DL_START_TIME=$SECONDS;
    CMD="ARK_download.gamma.py -a $i -p $URLPATTERN1 -d -b";
    echo $CMD >> $LOG;
    eval $CMD &>>$LOG;
    CMD_STATUS=$?;
    echo -e "Command status: \t"$CMD_STATUS >> $LOG;
    #GRIF_WO_PARCEL_DL_ENDTIME=`date +%s.%N`;
    #GRIF_WO_ELAPSEDTIME=`echo "$GRIF_WO_PARCEL_DL_ENDTIME - $GRIF_WO_PARCEL_DL_START_TIME" | bc -l`;
    GRIF_WO_ELAPSEDTIME=$(($SECONDS - $GRIF_WO_PARCEL_DL_START_TIME));
    if [ $DEBUG==1 ]; then
	echo "GRIF_WO_ELAPSEDTIME (s): "$GRIF_WO_ELAPSEDTIME;
    fi
    echo -e "Command runtime: \t"$GRIF_WO_ELAPSEDTIME >> $LOG;
    URL=`curl $i | python -mjson.tool | grep -e $URLPATTERN1` # get raw URL -- will be quoted and possibly followed by comma
    URL=`echo $URL | sed -e 's/,$//'` # remove trailing comma
    URL=`echo $URL | sed -e 's/^"//'  -e 's/"$//'` # remove quotes
    FILE=`basename $URL`;
    GRIF_WO_MD5=`md5sum $FILE | cut -d " " -f1`;
    echo -e "md5: \t"$GRIF_WO_MD5 >> $LOG;
    if [ ! -e $FILE ]; then
    	MESSAGE="$FILE does not exist; script can't use a file that doesn't exist";
    	echo $MESSAGE;
    	echo $MESSAGE >> $LOG;
    	exit 1;
    else
    	MESSAGE="$FILE exists, will now start processing";
    	echo $MESSAGE;
    	echo $MESSAGE >> $LOG;
    fi
    echo "#################################" >> $LOG # From Grif with parcel
    echo "#################################" >> $LOG
    echo "DOWNLOAD from Griffin WITH parcel:" >> $LOG
    #GRIF_W_PARCELDL_START_TIME=`date +%s.%N`;
    GRIF_W_PARCELDL_START_TIME=$SECONDS;
    CMD="ARK_download.gamma.py -a $i -p $URLPATTERN1 -up -rp $PARCELIP -d -b";
    echo $CMD >> $LOG;
    eval $CMD &>>$LOG;
    CMD_STATUS=$?;
    echo -e "Command status: \t"$CMD_STATUS >> $LOG;
    #GRIF_W_PARCELDL_ENDTIME=`date +%s.%N`;
    #GRIF_W_ELAPSED_TIME=`echo "$GRIF_W_PARCELDL_ENDTIME - $GRIF_W_PARCELDL_START_TIME" | bc -l`;
    GRIF_W_ELAPSED_TIME=$(($SECONDS - $GRIF_W_PARCELDL_START_TIME));
    if [ $DEBUG==1 ]; then
	echo "GRIF_W_ELAPSED_TIME (s): "$GRIF_W_ELAPSED_TIME;
    fi
    echo -e "Command runtime: \t"$GRIF_W_ELAPSED_TIME >> $LOG;
    URL=`curl $i | python -mjson.tool | grep -e $URLPATTERN1` # get raw URL -- will be quoted and possibly followed by comma
    URL=`echo $URL | sed -e 's/,$//'` # remove trailing comma
    URL=`echo $URL | sed -e 's/^"//'  -e 's/"$//'` # remove quotes
    FILE=`basename $URL`;
    GRIF_W_MD5=`md5sum $FILE | cut -d " " -f1`;
    echo -e "md5: \t"$GRIF_W_MD5 >> $LOG;
    if [ ! -e $FILE ]; then
	MESSAGE="$FILE does not exist/ does not need to be deleted";
	echo $MESSAGE
	echo $MESSAGE >> $LOG;
    else
	rm $FILE;
	echo $MESSAGE >> $LOG;
    fi
    echo "#################################" >> $LOG
    
    # From Grif without parcel    
    echo "#################################" >> $LOG
    echo "DOWNLOAD from Griffin withOUT parcel:" >> $LOG
    #GRIF_WO_PARCEL_DL_START_TIME=`date +%s.%N`;
    GRIF_WO_PARCEL_DL_START_TIME=$SECONDS;
    CMD="ARK_download.gamma.py -a $i -p $URLPATTERN1 -d -b";
    echo $CMD >> $LOG;
    eval $CMD &>>$LOG;
    CMD_STATUS=$?;
    echo -e "Command status: \t"$CMD_STATUS >> $LOG;
    #GRIF_WO_PARCEL_DL_ENDTIME=`date +%s.%N`;
    #GRIF_WO_ELAPSEDTIME=`echo "$GRIF_WO_PARCEL_DL_ENDTIME - $GRIF_WO_PARCEL_DL_START_TIME" | bc -l`;
    GRIF_WO_ELAPSEDTIME=$(($SECONDS - $GRIF_WO_PARCEL_DL_START_TIME));
    if [ $DEBUG==1 ]; then
	echo "GRIF_WO_ELAPSEDTIME (s): "$GRIF_WO_ELAPSEDTIME;
    fi
    echo -e "Command runtime: \t"$GRIF_WO_ELAPSEDTIME >> $LOG;
    URL=`curl $i | python -mjson.tool | grep -e $URLPATTERN1` # get raw URL -- will be quoted and possibly followed by comma
    URL=`echo $URL | sed -e 's/,$//'` # remove trailing comma
    URL=`echo $URL | sed -e 's/^"//'  -e 's/"$//'` # remove quotes
    FILE=`basename $URL`;
    GRIF_WO_MD5=`md5sum $FILE | cut -d " " -f1`;
    echo -e "md5: \t"$GRIF_WO_MD5 >> $LOG;
    if [ ! -e $FILE ]; then
    	MESSAGE="$FILE does not exist; script can't use a file that doesn't exist";
    	echo $MESSAGE;
    	echo $MESSAGE >> $LOG;
    	exit 1;
    else
    	MESSAGE="$FILE exists, will now start processing";
    	echo $MESSAGE;
    	echo $MESSAGE >> $LOG;
    fi
    echo "#################################" >> $LOG


    
