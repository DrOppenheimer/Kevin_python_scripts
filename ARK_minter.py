#!/usr/bin/env python

import argparse
import json
import requests
import uuid
import re

parser = argparse.ArgumentParser(description='Script to mint ARK ids - handles only simple case of ids for a single file - but with multiple urls')
parser.add_argument('-l','--list', help='file with list of metadata filename\tsize_in_bytes\tmd5\turl1\turl...     ', required=True, default="test")
parser.add_argument('-u','--user', help='user', required=True)
parser.add_argument('-p','--password', help='password', required=True)
parser.add_argument('-e','--endpoint', help='endpoint', default='https://signpost.opensciencedatacloud.org/')
parser.add_argument('-k','--keeper', help='keeper authority', required=True)
parser.add_argument('-o','--host', help='host authority', default='PDC')
parser.add_argument('-a','--ark', help='ark prefix', default='ark:/31807/DC2-')
parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()

# Create a coupe variables
index_endpoint = args.endpoint +'index/'
alias_endpoint = args.endpoint + 'alias/'

# start log
log = args.list +'.ARK_minter.log'
LOGFILE = open('./' + log, 'w+')

counter = 0

with open(args.list) as f:
    for my_line in f:

        my_line = my_line.rstrip()

        # write header to the log file       
        if re.match('^#', my_line):
            my_header = my_line + '\t' + "ARK" + "\n"
            LOGFILE.write(my_header)
            LOGFILE.flush()

        # process all other lines
        else:
    
            # exit on empty line
            if len(my_line)==0:
                exit("Reached an empty line (hopefully at the end of the file).")
              
            # count records as they are processed
            counter += 1

            # parse the input line
            splitline = my_line.split('\t')
            file_name = splitline[ 0 ]
            file_size = splitline[ 1 ]
            file_md5 = splitline[2]
            file_urls = []
            for i in range(3, len(splitline), 1):
                if args.debug==True:
                    print("Length :: " + str(len(splitline)))
                    print("Range  :: " + str(i))
                file_urls.append(splitline[i])
        
            # create index
            data={'form':'object', 'size':file_size, 'urls':file_urls, 'hashes':{'md5':file_md5}}
            #output=requests.post(index_endpoint, json=data, auth=auth).json()

            # create alias
            data = {'size':file_size, 'hashes':{'md5':file_md5}, 'release': 'private', 'keeper_authority': args.keeper, 'host_authority':args.host}
            my_uuid = str(uuid.uuid4())
            ark = args.ark + my_uuid
            ark_url = alias_endpoint + ark
            #output = requests.put(alias_endpoint+ark, json=data, auth=auth, proxies=proxies).json()

            # print to log and stdout when a record is created
            if args.debug==True:
                print "LINE         :: " + my_line
                print "ENDPOINT     :: " + alias_endpoint
                print "ARK          :: " + ark
                print "ARK ENDPOINT :: " + alias_endpoint + ark
            LOGFILE.write(my_line + '\t' + ark_url + '\n')
            LOGFILE.flush()
            print( 'completed ( ' + str(counter) + ' ) :: ' + file_name  )

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


            

### SUB to create each ARK
#def mint_ARK(my_key, my_dictionary):
        

# LOGFILE.write('file_name' + '\t' + 'ref_md5' +'\t' + 'local_size(bytes)' + '\t' + 'local_md5' + '\t' + 'local_md5_check' + '\t' + 'dl_time(s)' + '\t' + 's3_size(bytes)' + '\t' + 's3_md5' + '\t' + 's3_md5_check' + '\t' + 'ul_time(s)' + '\n')
# LOGFILE.flush()



               
# data = {'size': 123, 'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}, 'release': 'private', 'keeper_authority': 'CRI', 'host_authority': ['PDC']}
# ark = 'ark:/31807/DC3-' + str(uuid.uuid4())
# output = requests.put(alias_endpoint+ark, json=data, auth=auth, proxies=proxies).json()



        


        
# ########################################

# # Create the data
# data={'form': 'object', 'size': 131171985, 'urls': ['ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000_genomes_project/data/PJL/HG03633/exome_alignment/HG03633.alt_bwamem_GRCh38DH.20150826.PJL.exome.cram', 'https://1000_genome_exome.s3.amazonaws.com/HG03633.alt_bwamem_GRCh38DH.20150826.PJL.exome.cram'], 'hashes': {'md5': '12f38c03368865ff8a671c98d471e782'}}

        


        

# data={'form': 'object', 'size': 131171985, 'urls': ['ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000_genomes_project/data/PJL/HG03633/exome_alignment/HG03633.alt_bwamem_GRCh38DH.20150826.PJL.exome.cram', 'https://1000_genome_exome.s3.amazonaws.com/HG03633.alt_bwamem_GRCh38DH.20150826.PJL.exome.cram'], 'hashes': {'md5': '12f38c03368865ff8a671c98d471e782'}}

# output = requests.post(index_endpoint, json=data, auth=auth).json() # left out proxies


#         # create an alias
# data = {'size': 131171985, 'hashes': {'md5': '12f38c03368865ff8a671c98d471e782'}, 'release': 'private', 'keeper_authority': 'kkeegan@uchicago.edu', 'host_authority': ['PDC']}
# ark = 'ark:/31807/DC4-' + str(uuid.uuid4())
# output = requests.put(alias_endpoint+ark, json=data, auth=auth).json() # left out proxies





# #####################################################################################################################################################
# # From Phillis -- code to add an entry to the id service
# https://gist.github.com/philloooo/2e9e6b80641fcb5443e2
# #####################################################################################################################################################
# import requests
# import uuid
# auth = ('username', 'password')
# endpoint = "https://signpost.opensciencedatacloud.org/"
# index_endpoint = endpoint +'index/'
# alias_endpoint = endpoint + 'alias/'

# # if you are inside pdc, set http proxy
# proxies = {'http': 'http://cloud-proxy:3128', 'https': 'http://cloud-proxy:3128'}


# # create an index

# data={'form': 'object', 'size': 123, 'urls': ['s3://endpointurl/bucket/key'], 'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}}

# output = requests.post(index_endpoint, json=data, auth=auth, proxies=proxies).json()

# # create an alias

# data = {'size': 123, 'hashes': {'md5': '8b9942cf415384b27cadf1f4d2d682e5'}, 'release': 'private', 'keeper_authority': 'CRI', 'host_authority': ['PDC']}
# ark = 'ark:/31807/DC3-' + str(uuid.uuid4())
# output = requests.put(alias_endpoint+ark, json=data, auth=auth, proxies=proxies).json()
# ### <- You only need to here for now to create arks

# # modify an index

# id = output['did']
# revision = output['rev']
# data['size'] = 5
# output = requests.put(index_endpoint+id+'?rev='+revision, auth=auth, proxies=proxies, json=data).json()

# # get an index
# requests.get(index_endpoint+id, proxies=proxies)

# # delete an index
# requests.delete(index_endpoint+id+'?rev='+ output['rev'], auth=auth, proxies=proxies)






