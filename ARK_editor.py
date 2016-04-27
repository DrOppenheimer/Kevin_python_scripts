

#!/usr/bin/env python

import argparse
import json
import requests
import uuid
import re

parser = argparse.ArgumentParser(description='Script to edit exisiting ARK ids - handles only simple case of ids for a single file - but with multiple urls')
parser.add_argument('-l','--list', help='file with list of metadata filename\tsize_in_bytes\tmd5\turl1\turl...     ', required=True, default="test")
parser.add_argument('-u','--user', help='user', required=True)
parser.add_argument('-p','--password', help='password', required=True)
parser.add_argument('-e','--endpoint', help='endpoint', default='https://signpost.opensciencedatacloud.org/')
parser.add_argument('-k','--keeper', help='keeper authority', required=True)
parser.add_argument('-o','--host', help='host authority', default='PDC')
parser.add_argument('-a','--ark', help='ark prefix', default='ark:/31807/DC2-')
parser.add_argument('-d', '--debug', action="store_true", help='run in debug mode')
args = parser.parse_args()





parser = argparse.ArgumentParser(description='Script to mint ARK ids - handles only simple case of ids for a single file - but with multiple urls')
parser.add_argument('-l','--list', help='file with list of metadata filename\tsize_in_bytes\tmd5\turl1\turl...     ', required=True, default="test")
auth = (args.user, args.password)

id = output['did']
revision = output['rev']
data['size'] = 5
output = requests.put(index_endpoint+id+'?rev='+revision, auth=auth, proxies=proxies, json=data).json()






{

    "hashes": 

{

    "md5": "8924711b174974f19f6aa47f7e726717"

},
"host_authorities": [ ],
"keeper_authority": "PDC",
"limit": ​100,
"metadata": null,
"name": "ark:/31807/DC2-e0d641b2-a22e-421a-bb5d-e3e1bf6e3a92",
"release": "private",
"rev": "04b4fee8",
"size": ​3020542338,
"start": ​0,
"urls": 

    [
        "https://griffin-objstore.opensciencedatacloud.org/1000_genome_exome/HG00103.alt_bwamem_GRCh38DH.20150826.GBR.exome.cram",
        "https://s3.amazonaws.com/1000_genome_exome/HG00103.alt_bwamem_GRCh38DH.20150826.GBR.exome.cram"
    ]

}








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
