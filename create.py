#!/usr/bin/env python

# From Mark's script
# see ~/git/Mark_mint_ARK # for the original

import os
import json
import uuid

import requests


# First, get some alias name from somewhere. Here we'll just generate it:
alias = 'ark:/31807/DC0-{uuid}'.format(uuid=uuid.uuid4())

# Next, we want to generate some data to associate with the alias:
alias_json = {

    # There are other pieces of data that can be associated with the alias,
    # however the two pieces you really need to specify are the size and
    # hashes of the data being named by the alias. We'll see why shortly.
    'size': 1234, # Byte size.
    'hashes': {
        'md5': 'bfa1a386b09fb726b15c109e139f093f', # MD5 checksum. Also valid are SHA256, SHA512.
    },
}

# Finally, we'll create the alias by PUTing to the index service:
url = 'https://signpost.opensciencedatacloud.org/alias/{alias}'.format(alias=alias)

requests.put(url, data=json.dumps(alias_json)) # Using the requests module, or...

# ...Using a system call to curl.
os.system('curl -X PUT {url} -H "Content-Type: application/json" -d "{json}"'.format(
    url=url,
    json=json.dumps(alias_json),
))

# If you now GET the alias url, you should see a new alias with the size and
# hashes defined above.


# Now that the alias has been created, we can add some index records to tie
# known locations to the alias. We could have done this before creating the
# alias or after - they're two separate processes that are linked when a user
# issues a query for location information.

# We'll take the same alias_json as above and reuse the size and hashes while
# adding some known urls for the data:

index_json = alias_json
index_json['form'] = 'object' # This should simply be hardcoded at this time - experimental feature.
index_json['urls'] = [
    'https://www.google.com/index.html',
    'ftp://www.google.com/some/other/path.html',
]

# We can then simply POST this to the index service.
# NOTE We do not specify an identifier for the index record.
url = 'https://signpost.opensciencedatacloud.org/index/'

requests.post(url, data=json.dumps(index_json)) # Using the requests module, or...

# ...Using a system call to curl.
os.system('curl -X POST {url} -H "Content-Type: application/json" -d "{json}"'.format(
    url=url,
    json=json.dumps(index_json),
))


# At this point, we have now created an alias record and an index record.
# If you perform a GET on the alias, you should now see urls appear along
# with the size and hashes of the data. These locations are linked at query
# time by the index system.
