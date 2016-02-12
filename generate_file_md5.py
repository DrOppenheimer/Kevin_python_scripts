#!/usr/bin/python

# md5 large files
# http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
# execfile("~/git/Kevin_python_scripts/generate_file_md5.py")

import hashlib
import os

def generate_file_md5(rootdir, filename, blocksize=2**20):
    m = hashlib.md5()
    with open( os.path.join(rootdir, filename) , "rb" ) as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update( buf )
            return m.hexdigest()
