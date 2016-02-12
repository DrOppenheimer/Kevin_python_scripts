#!/usr/bin/python

# md5 large files
# http://stackoverflow.com/questions/1131220/get-md5-hash-of-big-files-in-python
# execfile("~/git/Kevin_python_scripts/generate_file_md5.py")

import hashlib

def generate_file_md5(filename):
    md5 = hashlib.md5()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(8192), b''): 
            md5.update(chunk)
    return md5.hexdigest()

# #def generate_file_md5(rootdir, filename, blocksize=2**20):
# def generate_file_md5(filename, blocksize=2**20):
#     m = hashlib.md5()
#     #with open( os.path.join(rootdir, filename) , "rb" ) as f:
#     with open( filename , "rb" ) as f:
#         while True:
#             buf = f.read(blocksize)
#             if not buf:
#                 break
#             m.update( buf )
#             return m.hexdigest()


# def generate_file_md5(file):
#     os_string = "md5sum " + file + " | cut -d \" \" -f 1"
#     print os_string
#     my_md5 = subprocess.check_output(os_string, shell=True)
#     return my_md5

#result = subprocess.check_output("md5sum HG00101.alt_bwamem_GRCh38DH.20150826.GBR.exome.cram | cut -d \" \" -f 1", shell=True)
