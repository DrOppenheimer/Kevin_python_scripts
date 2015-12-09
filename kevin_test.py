#!/usr/bin/python
import argparse
import os
import sys
print ("\n" + os.path.basename(sys.argv[0]) + " : A tool to extract ftp or S3 URLs from CDIS ARKs\n")
parser = argparse.ArgumentParser("ARK_parser.py")
parser.add_argument("ARK_id", help="ARK id to parse")


parser.add_argument("-s", "--store_type", help="Store type you want returned, \"ftp\" or \"s3\" URL", default="ftp")
args = parser.parse_args()

# exit with error if store type is not an acceptable value
if args.store_type == "ftp":
        print("store_type", args.store_type)
elif args.store_type == "S3":
        print("store_type", args.store_type)
else:
        print("\t( "+ args.store_type + " )" + " is not an acceptable value for store_type.\n\tIt Must be \"ftp\" or \"S3\"")
        exit()

print("ARK       ", args.ARK_id) 





# parser.add_argument("-o", "--output", help="output ftp or s3 URL", action="store_true")
# sys.argv[0]
