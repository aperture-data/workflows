#utils.py
import hashlib

def hash_string(string):
    return hashlib.sha1(string.encode('utf-8')).hexdigest()

def generate_bucket_hash(scheme,bucket):
    return hash_string( "{}/{}".format(scheme,bucket)) 
