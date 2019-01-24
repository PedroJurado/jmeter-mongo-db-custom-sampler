#!/usr/bin/python

import uuid
import sys
import random
import string

if(len(sys.argv)!=3):
	sys.exit("Syntax: "+sys.argv[0]+" <no_values> <value_size_in_bytes>");


no_vals=long(sys.argv[1]);
value_size=long(sys.argv[2]);

def get_random_value(size_in_bytes):
	return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size_in_bytes))

def get_random_key():
	return str(uuid.uuid4());

for i in range(no_vals):
	print("{},{}".format(get_random_key(),get_random_value(value_size))) 
