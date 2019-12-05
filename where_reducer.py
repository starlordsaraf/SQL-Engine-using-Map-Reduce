#!/usr/bin/python

import sys

for row in sys.stdin:
	#print(row)
	row = row.strip()
	row = row.split("\t")
	if(len(row)==1):
		print(row[0])
	else:
		elems = row[1]
		#count = row[1]
	
		print("%s" % (elems))
	
	
