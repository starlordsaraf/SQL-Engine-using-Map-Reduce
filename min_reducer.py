import sys

min_val = 100000000000000000000000000
column_name = None
for line in sys.stdin:
	record = line.strip().split("\t")
	if(len(record)==1):
		print(line)
	else:
		column_name = record[0]
		value = eval(record[1])
		if(value<min_val):
			min_val = value

if(column_name!=None):
	print("Min of",column_name,':',min_val)
