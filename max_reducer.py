import sys

max_val = -1
column_name = None
for line in sys.stdin:
	record = line.strip().split("\t")
	if(len(record)==1):
		print(line)
	else:
		column_name = record[0]
		value = eval(record[1])
		if(value>max_val):
			max_val = value

if(column_name!=None):
	print("Max of",column_name,':',max_val)
