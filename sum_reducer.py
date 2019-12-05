import sys

sum_val = 0
column_name = None

for line in sys.stdin:
	record = line.strip().split("\t")
	if(len(record)==1):
		print(line)
	else:
		column_name = record[0]
		value = eval(record[1])
		sum_val+=value

if(column_name!=None):
	print("Sum of",column_name,':',sum_val)

