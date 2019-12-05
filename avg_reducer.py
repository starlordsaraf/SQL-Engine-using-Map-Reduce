import sys

sum_val = 0
count = 0
column_name= None
for line in sys.stdin:
	record = line.strip().split("\t")
	if(len(record)==1):
		print(line)
	else:
		column_name = record[0]
		value = eval(record[1])
		sum_val+=value
		count+=1

if(column_name!=None):
	print("Average of",column_name,':',sum_val/count)
