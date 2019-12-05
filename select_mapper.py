import sys
import csv

try:
	command = sys.argv[1]
	cmdlist = command.split()
	filename = cmdlist[3].split('/')[-1][:-3]+'txt'
	infile = sys.stdin

	for row in infile:
		row = row.strip()
		row = row.split(',')
	
	
	
		sel_ind = cmdlist.index('SELECT')
	
		from_ind = cmdlist.index('FROM')

		sel_cols = [x for x in cmdlist[sel_ind+1:from_ind]]
	
		sel_cols = "".join(sel_cols)
		sel_cols = sel_cols.split(',')

	
		# sel_cols contains all the columns to be selected
		# load schema from hdfs
	
		#fp = open("./Schema/data.txt",'r')  # schema is accessed
		columns = []   # column names in the dataset
		with open("/home/hduser/Desktop/Project/Schema/"+filename) as fp:
			for line in fp:
				line = line.split(',')
				columns.append(line[1])
	   
		
		elems = []  # elements from the row to be displayed
	
		if sel_cols[0] != '*':
		
			for col in sel_cols:
				elem_ind = columns.index(col)
				elems.append(row[elem_ind])    
		
	
	
			elems = " ".join(elems)
	
			print("%s\t%s" %(str(1),elems))
		
		else:						# if '*' is provided then all columns need to be provided
			
			for ind in range(len(columns)):
				elems.append(row[ind])
			
			elems = " ".join(elems)
			
			print("%s\t%s" % (str(1),elems))
except:
	print("Invalid Query")
	
	
	

	
