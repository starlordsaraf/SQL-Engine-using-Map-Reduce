import sys
import csv
from subprocess import Popen,PIPE

try:
	query = sys.argv[1]
	command = query
	query = query.split(' ')
	filename = query[3].split('/')[-1][:-3]+'txt'
	
	#fp = open("./Schema/"+filename,'r')
	#fp = open('hdfs://localhost:9000/schema/'+filename)
	#fp = open("file:///home/hduser/Desktop/Project/Schema/"+filename,'r')
	

	flag=0
	with open("/home/hduser/Desktop/Project/Schema/"+filename) as fp:
		for line in fp:
			column = line.strip().split(',')
			if(column[1]==query[1]):
				#column found
				flag = 1
				#check datatype for aggregation
				if(column[2]=='string'):
					print("Aggregation not possible on strings")
				else:
					#do mapper job
					if('WHERE' not in query):
						#if(query[5] in ['min','max','sum','avg']):
						for record in sys.stdin:
							record = record.split(',')
							print("%s\t%s"%(column[1],record[int(column[0])]))
						#else:
						#	print("Invalid Query")
					else:
						cmdlist1 = command.split(":")
						cmdlist=cmdlist1[0].split()
						sel_ind = cmdlist.index('SELECT')	
						from_ind = cmdlist.index('FROM')
						where_ind= cmdlist.index('WHERE')
						sel_cols = [x for x in cmdlist[sel_ind+1:from_ind]]
						sel_cols = "".join(sel_cols)
						sel_cols = sel_cols.split(',')
						where_col = cmdlist[where_ind+1]
						operation= cmdlist[where_ind+2]
						if(operation == "BETWEEN" ):
							value="("+cmdlist[where_ind+3]+","+cmdlist[where_ind+5]+")"
						elif(operation == "NOT"):
							if(len(cmdlist1)>1):
								value="('"
								for i in range(1,len(cmdlist1)-1):
									value=value+cmdlist1[i]+"'"	
								value=value+")"
							else:
								value=cmdlist[where_ind+4]
						else:
							if(len(cmdlist1)>1):
								value="('"
								for i in range(1,len(cmdlist1)-1):
									value=value+cmdlist1[i]+"'"	
								value=value+")"
							else:
								value=cmdlist[where_ind+3]
						#print(value)
						if (operation == '='):
							operation ='='+operation;

						columns = [] 
						datatype=[]  # column names in the dataset
						with open("/home/hduser/Desktop/Project/Schema/"+filename) as fp:
							for line in fp:
								line=line.strip()
								line = line.split(',')
								columns.append(line[1]) 
								datatype.append(line[2])	
						#print(columns)
						#print(where_col)		
						where_ind = columns.index(where_col)

						infile = sys.stdin
						for row in infile:
							row = row.strip()
							row = row.split(',')
							if(datatype[where_ind]=='string'):
								a="'"+row[where_ind]+"'"
							else:
								a=row[where_ind]
							if(operation == "IN" and  (eval(a+"in"+value)==True) or (operation == "NOT" and (eval(a+"not in"+value)==True)) or (operation!="IN" and operation!="NOT" and operation!="BETWEEN"and (eval(a+operation+value)==True)) or (operation=="BETWEEN" and (eval(a+"in range"+value)==True))): 
	
								elems = []  # elements from the row to be displayed
								#elems.append(row[where_ind])
		
								if sel_cols[0] != '*':
	
									for col in sel_cols:
										elem_ind = columns.index(col)
										elems.append(row[elem_ind])    
				
								else:						# if '*' is provided then all columns need to be provided
		
									for ind in range(len(columns)):
										elems.append(row[ind])
		
								elems = ",".join(elems)
								print("%s\t%s" %(query[1],elems))
						
	
		
	if(flag==0):
		print("Column dosen't exist")
except Exception as e:
	print(e)

	
		

