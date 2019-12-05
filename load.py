#load dataset from hdfs and make schema
import os
import sys
from subprocess import Popen,PIPE

print('\nWelcome to our Mini-Hive, a cumbersome way to execute your SQL queries\n')

while(True):
	print('Enter 1 to enter SQL query')
	print('Enter 0 to exit')
	user_inp = input()
	if user_inp == '0':
		print('Hope you enjoyed the Mini-Hive experience')	
		break
	elif user_inp != '1':
		print('Invalid entry, please try again\n')
	
	else:
		
		my_cmd = input('Enter query: \n')

		query = my_cmd.split(' ')
		if(query[0]=='LOAD'):
			ctr=0
			folder = query[1].split('/')[-1][:-4]
			create = Popen(["hadoop","fs","-mkdir","/files/"+folder],stdin=PIPE, bufsize=-1)
			create.communicate()
			put = Popen(["hadoop","fs","-put",query[1],"/files/"+folder], stdin=PIPE, bufsize=-1)
			put.communicate()
			filename = query[1].split('/')[-1][:-3]+'txt'
			#print(filename)
			fp = open("./Schema/"+filename,'w+')
			schema = query[3][1:-1].split(',')
			#print(schema)
			for s in schema:
				s = s.split(':')
				fp.write("%s,%s,%s\n"%(ctr,s[0],s[1]))
				ctr+=1
			fp.close()
			put = Popen(["hadoop","fs","-put","/home/hduser/Desktop/Project/Schema/"+filename,"/schema"], stdin=PIPE, bufsize=-1)
			put.communicate()
			print("Table Loaded")
		else:
			if(query[0]=='SELECT'):
	
				input_dir = "/files/"+query[query.index('FROM')+1].split('/')[-1][:-4]
				output_dir = "/output"
		
				if('WHERE' not in query and "AGGREGATE_BY" not in query):
					#call mapper reducer without condition
			
					#files for command
					mapper_file_path = "/home/hduser/Desktop/Project/select_mapper.py"
					reducer_file_path = "/home/hduser/Desktop/Project/select_reducer.py"
					mapper_file = "select_mapper.py"
					reducer_file = "select_reducer.py"
			
					#hadoop streaming command
					command = "hadoop jar /home/hduser/Apache/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -file "+mapper_file_path+" "+reducer_file_path+" -mapper "+"'python3 "+mapper_file+' "'+my_cmd+'"'+"' -reducer "+'"python3 '+reducer_file+'" -input '+input_dir+" -output "+output_dir
					#print(command)
					os.system(command)
			
				else:
		
					if('AGGREGATE_BY' not in query):
						#call mapper reducer with condition without aggregate
						#files for command
						mapper_file_path = "/home/hduser/Desktop/Project/where_mapper.py"
						reducer_file_path = "/home/hduser/Desktop/Project/where_reducer.py"
						mapper_file = "where_mapper.py"
						reducer_file = "where_reducer.py"
				
						#hadoop command
						#hadoop streaming command
						command = "hadoop jar /home/hduser/Apache/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -file "+mapper_file_path+" "+reducer_file_path+" -mapper "+"'python3 "+mapper_file+' "'+my_cmd+'"'+"' -reducer "+'"python3 '+reducer_file+'" -input '+input_dir+" -output "+output_dir
						#print(command)
						os.system(command)
				
					else:
						#call mapper reducer with condition and aggregate
						#mapper- aggregate_mapper.py
						flag=0
						mapper_file_path = "/home/hduser/Desktop/Project/aggregate_mapper.py"
						mapper_file = "aggregate_mapper.py"
						if(query[-1]=='min'):
							#call min_reducer.py
							reducer_file_path = "/home/hduser/Desktop/Project/min_reducer.py"
							reducer_file = "min_reducer.py"
					
						elif(query[-1]=='max'):
							#call max_reducer.py
							reducer_file_path = "/home/hduser/Desktop/Project/max_reducer.py"
							reducer_file = "max_reducer.py"
				
						elif(query[-1]=='sum'):
							#call sum_reducer.py
							reducer_file_path = "/home/hduser/Desktop/Project/sum_reducer.py"
							reducer_file = "sum_reducer.py"
						elif(query[-1]=='avg'):
							#call avg_reducer.py
							reducer_file_path = "/home/hduser/Desktop/Project/avg_reducer.py"
							reducer_file = "avg_reducer.py"
						else:
							flag=1
							print("Invalid Aggregate Function")
				
						#if flag=0 execute command
						if(flag==0):
							command = "hadoop jar /home/hduser/Apache/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -file "+mapper_file_path+" "+reducer_file_path+" -mapper "+"'python3 "+mapper_file+' "'+my_cmd+'"'+"' -reducer "+'"python3 '+reducer_file+'" -input '+input_dir+" -output "+output_dir
							os.system(command)
				
				#print output to console
				output = Popen(["hadoop","fs","-cat","/output/part-00000"], stdout = PIPE)
				for line in output.stdout:
					line = line.decode("utf-8")
					line = line.strip()
					print(line)
						
				#delete ouput file to get new outputs
				del_s = Popen(["hadoop","fs","-rm","/output/_SUCCESS"], stdin=PIPE, bufsize=-1)
				del_s.communicate()
				del_p = Popen(["hadoop","fs","-rm","/output/part-00000"], stdin=PIPE, bufsize=-1)
				del_p.communicate()
				del_o = Popen(["hadoop","fs","-rmdir","/output"], stdin=PIPE, bufsize=-1)
				del_o.communicate()
					
			elif(query[0]=='DELETE'):
					data_filename = query[1].split('/')[-1][:-3]+'csv'
					folder = query[1].split('/')[-1][:-4]
					#delete csv file and folder from hdfs
					del_csv = Popen(["hadoop","fs","-rm","/files/"+folder+"/"+data_filename], stdin=PIPE, bufsize=-1)
					del_csv.communicate()
					del_dir = Popen(["hadoop","fs","-rmdir","/files/"+folder], stdin=PIPE, bufsize=-1)
					del_dir.communicate()
					#call command to delete schema from hdfs
					filename = query[1].split('/')[-1][:-3]+'txt'
					put = Popen(["hadoop","fs","-rm","/schema/"+filename], stdin=PIPE, bufsize=-1)
					put.communicate()
					print("Table Deleted")
			else:
				print("Command is Invalid") 
			
			
			
