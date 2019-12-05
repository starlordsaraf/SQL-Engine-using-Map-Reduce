# SQL Engine Using Map Reduce
This project is an SQL engine that performs selection, projection and aggregation operations on structured data using Map-Reduce on Hadoop
<h1>Introduction</h1><br>
This project is a mini implementation of Hive that performs operations like LOAD dataset, SELECT queries and AGGREGATE_BY (max , min, sum, avg) and DELETE operations on a given dataset. These queries run map-reduce jobs in the background to give the output by performing the desired actions on the provided dataset.<br>

<h1>ALGORITHM/DESIGN</h1>
The project can implement LOAD, SELECT, AGGREGATE_BY and DELETE operations on a given dataset. <br><br>
LOAD:<br>                                                                                                                          
The dataset with its local path is given as input along with the required schema. On this command, in the predefined folder /files in hdfs, a folder with the name of the dataset is created and the csv file is stored in this folder. The schema of the dataset is stored locally and on hdfs in a folder /schema as a text file with the name of the dataset file.<br><br>
DELETE: <br>                                                                                                                                The dataset is deleted from hdfs and the folder with the dataset name in /files is also deleted. The schema text file in /schema is also deleted form hdfs and locally.<br><br>
SELECT:           <br>                                                                                                                      This query enables the user to view various columns of the dataset. Single column can be used, multiple columns or all columns (*) can be projected. Conditions can be added to the projection using the WHERE keyword followed by the condition on a given column. Conditions like logical operations (=, >=, <=, !=) for columns can be performed.<br>
The mapper is responsible for filtering the data based on the column names and the condition required and sends the result to the reducer. The reducer is an identity reducer and prints the values sent by the mapper.<br><br>
AGGRGEGATE_BY (min, max, sum, avg):      <br>                                                                                                This query is paired up with the SELECT query. It involves the functions: min, max, sum, avg to be performed on the projected column. The column has to be a numeric column for these functions else the query fails. The output returns a statement with the projected column name and the aggregated value based on the function specified.<br>
The mapper selects the rows based on the condition given in the where clause if any and passes the projected columns name and value to the reducer as a key value pair. The reducer performs the desired aggregation on these values and prints the desired output for the aggregation specified for the given column.

