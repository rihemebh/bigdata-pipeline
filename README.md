# Bigdata Pipeline


### Goal :
Create a pipeline that could support streaming/batch processing and store in a datalake:

### Prerequisite and Tools

- Docker  
- Apache Hbase 
- Apache kafka
- Apache Hadoop
- Java 
- Intellij


### Architecture


<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/bigdata.png" /> 


### Explain it
- The input dataset is stored in hdfs 
- Process it with spark : Some modifications on the column "Value"
- Send dataset lines through kafka producer 
- Consume these data
- Store it in hbase
- execute spark operations 


### Execution results : 

<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/exec.png" /> 

- Enterprises' Statistics from 2013 - 2020
<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/spark-operations-1.jpg" /> 
- Enterprises' Statistics per given year
<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/spark-operation-2.jpg" /> 

References: 

- https://spark.apache.org/docs/latest/rdd-programming-guide.html
- https://insatunisia.github.io/TP-BigData/
