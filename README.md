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


### Execution results : 

<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/exec.png" /> 


<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/spark-operations-1.png" /> 


<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/spartk-operations-2.png" /> 

References: 

- https://spark.apache.org/docs/latest/rdd-programming-guide.html
- https://insatunisia.github.io/TP-BigData/
