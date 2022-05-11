# bigdata-pipeline

### Goal :
Create a pipeline that could support streaming/batch processing using habse as database, spark for processing and hadoop hdfs as datalake:

### Architecture


<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/bigdata1.png" /> 


### Explain it
- The input dataset is stored in hdfs 
- Process it with spark : Some modifications on the column "Value"
- Send dataset lines through kafka producer 
- Consume these data
- Store it in hbase


### Execution results : 


<img src="https://github.com/rihemebh/bigdata-pipeline/blob/main/exec.png" /> 
