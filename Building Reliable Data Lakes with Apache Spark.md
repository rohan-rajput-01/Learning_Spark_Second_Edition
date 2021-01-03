# Chapter 9
## Building Reliable Data Lakes with Apache Spark

In this chapter we will discuss two broad class of storage solution, data bases and data lakes and how to use Spark with them. Finally a new wave of storage solution lakehouses.

## The importance of an Optimal Storage Solution 

Properties of a desired storage solution 

- *Scalability and performance*
  - The storage solution should be able to scale to the volume of data and provide read/write throughput and latency that the workload requires 
- *Transaction support*
  - Complex workloads are often reading and writing data concurrently, so support for ACID transaction is essential quality of the end results
- *Support for diverse data formats*
  - The storage solution should be able to store unstructured data (text files), semi-structured data (json) and structured data (tabular)
- *Support dicers workloads*
  - The storage should support diverse range of business workloads including 
    - SQL Workloads like BI analytics 
    - Batch workloads like traditional ETL jobs processing raw unstructured data 
    - Streaming workloads like real-time monitoring and alerting 
    - ML and AI workloads like recommendations and churn prediction 
  - *Openness*
    - Support wide range of workloads ofter requires the data to be stored in the open data format 
We will discuss about databases and data lakes and how to use spark with them. Finally lakehouses which provide scalability, flexibility and transactional guarantees of databases. 

# Databases 
In this section we will discuss different databases along with their workloads, and how to use spark analytics with the workloads on databases. We will also discuss some of the limitations of no-SQL workloads.

## Small introduction of Databases 
Databases are designed to store data which must strictly adhere to the schema, this way they tightly coupled to their internal layout of the data and indexes in on-disk files with their highly optimized query processing engines, thus providing very fast computations on the stored data along with some strong transactional ACID guarantees on the all read/write operations. 

These workloads can be classified into two broad categories.

### Online transaction processing (OLTP) workloads 
  Like bank account transactions, OLTP workloads are typically high-concurrency, low latency, simple queries that read or update a few records at a time. 
### Online analytics processing (OLAP)
  OLAP workloads, like periodic reporting, are typically complex queries (involving aggregates and joins) that requires high-throughput scans over many queries.
Spark engine is primarily designed to perform OLAP task, we will discuss about the analytics workloads for the rest of the chapters 

### Reading from and Writing to Databases Using Apache Spark 
Apache Spark supports various connectors through JDBC connector like PostgreSQL, MySQL it also connects with modern daabases like Snowflakes, Azure Cosmo DB using various connectors. 

### Limitations of Databases 
There are two major trends emerged from last dacaded 

- *Growth in data sizes*
  - With the advent of big data, there has been a global trend in the industry to measure and collect everything in order to understand trend and user behaviors.
- *Growth in diversity o analytics*
  - Along with the increase in data collection, there is a need for a deeper insight. This has led to an explosive growth of complex analytics like machine learning and deep learning 
Databases have been shown to be rather inadequate at accommodating these new trends, because of following limitations 

- *Databases are extremely expensive to scale out*
  - Databases are extremely efficient at processing data on a single machine but growing scale of data demands multiple machine to process the data large quantity of data. Some databases can keep up with these trends but they are expensive to acquire and maintain. 
- *Databases don't support non-SQL based analytics very well*
  - Databases store data in highly optimized format which works efficiently on SQL type data. However, they don't work similarly on no-SQL based operations for analytics and machine learning. 
  
## Data Lakes 
A data lake is a distributed storage solution that runs on commodity hardware and easily scales out horizontally. We will start with a discussion of how data lakes satisfy the requirements of modern workloads, then see how the Apache Spark integrates with data les to make workloads scale to data of any size. Finally, we will explore the impact of the architectural sacrifices made by data lakes to achieve scalability. 

### Introduction to Data Lakes 
Data lake architecture allows decoupling of distributed compute from distributed storage. This allows each system to scale out as needed by the workloads. It includes the open file format which can be read and processed by any processing engine. For example HDFS and Parquet. 

Organizations build their data lakes bt independently choosing the following 
- *Storage System*
  They choose to either run HDFS on a cluster machine or use any cloud object store
- *File Format*
  - Depending on the downstream workloads, the data is stored as files in either structured (Parquet, ORC), semi-structured(JSON), or sometimes even unstructured format(e.g. text, image, audio, video)
- *Processing Engines*
  - This can be either batch processing (Spark, Presto, HIve), a Streaming engine (Spark, Flink) or a ML library (MLlib Sk-learn)

### Reading from and Writing to Data Lakes using Apache Spark
Spark provides various features for data lake build up.

- *Support for diverse workloads*
  - Spark provides all the necessary tools to handle a diverse range of workloads, including batch processing, ETL operations, SQL workloads using Spark SQL, stream processing using Structured Streaming and Machine Learning using MLlib
- *Support for diverse file formats*
  - It support various file formats structured, semi-structured and un-structured format. 
- *Support for various diverse filesystem*
  - Spark support various data from any storage systems that support Hadoop's FileSystem APIs. The API is de facto standard in the big data system, most cloud and on-premise storage system. However, for some cloud storage like AWS S3 we have to configure the Spark to access the data in secure manner. 

### Limitation of Data Lakes 
The most egregious limitation of the data lake is lack of transactional guarantees, Data lakes fail to provide ACID guarantees on 

- *Atomicity and isolation*
  - Processing engine write data in data lakes as many files in a distributed manner. If the operation fails there is not mechanism to roll back the files already written thus leaving behind the potentially corrupted data.
- *Consistency*
  - Lack of atomicity on failed writes further causes renders to get an inconsistent view of data. It is hard to ensure the data quality even with successfully written data. A very common issue with the data lakes is accidentally writing out data files in a format and schema inconsistent with the existing data. 

We can use following tricks to avoid these problems 

- Large collection of data files in data lakes are often partitioned by subdirectories based on a column's value (a parquet by date). TO achieve atomic modification to the existing data, often entire subdirectories are rewritten just to update or delete a few records 
- The scheduled data update jobs and data querying jobs are often staggered to avoid concurrent access to data and any inconsistencies caused by it. 

## Lakehouses ; The Next Step in the Evolution of Storage Solutions 

The *Lakehouse* is a new paradigm that combines the best element of data lakes and data warehouses for OLAP workloads. Lakehouses are enabled by a new system design that provides data management feature similar to databases directly on low-cost scalable storage used for data kales. They provide following features. 

- *Transaction support*
  - Similar to databases, lakehouse provide ACID guarantees in the presence of concurrent workloads 
- *Schema enforcement and governance*
  - Lakehouse prevent data with an incorrect schema being inserted into a table, and when needed, the table schema can be explicitly evolved to accommodate ever-changing data. The system should be able to reason data integrity, and it should have robust governance and auditing mechanisms. 
- *Support for diverse data types in open formats*
  - Unlike databases, but similar to data lakes, lakehouses can store, refine, analyze and access all type of data needed for many new data applications, be it structured, semi-structured, or unstructured. To enable a wide variety of tools to access it directly and efficiently, the data must be stored in open format with standard APIs to read and write them. 
- *Support ofr diverse workloads*
  - Powered by the variety of tools reading data using open APIs. lakehouses enable diverse workloads to operate on data in a single repo. Breaking down isolated data silos enables developers to more easily build diverse and complex data solutions from traditional SQL and streaming analytics to machine learning. 
- *Support for updates and deletes*
  - Complex dt operations like change-data-capture(CDC) and slowly changing dimension(SCD) operations require data in tables to be continuously updates. Lakehouse allow data to be concurrently deleted and updated with transactional guarantees
- *Data governance*
  - Lakehouses provide the tools with which we can reason with data integrity and audit all the data changes for policy compliance

Apache Hudi, Iceberg and Delta Lake can be used to build lakehouses with these properties, all have similar architecture, they all have open data storage format that do the following 

- Store large volumes of data in structured file format on scalable filesystem 
- Maintain a transaction log to record a timeline of atomic changes to the data 
- Use the log to define versions of the table data and provide snapshot isolation guarantees between readers and writers 
- Support reading and writing to the tables using Apache Spark 
  
### Apache Hudi 
It is acronym for Hadoop Update, Delete and Incremental - a data format that is designed for the incremental upserts and deletes over key/value style data. The data is stored as a combination of columnar format(parquet) and row based avro. Common features include 

- Upserting with fast, pluggable indexing 
- Atomic publishing of data with rollback support 
- Reading incremental changes to a table 
- Savepoints for data recovery 
- File size and layout management using statistics 
- Async compaction of row and columnar data 

### Apache Iceberg 
Iceberg is a data storage that scales to petabytes in a single table and schema evolution properties. It provides following features 

- Schema evolution by adding, dropping, updating, renaming ans reordering of the columns, field and/or nested structures 
- Hidden partitioning, which under the cover creates the partitions values for rows in a table 
- Partition evolution, were it automatically performac a mea oepration to update table layout 
- time travel 
- rollback to previous version to correct errors 
- serializable isolation, even between concurrent writers 

### Delta Lake 
Delta lake is an open data storage format that provides several transactional guarantees and enable schema enforcement and evolution. It provides several features that are unique.

- Streaming reading and writing to tables using Structured Streaming sources and sinks. 
- Update, delete and merge operations, even in Scala and Python API 
- Schema evolution either by explicitly altering the table schema or by implicitly merging a DataFrame's schema to table's during the DataFrame's write. 
- Time travel, which allow us to query specific table snapshot by ID or by timestamp. 
- Rollback to previous version to correct errors
- Serializable isolation between multiple concurrent writers performing ant SQL batch or streaming operations 
  
## Building Lakehouses with Apache Spark and Delta Lake 
We will explore 
- Reading and writing Delta Lake using Apache Spark 
- How Delta Lake allows concurrent batch and streaming writes with ACID guarantees 
- How Delta Lake ensures better data quality by enforcing schema on all writes while allowing for explicit schema evolution.
- Building complex data pipelines using update, delete and merge operations all of with ensures ACID guarantees 
-  Auditing the history of operations that modifies a Delta Lake table and travelling back in the time by querying earlier versions of the table
  
### Configuring Apache Spark with Delta Lake 
We can Spark with Delta Lake library in one of the following ways 

- *Set up an interactive shell*
  `pyspark --packages io.delta:delta-core_2.12:0.7.0`

### Loading Data into a Delta Lake Table 
We can use Delta lake by ust replacing any structured format like `format(parquet)` to `format(delta)`

```
# In Python
# Configure source data path 
sourcePath = 'path_to_dataset'

# Configure delta ale Path
deltaPath = 'path_to_deltalake'

#Create the Delta Lake with the same loans data 
(spark.read.format("parquet").load(sourcePath)
  .write.format("delta").save(deltaPath))

#Create a view on the data called loans_delta
spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")


```
Now in SQL we can write 
```
spark.sql("SELECT COUNT(*) FROM loans_delta").show()

```

### Loading Data Streams into a Delta Lake Table 
We can easily modify our existing Structured Streaming jobs to write to and read from a Delta Lake table by setting the format to "delta".

```
# In Python 
newLoadStreamDF = ... # Streaming DataFrame with new loans data 
checkpointDir = ... # Directory for streaming checkpoints 
streamingQuery = (newLoanStreamDF.writeStream
  .format("delta")
  .option("checkpointLocation", checkpointDir)
  .trigger(processingTime, "10 seconds")
  .start(deltaPath))
```
Delta Lake has few advantages over traditional formats like JSON, parquet or ORC 
- *It allows write from both batch and streaming jobs into the same tables*
  - With other data format the SS job will overwrite any existing data in the table. Delta Lake advanced metadata management allows both batch and streaming data to written 
- *It allows multiple streaming jobs to append data to same table*
  - Delta lake metadata maintains transaction information for each streaming query, thus enabling any number of streaming queries to concurrently write into a table with exactly-once guarantees
- *It provides ACID guarantees even under concurrent writes*
  - Unlike built-in formats, Delta Lake allows concurrent batch and streaming operations to write data with ACID guarantees 

### Enforcing Schema on Write to Prevent Data Corruption 
It is challenging to write a program for preventing data corruption caused by writing incorrectly formatted data, since these format define the data layout of individual files and not the entire table, there is no mechanism to prevent any Spark job from writing files with different schemas into existing tables. This means there are no guarantees of consistency for the entire table of many Parquet files 

The Delta Lake format records the schema as table-level metadata. All write to a Delta Lake table can verify whether the data being written has a Schema compatible with that of the table. If it is not compatible, Spark will throw an error before any data is written and committed to the table, thus preventing such accidental data corruption. 

### Evolving Schemas to Accommodate Changing Data 
We can add extra column to the existing schema by setting the opetion `mergeSchema` to `true`.

```
# In Python 
(loadUpdates.write.format("delta").mode("append")
  .option("mergeSchema", "true")
  .save(deltaPath))
```
With this, the column closed will be added to the table schema, and new data will be appended. When existing rows are read, the value of the new column is considered as NULL. 

#### Transforming Existing Data 
Delta Lake supports the DML commands UPDATE, DELETE and MERGE, which allow us to build complex data pipelines. These commands can be invoked using Java, Scala, Python, and, SQL giving users the flexibility of using the commands with any APIs they are familiar with, using either DataFrames or tables. Furthermore, each of these data modification operations ensures ACID guarantees. 

These are some read world use cases. 
##### Updating data to fix errors 
We can use Delta Lake programmatic API as follows to update a record. 
```
# In Python 
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, deltaPath)
deltaTable.update("add_state = 'OR'", {"addr_state" : "'WA'"})
```
##### Deleting user-related data
```
# In Python
deltaTable = DeltaTable.forPath(spark, deltaPath)
deltaTable.delete("funded_amount >= paid_amount")

```
##### Upserting change data to a table using merge()
This operation can change multiple tables with one operation. We can upsert the change by using `DeltaTable.merge()` operation. 
```
# In Python 
(deltaTable
  .alias("t")
  .merge(loanUpdates.alias("s"), "t.loan_id = s.loan_id")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute())
```
This operation is can also be used with SQL MERGE command. For streaming service we can use `forBatch()` to apply the changes in the micro-batch to the Delta Lake table. 

##### Deduplicating data while inserting using insert-only merge 
Delete action 
`MERGE ... WHEN MATCHED THEN DELETE`
Clause condition 
`MERGE ... WHEN MATCHED AND <condition> THEN ...`
Optional actions 
`MATCHED and NOT MATCHED` clauses are optional 

#### Auditing Data Changes with Operation History 
All of the changes to the Delta Lake table are recorded as commits in the tables and they are logged and versioned. We can query the table's operation history as noted in the following code snippet. 

```
deltaTable.history().show()
# For limited view 
(deltaTable
.history(3)
,select("version", "timestamp", "operation", "operationParameters")
.show(truncate=False)
  )

```
##### Querying Previous Snapshot of a Table with Time Travel 
We can query previous snapshots of a table by using the `DataFrameReader` options "versionsAsOf" and "timestampAsOd"

```
(spark.read
  .format("delta")
  .option("timestampAsOf", "2020-01-01") # timestamp after table creation 
  .load(deltaPath))
(spark.read.format("delta")
)

(spark.read.format("delta")
  .option("versionAsOf", "4")
  .load(deltaPath))
```

The is useful in a variety of situation such as : 

- Reproducing machine learning experiments and reports by rerunning the job on a specific table version 
- Comparing the data changes between different versions of auditing 
- Rolling back incorrect changes by reading a previous snapshot as a DataFrame and overwriting thr table with it 
- 
