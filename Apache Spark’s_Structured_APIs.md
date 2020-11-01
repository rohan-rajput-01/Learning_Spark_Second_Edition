# Apache Spark's Structured APIs 

This chapter will discuss in detail about the structured API of Spark (DataFrames and DataSet). These APIs are built on the top of Spark native RDDs. This will also provide information regarding the Spark SQL engine that underpins these structured high-level APIs. 

## Spark RDD 
The RDD (Resilient Distributed Dataset) is the most basic abstraction in Spark. There are three vital characteristics of RDD.

- Dependencies 
- Partitions 
- Compute function : Partition => Iterator[T]

All three are integral to simple RDD programming API model upon which all higher-level functionality is constructed. 
First with the list of dependencies that instructs Spark how an RDD is constructed with its input required. Spark can recreate an RDD from these dependencies and replicate the operations on it. This provides the RDD resiliency 

Second, partitions provide RDD ability for split the work in parallel across different executors. This provides the locality feature where the data which is in the nearest storage get priority. 

Third, RDD has a compute function that produces an Iterator[T] foe a data that will be stored in RDD

There are some problems with this approach. The compute function is hidden from the Spark, it doesn't provide the information what type of transformation is required for the task. It only sees lambda expression. Whereas the Iterator[T] data type is also opaque for Python RDD, Spark only knows generic object in Python. Spark has no information about the accessing the column of a certain type of an object. Spark can only serialize the object as a series of bytes without any data compression technique.

## Spark Structuring 

Spark structuring allows various data structuring techniques on the data which allows various operations to be performed on structure like filtering, selecting, joining and aggregating. The final scheme of order and structure allow us to arrange the data into the tabular form like SQL. 

### Key Merits and Benefits 
Structuring allows number of benefits like better performance and space efficiency. It provides expressivity, simplicity, compose ability and uniformity.

## DataFrame API
Scala DataFrame API is inspired by Python Pandas DataFrame which has identical look and feel. 
![](images/6.png)
DataFrame is immutable in Spark that means Spark remembers their lineage. We can add or change the data types of columns, creating new DataFrames while the previous versions are preserved. 

### Spark's Basic Data Types 
Spark has basic internal data types which includes for both Python and Scala. For Scala it has `String, Bytes, Long and Maps`
![](images/7.png)
Similarly for python we have 
![](images/8.png)

### Spark's Structured and Complex Data Types 
For Complex structures Spark also provides some complex APIs 
![](images/9.png)
![](images/10.png)

### Schemas and Creating DataFrames 
Schema is a column with name and datatype. Defining a Schema helps Spark to infer the data and read efficiently from a larges file. There are two ways to define a schema

### Two ways to define schema
We can define the schema using DDL (Data Definition Language) 
Example of defining in DataFrame API
```
// In Scala 
import org.apache.spark.sql.types._
val schema = StructType(Array(StructField("author", StringType, false),
            StructField("title", StringType, false),
            StructField("pages", IntegerType, false)))

// In python 
from pyspark.sql.types import *
schema = StructType([StructField("author", StringType(), False),
            StructField("title", StringType(), False),
            StructField("pages", IntegerType(), False)])

```

Example of defining using DDL 
```
// In Scala 
val schema = "author STRING, title STRING, pages INT"

# In Python
schema = "author STRING, title STRING, pages INT"

```
## Columns and expression 
DataFrame in Spark are inspired by the Pandas and R DataFrame they are like RDBMS table. We can list all the columns 
by their name and perform operation using relational or computation expressions. In spark columns are object with public methods (`Column` type) 

We can also apply logical or mathematical operations using `expr("columnName * 5")` where the `expr()` is a part of the `pyspark.sql.function` package. 

```
// In scala 
import org.apache.spark.sql.functions._
blogsDF.columns 

// Access a Particular column with col and it returns a Column type 
blogsDF.col("Id")

// Use an expression to compute a value 
blogsDF.select(expr("Hits * 2")).show(2)
// or 
blogsDF.select(col("Hits") * 2).show(2)
// Use an expression to compute bigger hitters for blogs 
// This adds a new columns, Bigger Hitters, based on conditional //expression 
blogsDF.withColumn("Big Hitters", (expr("Hits > 10000")))
// Concatenate three columns, create a new column, and show the
// newly created concatenated column
blogsDF
.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
.select(col("AuthorsId"))
.show(4)
// Sort by column "Id" in descending order
blogsDF.sort(col("Id").desc).show()
blogsDF.sort($"Id".desc).show()

```

`Column object` in a DataFrame can't exist in isolation; each column is a part of a row in a record and all the row together constitute a DataFrame, which as we wil see later in the chapter is really a Dataset[Row] in Scala

### Rows 
A row in Spark is a generic `Row Object`, containing one or more columns. Each colum many be od same data type or the have different data types. Row is an object in Spark and an Ordered collection of the fields, we can instantiate a Row in each of the Spark's supported language and access its fields by an index starting at 0 
```
// In scala 
import org.apache.spark.sql.Row 
// Create a Row 
val blogRow = Row(6, "Reynold", "url" , "3/2/2015", Array("twitter", "LinkedIn"))
// Access the Row 
blogRow(1)

# In python 
from pyspark.sql import Row 

blog_row = Row(6, "Reynold", "url", "3/2/2015", ["twitter" , "LinkedIn"])
blog_row[1]
```

Row Objects can be used to create the DataFrames in for quick and interactivity and exploration 

```
# In python 
rows = [Row("Metal x" , "CA"), Row("Reynold Xin" , "CA")]
authors_df = spark.createDataframe(rows, ["Authors" , "State"])
authors_df.show()


// In Scala 
val rows = Seq(("Matei Zahari", "CA") , ("Reynold Xin", "CA"))
authorDF = rows.toDF("Author" , "State")
authorDF.show()
```

### Common DataFrame Operations 
Spark provides read and write operation for the DataFrame. They are `DataFrameReader` and `DataFrameWriter`. 

### DataFrameReader and DataFrameWriter 
Reading and Writing are simple in Spark because of these high-level abstractions and contributions to connect various data sources like NoSQL stores, RDBMS, Streaming Engines like Apache Kafka 

### Transformation and Actions 

In this chapter we will see some of the transformations and actions operation on the DataFrame 

#### Projections and Filters 
`Projection` help us to get only machine rows based on some condition. 
We have `select()` and `filter()` projection for dataFrame()

```
# In Python
few_fire_df = (fire_df
.select("IncidentNumber", "AvailableDtTm", "CallType")
.where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)
// In Scala
val fewFireDF = fireDF
.select("IncidentNumber", "AvailableDtTm", "CallType")
.where($"CallType" =!= "Medical Incident")
fewFireDF.show(5, false)
```
### Renaming, Adding and Dropping column 
We can rename the columns where the values are not in correct name format, for such operation we have `withColumnRename()` 
```
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMin")
new_fire_df
    .select("ResponsedDelayedinMin")
    .where(col("ResponseDelayedinMin") > 5)
    .show(5, False)

// In Scala
val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF
.select("ResponseDelayedinMins")
.where($"ResponseDelayedinMins" > 5)
.show(5, false)
```
We can convert the `string` into `date/time` functions 
```
# In python 


// In Scala 
val fireTsDF = newFireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("watchDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .withColumns("AvailableDtTS", to_timestamp(col("Available", "MM/dd/yyyy"))
    .drop("AvailableDtTm"))

// Select DataFrame with Converted columns 
fireTsDF
.select("IncidentDate", "OnWatchDate", "AvailableDtTs")
.show(5, false)
```
Similarly we can do for the `year(), month(), day()`

```
# In Python 

```