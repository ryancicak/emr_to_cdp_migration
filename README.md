# emr_to_cdp_migration
Migrating data in EMR to CDP.  In this tutorial, we'll take Parquet snappy files written from EMR Spark into S3, and read those files within CDP (Cloudera Data Warehouse) Impala.  We'll create a Virtual Warehouse and create an external Impala table pointing to the same S3 location.  Running a few DDL statements, we'll be able to read the file(s) from within CDP. 

To prove this out (Migrating data from EMR into CDP), there will be two loads, each load being 50k rows.  


# Step 1
# Within EMR, First - Create the PySpark session to read/write CSV into DataFrames, where we'll write to Parquet (snappy) at the end.  In a shell, type "pyspark":
```shell
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType
from pyspark.sql import SparkSession

# Create SparkSession 

spark = SparkSession.builder \
      .master("yarn") \
      .appName("cdp_migration_test") \
      .getOrCreate()

      
# Schema
customSchema = StructType() \
      .add("ticketnumber",LongType(),True) \
      .add("leg1flightnum",LongType(),True) \
      .add("leg1uniquecarrier",StringType(),True) \
      .add("leg1origin",StringType(),True) \
      .add("leg1dest",StringType(),True) \
      .add("leg1month",LongType(),True) \
      .add("leg1dayofmonth",LongType(),True) \
      .add("leg1dayofweek",LongType(),True) \
      .add("leg1deptime",LongType(),True) \
      .add("leg1arrtime",LongType(),True) \
      .add("leg2flightnum",LongType(),True) \
      .add("leg2uniquecarrier",StringType(),True) \
      .add("leg2origin",StringType(),True) \
      .add("leg2dest",StringType(),True) \
      .add("leg2month",LongType(),True) \
      .add("leg2dayofmonth",LongType(),True) \
      .add("leg2dayofweek",LongType(),True) \
      .add("leg2deptime",LongType(),True) \
      .add("leg2arrtime",LongType(),True)
    
# First 50k rows
df1 = spark.read.option("header",True) \
  		  .schema(customSchema) \
          .csv("s3://ryancicak/airlines-csv/unique_tickets/unique_tickets_50krows_plus_header_part1.csv")

# Second 50k rows
df2 = spark.read.option("header",True) \
          .schema(customSchema) \
          .csv("s3://ryancicak/airlines-csv/unique_tickets/unique_tickets_50krows_plus_header_part2.csv")


# Write the first part 50k rows to the S3 location - as Parquet Snappy
df1.write.mode("APPEND") \
.option("compression", "snappy") \
.parquet("s3://ryancicak/airlines/unique_tickets/")


# Read the rows back from the S3 Parquet snappy location
reader = spark.read.parquet("s3://ryancicak/airlines/unique_tickets")

# Validate you see 50k rows
reader.count()
```

----
# Step 2
# Cloudera Data Warehouse - Go into CDW and validate you can CREATE TABLE IF NOT EXIST and run the rest of the DDL commands

```sql
CREATE external TABLE IF NOT EXISTS unique_tickets (ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING, leg1origin STRING,   leg1dest STRING, leg1month BIGINT, leg1dayofmonth BIGINT,   
 leg1dayofweek BIGINT, leg1deptime BIGINT, leg1arrtime BIGINT,   
 leg2flightnum BIGINT, leg2uniquecarrier STRING, leg2origin STRING,   
 leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,   leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT ) 
STORED AS PARQUET LOCATION 's3a://ryancicak/airlines/unique_tickets';

REFRESH unique_tickets; --or a single partition at a time (if a single partition was loaded): https://docs.cloudera.com/runtime/7.2.16/impala-sql-reference/topics/impala-refresh.html

INVALIDATE METADATA unique_tickets; --https://docs.cloudera.com/runtime/7.2.16/impala-sql-reference/topics/impala-invalidate-metadata.html

--You'll now see 50k rows within CDW
select count(1) from unique_tickets;

select count(1) as a, leg1dest from unique_tickets group by leg1dest order by a desc;
```

----
# Step 3
# Within EMR - Load the remaining 50k rows into the parquet snappy location (making a total of 100k rows)
```shell
df2.write.mode("APPEND") \
.option("compression", "snappy") \
.parquet("s3://ryancicak/airlines/unique_tickets/")

# Read the rows back from the S3 Parquet snappy location
reader = spark.read.parquet("s3://ryancicak/airlines/unique_tickets")

# Validate you see 100k rows - again this is within EMR
reader.count()
```
----
# Step 4
# Cloudera Data Warehouse - Go into CDW and and validate you CANNOT see the new 50k rows (you'll only see 50k rows not 100k rows), until you re-run the DDLs. This means after loading data within EMR, you'll need to re-run the statements each time (on the impacted tables).
# If you don't want to run DDLs every time you load data, move your Spark jobs from EMR into CDP, using the saveAsTable in the Spark statement - meaning Spark will keep the Cloudera HMS up-to-date.
```sql
select count(1) from unique_tickets;
select count(1) as a, leg1dest from unique_tickets group by leg1dest order by a desc;
--You'll receive 50k rows, not 100k rows until you run the following statement(s).  You'd do this each time you're loading data within EMR on the impacted tables:
```
# Step 5
# By re-running the DDL statements, you'll see the new loaded data from Spark
```sql
CREATE external TABLE IF NOT EXISTS unique_tickets (ticketnumber BIGINT, leg1flightnum BIGINT, leg1uniquecarrier STRING, leg1origin STRING,   leg1dest STRING, leg1month BIGINT, leg1dayofmonth BIGINT,   
 leg1dayofweek BIGINT, leg1deptime BIGINT, leg1arrtime BIGINT,   
 leg2flightnum BIGINT, leg2uniquecarrier STRING, leg2origin STRING,   
 leg2dest STRING, leg2month BIGINT, leg2dayofmonth BIGINT,   leg2dayofweek BIGINT, leg2deptime BIGINT, leg2arrtime BIGINT ) 
STORED AS PARQUET LOCATION 's3a://ryancicak/airlines/unique_tickets' ;
REFRESH unique_tickets; # or a single partition at a time (if a single partition was loaded): https://docs.cloudera.com/runtime/7.2.16/impala-sql-reference/topics/impala-refresh.html
INVALIDATE METADATA unique_tickets; # https://docs.cloudera.com/runtime/7.2.16/impala-sql-reference/topics/impala-invalidate-metadata.html

select count(1) from unique_tickets;
select count(1) as a, leg1dest from unique_tickets group by leg1dest order by a desc;

--You'll now see the new 50k rows (being 100k rows total)
```
