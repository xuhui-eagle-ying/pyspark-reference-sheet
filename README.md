
# 🔥 PySpark Syntax Reference Sheet

A quick reference for performing common data operations in **PySpark** (DataFrame API).

---

## 📌 Table of Contents

- [Initialize Spark](#initialize-spark)
- [Load Data](#load-data)
- [Inspect Data](#inspect-data)
- [Select & Filter](#select--filter)
- [Sort Data](#sort-data)
- [Group & Aggregate](#group--aggregate)
- [Join & Merge](#join--merge)
- [Column Operations](#column-operations)
- [PySpark `F` Functions (from pyspark.sql.functions)](#pyspark-f-functions-from-pysparksqlfunctions)
- [Missing Data](#missing-data)
- [RDD Operations](#rdd-operations)
- [Write Data](#write-data)

---

## Initialize Spark

```python
from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("App").getOrCreate()
```

## Load Data

```python
# Load data from CSV with header and infer schema
df = spark.read.csv('data.csv', header=True, inferSchema=True)

# Load data from JSON
df = spark.read.json('data.json')
```

## Inspect Data

```python
# Show the first few rows of the dataframe
df.show()

# Print the schema of the dataframe
df.printSchema()

# List the columns of the dataframe
df.columns

# Get summary statistics of numerical columns
df.describe().show()
```

## Select & Filter

```python
# Select a single column
df.select('col')

# Select multiple columns
df.select('col1', 'col2')

# Filter rows based on a condition
df.filter(df['age'] > 30)

# Filter rows based on multiple conditions
df.filter((df['age'] > 30) & (df['gender'] == 'M'))
```

## Sort Data

```python
# Sort by a column in ascending order (default)
df.orderBy('age')

# Sort by a column in descending order
df.orderBy(df['age'].desc())
```

## Group & Aggregate

```python
# Count the number of entries for each department
df.groupBy('department').count()

# Aggregate by calculating the average salary per department
df.groupBy('department').agg({'salary': 'avg'})

# Aggregate by calculating the maximum salary per department and gender
df.groupBy('dept', 'gender').agg({'salary': 'max'})
```

## Join & Merge

```python
# Perform an inner join on two dataframes based on the 'id' column
df1.join(df2, on='id', how='inner')  # options: left, right, outer
```

## Column Operations

```python
from pyspark.sql.functions import col

# Create a new column 'new_col' by multiplying the 'salary' column by 1.1
df = df.withColumn('new_col', col('salary') * 1.1)

# Rename a column from 'old' to 'new'
df = df.withColumnRenamed('old', 'new')

# Drop a column named 'unwanted'
df = df.drop('unwanted')
```

## PySpark F Functions (from pyspark.sql.functions)

### Importing F Functions

```python
from pyspark.sql import functions as F
```

## Missing Data

```python
# Drop rows with any null values
df.dropna()

# Fill null values with 0
df.fillna(0)

# Filter rows where 'col' is not null
df.filter(df['col'].isNotNull())
```

## RDD Operations

```python
# Convert a DataFrame to an RDD
rdd = df.rdd

# Perform a transformation on an RDD (e.g., map)
rdd_mapped = rdd.map(lambda x: (x['id'], x['salary']))

# Collect the results from an RDD back to the driver
results = rdd_mapped.collect()

# Perform an action on the RDD (e.g., reduce)
total_salary = rdd_mapped.map(lambda x: x[1]).reduce(lambda a, b: a + b)

# Perform a filter operation on an RDD
filtered_rdd = rdd.filter(lambda x: x['age'] > 30)
```

## Write Data

```python
# Write the dataframe to a CSV file with a header
df.write.csv('output.csv', header=True)

# Write the dataframe to a JSON file
df.write.json('output.json')
```
