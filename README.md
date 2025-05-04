
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
- [PySpark F functions (from pyspark.sql.functions)](#pyspark-f-functions-from-pysparksqlfunctions)
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

## PySpark F functions (from pyspark.sql.functions)

### Importing F Functions

```python
from pyspark.sql import functions as F
```

### 1. Basic Column Operations

```python
# Add a new column by applying a constant value
df = df.withColumn('new_column', F.lit(100))

# Add a new column by performing a mathematical operation
df = df.withColumn('double_salary', F.col('salary') * 2)

# Convert a string column to uppercase
df = df.withColumn('upper_name', F.upper(F.col('name')))

# Concatenate two string columns
df = df.withColumn('full_name', F.concat(F.col('first_name'), F.lit(' '), F.col('last_name')))

# Check if a string column starts with a specific substring
df = df.withColumn('starts_with_A', F.col('name').startswith('A'))

# Length of a string
df = df.withColumn('name_length', F.length(F.col('name')))

# Current date
df = df.withColumn('current_date', F.current_date())

# Current timestamp
df = df.withColumn('current_timestamp', F.current_timestamp())

# Extract the year from a date column
df = df.withColumn('year', F.year(F.col('date_of_birth')))

# Calculate the difference between two dates
df = df.withColumn('days_difference', F.datediff(F.col('current_date'), F.col('date_of_birth')))

# Use a condition to create a new column
df = df.withColumn('salary_category', 
                  F.when(F.col('salary') > 50000, 'High')
                   .when(F.col('salary') > 30000, 'Medium')
                   .otherwise('Low'))
```

### 2. Aggregations

```python
# Sum the 'salary' column
df.groupBy('department').agg(F.sum('salary').alias('total_salary'))

# Calculate average of 'salary' column
df.groupBy('department').agg(F.avg('salary').alias('avg_salary'))

# Get the minimum and maximum salary
df.groupBy('department').agg(F.min('salary').alias('min_salary'), F.max('salary').alias('max_salary'))

# Count distinct values in a column
df.groupBy('department').agg(F.countDistinct('employee_id').alias('distinct_employees'))
```

### 3. Window Functions

```python
from pyspark.sql.window import Window

# Define a window specification with partition and ordering by salary
window_spec = Window.partitionBy('department').orderBy('salary')

# Adding ROWS BETWEEN to the window specification
# Example: Current row and 2 previous rows (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
window_spec_with_range = window_spec.rowsBetween(-2, 0)  # This includes the current row and the previous two rows

# Calculate rank based on salary within each department
df = df.withColumn('rank', F.rank().over(window_spec))

# Calculate the cumulative sum (running total) of 'salary' within each department
df = df.withColumn('running_total', F.sum('salary').over(window_spec))

# Get the lag value of salary (previous row's salary) within each department
df = df.withColumn('previous_salary', F.lag('salary', 1).over(window_spec))

# Calculate the rolling sum (sum of salaries for the current row + 2 previous rows)
df = df.withColumn('rolling_sum', F.sum('salary').over(window_spec_with_range))
```

### 4. Other Useful Functions (eg.Null Handling)

```python
# Replace null values in a column with a specific value
df = df.fillna({'salary': 0, 'department': 'Unknown'})

# Replace null values in a specific column
df = df.fillna({'salary': 0})

# Check if a column value is null
df = df.withColumn('is_null', F.col('salary').isNull())

# Create an array from a list of columns
df = df.withColumn('array_column', F.array('first_name', 'last_name'))

# Explode an array column into multiple rows
df = df.withColumn('exploded', F.explode('array_column'))
```

### 5. Advanced Transformations

```python
# Compare two columns and create a new column based on the comparison
df = df.withColumn('is_salary_equal', F.col('salary') == F.col('previous_salary'))

# Find rows where salary has increased (comparing current and previous salary)
df = df.withColumn('salary_increase', F.when(F.col('salary') > F.col('previous_salary'), True).otherwise(False))

# Check if a column is an array and access its elements
df = df.withColumn('first_element', F.col('array_column')[0])  # First element of array

# Get the size of the array
df = df.withColumn('array_size', F.size('array_column'))

# Apply a map transformation to a DataFrame column
df = df.withColumn('mapped_salary', F.expr('salary * 1.1'))

# Use a map transformation on an RDD
rdd = df.rdd.map(lambda x: (x['id'], x['salary'] * 1.1))
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
